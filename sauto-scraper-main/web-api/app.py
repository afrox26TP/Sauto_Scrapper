from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


ROOT_DIR = Path(__file__).resolve().parents[1]
PARAMS_PATH = ROOT_DIR / "params.json"
API_VERSION = "1.0.0"
API_START_TIME = time.time()
DEFAULT_RESULTS_PATH = ROOT_DIR / "data" / "sauto_interesting.json"
MARKED_IDS_PATH = ROOT_DIR / "marked_ids.json"


class ParamsPayload(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)


class RunPayload(BaseModel):
    output_file: str = "data/sauto_interesting.json"


class ResultsPathPayload(BaseModel):
    path: str = "data/sauto_interesting.json"


class ResultIdsPayload(BaseModel):
    ids: list[str] = Field(default_factory=list)
    path: str = "data/sauto_interesting.json"


class ResultsImportPayload(BaseModel):
    items: list[dict[str, Any]] = Field(default_factory=list)
    path: str = "data/sauto_interesting.json"


class ResultMarkPayload(BaseModel):
    ids: list[str] = Field(default_factory=list)
    marked: bool = True


class ScraperRunner:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.process: subprocess.Popen[str] | None = None
        self.log_lines: deque[str] = deque(maxlen=250)
        self.last_exit_code: int | None = None
        self.last_started_at: float | None = None
        self.last_finished_at: float | None = None
        self.last_command: list[str] = []
        self.last_output_file: str = "data/sauto_interesting.json"

    def is_running(self) -> bool:
        with self.lock:
            return self.process is not None and self.process.poll() is None

    def _status_unlocked(self) -> dict[str, Any]:
        running = self.process is not None and self.process.poll() is None
        pid = self.process.pid if running and self.process else None
        return {
            "running": running,
            "pid": pid,
            "last_exit_code": self.last_exit_code,
            "last_started_at": self.last_started_at,
            "last_finished_at": self.last_finished_at,
            "last_command": self.last_command,
            "last_output_file": self.last_output_file,
            "log_count": len(self.log_lines),
        }

    def logs(self, limit: int = 120) -> dict[str, Any]:
        with self.lock:
            lines = list(self.log_lines)[-max(1, min(limit, 250)):]
            return {
                "lines": lines,
                "count": len(self.log_lines),
                "running": self.process is not None and self.process.poll() is None,
            }

    def _append_log(self, line: str) -> None:
        cleaned = line.rstrip("\r\n")
        if not cleaned:
            return
        with self.lock:
            self.log_lines.append(cleaned)

    def status(self) -> dict[str, Any]:
        with self.lock:
            return self._status_unlocked()

    def start(self, output_file: str) -> dict[str, Any]:
        with self.lock:
            if self.process is not None and self.process.poll() is None:
                raise RuntimeError("Scraper is already running.")

            command = [sys.executable, "-m", "scrapy", "crawl", "sauto", "-O", output_file]
            self.log_lines.clear()
            self.log_lines.append(f"[web-api] Spouštím: {' '.join(command)}")
            self.process = subprocess.Popen(
                command,
                cwd=ROOT_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            self.last_started_at = time.time()
            self.last_finished_at = None
            self.last_exit_code = None
            self.last_command = command
            self.last_output_file = output_file

            thread = threading.Thread(target=self._watch_process, daemon=True)
            thread.start()

            log_thread = threading.Thread(target=self._read_output, daemon=True)
            log_thread.start()

            return self._status_unlocked()

    def _read_output(self) -> None:
        with self.lock:
            process = self.process

        if process is None or process.stdout is None:
            return

        for line in process.stdout:
            self._append_log(line)

        try:
            process.stdout.close()
        except Exception:
            pass

    def _watch_process(self) -> None:
        with self.lock:
            process = self.process

        if process is None:
            return

        exit_code = process.wait()

        with self.lock:
            self.last_exit_code = exit_code
            self.last_finished_at = time.time()
            self.process = None
            self.log_lines.append(f"[web-api] Dokončeno s exit code {exit_code}")


def load_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback

    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except (json.JSONDecodeError, OSError):
        return fallback


def dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def normalize_relative_path(path: str | None, fallback: str = "data/sauto_interesting.json") -> str:
    rel_path = (path or fallback).strip() or fallback
    if os.path.isabs(rel_path):
        raise HTTPException(status_code=400, detail="Use a relative path.")

    resolved = (ROOT_DIR / rel_path).resolve()
    if ROOT_DIR not in resolved.parents and resolved != ROOT_DIR:
        raise HTTPException(status_code=400, detail="Path must stay inside project directory.")
    return rel_path


def load_marked_ids() -> set[str]:
    data = load_json(MARKED_IDS_PATH, [])
    if not isinstance(data, list):
        return set()
    return {str(item) for item in data}


def save_marked_ids(ids: set[str]) -> None:
    dump_json(MARKED_IDS_PATH, sorted(ids))


def load_result_items(result_path: Path) -> list[dict[str, Any]]:
    data = load_json(result_path, [])
    if not isinstance(data, list):
        raise HTTPException(status_code=500, detail="Results file does not contain a JSON array.")
    items: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            items.append(item)
    return items


app = FastAPI(title="Sauto Scraper API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

runner = ScraperRunner()


@app.get("/api/health")
def health() -> dict[str, Any]:
    uptime_s = int(time.time() - API_START_TIME)
    return {
        "status": "ok",
        "version": API_VERSION,
        "uptime_s": uptime_s,
        "python": sys.version.split()[0],
    }


@app.get("/api/params")
def get_params() -> dict[str, Any]:
    params = load_json(PARAMS_PATH, {})
    return {"params": params}


@app.put("/api/params")
def update_params(payload: ParamsPayload) -> dict[str, Any]:
    if not isinstance(payload.params, dict):
        raise HTTPException(status_code=400, detail="Invalid params payload.")

    normalized = {str(key): "" if value is None else str(value) for key, value in payload.params.items()}
    dump_json(PARAMS_PATH, normalized)
    return {"saved": True, "params": normalized}


@app.post("/api/run")
def run_scraper(payload: RunPayload) -> dict[str, Any]:
    output_file = payload.output_file.strip() or "data/sauto_interesting.json"

    if os.path.isabs(output_file):
        raise HTTPException(status_code=400, detail="Use a relative output_file path.")

    resolved = (ROOT_DIR / output_file).resolve()
    if ROOT_DIR not in resolved.parents and resolved != ROOT_DIR:
        raise HTTPException(status_code=400, detail="output_file must stay inside project directory.")

    try:
        status = runner.start(output_file=output_file)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to start scraper: {exc}") from exc

    return {"started": True, "status": status}


@app.get("/api/status")
def get_status() -> dict[str, Any]:
    return runner.status()


@app.get("/api/logs")
def get_logs(limit: int = 120) -> dict[str, Any]:
    return runner.logs(limit=limit)


@app.get("/api/results")
def get_results(path: str | None = None) -> dict[str, Any]:
    rel_path = normalize_relative_path(path, runner.last_output_file or "data/sauto_interesting.json")
    result_path = (ROOT_DIR / rel_path).resolve()

    if not result_path.exists():
        result_path = DEFAULT_RESULTS_PATH

    if not result_path.exists():
        return {"items": [], "path": str(rel_path), "count": 0, "marked_ids": []}

    items = load_result_items(result_path)
    marked_ids = load_marked_ids()
    annotated = []
    for item in items:
        ad_id = str(item.get("ad_id", ""))
        annotated.append({**item, "is_marked": ad_id in marked_ids})

    sorted_items = sorted(annotated, key=lambda item: item.get("score", 0), reverse=True)
    return {"items": sorted_items, "path": str(result_path.relative_to(ROOT_DIR)), "count": len(sorted_items), "marked_ids": sorted(marked_ids)}


@app.get("/api/results/export")
def export_results(path: str | None = None) -> dict[str, Any]:
    return get_results(path)


@app.post("/api/results/delete")
def delete_results(payload: ResultIdsPayload) -> dict[str, Any]:
    rel_path = normalize_relative_path(payload.path, runner.last_output_file or "data/sauto_interesting.json")
    result_path = (ROOT_DIR / rel_path).resolve()
    if not result_path.exists():
        result_path = DEFAULT_RESULTS_PATH

    if not result_path.exists():
        return {"deleted": 0, "remaining": 0, "path": str(rel_path)}

    ids = {str(item) for item in payload.ids if str(item).strip()}
    items = load_result_items(result_path)
    before = len(items)
    if ids:
        items = [item for item in items if str(item.get("ad_id", "")) not in ids]
    else:
        items = []

    dump_json(result_path, items)

    marked_ids = load_marked_ids()
    if ids:
        marked_ids.difference_update(ids)
        save_marked_ids(marked_ids)

    return {"deleted": before - len(items), "remaining": len(items), "path": str(result_path.relative_to(ROOT_DIR))}


@app.post("/api/results/clear")
def clear_results(payload: ResultsPathPayload) -> dict[str, Any]:
    rel_path = normalize_relative_path(payload.path, runner.last_output_file or "data/sauto_interesting.json")
    result_path = (ROOT_DIR / rel_path).resolve()
    dump_json(result_path, [])
    return {"cleared": True, "path": str(result_path.relative_to(ROOT_DIR))}


@app.post("/api/results/import")
def import_results(payload: ResultsImportPayload) -> dict[str, Any]:
    rel_path = normalize_relative_path(payload.path, runner.last_output_file or "data/sauto_interesting.json")
    result_path = (ROOT_DIR / rel_path).resolve()
    items = [item for item in payload.items if isinstance(item, dict)]
    dump_json(result_path, items)
    return {"imported": len(items), "path": str(result_path.relative_to(ROOT_DIR))}


@app.post("/api/results/mark")
def mark_results(payload: ResultMarkPayload) -> dict[str, Any]:
    ids = {str(item) for item in payload.ids if str(item).strip()}
    marked_ids = load_marked_ids()
    if payload.marked:
        marked_ids.update(ids)
    else:
        marked_ids.difference_update(ids)
    save_marked_ids(marked_ids)
    return {"marked_count": len(marked_ids), "updated": len(ids), "marked": payload.marked}
