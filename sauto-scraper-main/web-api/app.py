from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


ROOT_DIR = Path(__file__).resolve().parents[1]
PARAMS_PATH = ROOT_DIR / "params.json"
DEFAULT_RESULTS_PATH = ROOT_DIR / "data" / "sauto_interesting.json"


class ParamsPayload(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)


class RunPayload(BaseModel):
    output_file: str = "data/sauto_interesting.json"


class ScraperRunner:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.process: subprocess.Popen[str] | None = None
        self.last_exit_code: int | None = None
        self.last_started_at: float | None = None
        self.last_finished_at: float | None = None
        self.last_command: list[str] = []
        self.last_output_file: str = "data/sauto_interesting.json"

    def is_running(self) -> bool:
        with self.lock:
            return self.process is not None and self.process.poll() is None

    def status(self) -> dict[str, Any]:
        with self.lock:
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
            }

    def start(self, output_file: str) -> dict[str, Any]:
        with self.lock:
            if self.process is not None and self.process.poll() is None:
                raise RuntimeError("Scraper is already running.")

            command = [sys.executable, "-m", "scrapy", "crawl", "sauto", "-O", output_file]
            self.process = subprocess.Popen(
                command,
                cwd=ROOT_DIR,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            self.last_started_at = time.time()
            self.last_finished_at = None
            self.last_exit_code = None
            self.last_command = command
            self.last_output_file = output_file

            thread = threading.Thread(target=self._watch_process, daemon=True)
            thread.start()

            return self.status()

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
def health() -> dict[str, str]:
    return {"status": "ok"}


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


@app.get("/api/results")
def get_results(path: str | None = None) -> dict[str, Any]:
    rel_path = (path or runner.last_output_file or "data/sauto_interesting.json").strip()
    if not rel_path:
        rel_path = "data/sauto_interesting.json"

    if os.path.isabs(rel_path):
        raise HTTPException(status_code=400, detail="Use a relative path.")

    result_path = (ROOT_DIR / rel_path).resolve()
    if ROOT_DIR not in result_path.parents and result_path != ROOT_DIR:
        raise HTTPException(status_code=400, detail="Path must stay inside project directory.")

    if not result_path.exists():
        # Fallback keeps endpoint useful right after first startup.
        result_path = DEFAULT_RESULTS_PATH

    if not result_path.exists():
        return {"items": [], "path": str(rel_path), "count": 0}

    data = load_json(result_path, [])
    if not isinstance(data, list):
        raise HTTPException(status_code=500, detail="Results file does not contain a JSON array.")

    sorted_items = sorted(data, key=lambda item: item.get("score", 0), reverse=True)
    return {"items": sorted_items, "path": str(result_path.relative_to(ROOT_DIR)), "count": len(sorted_items)}
