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
from urllib.parse import urlencode
from urllib.request import urlopen

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sauto" / "spiders"))
from sauto_spider import CarEvaluator


ROOT_DIR = Path(__file__).resolve().parents[1]
PARAMS_PATH = ROOT_DIR / "params.json"
API_VERSION = "1.0.0"
API_START_TIME = time.time()
DEFAULT_RESULTS_PATH = ROOT_DIR / "data" / "sauto_interesting.json"
RAW_OUTPUT_PATH = ROOT_DIR / "data" / "sauto_raw.json"
MARKED_IDS_PATH = ROOT_DIR / "marked_ids.json"
CATALOG_CACHE_PATH = ROOT_DIR / "data" / "sauto_catalog_cache.json"
CATALOG_CACHE_TTL_S = 24 * 60 * 60
SAUTO_SEARCH_API = "https://www.sauto.cz/api/v1/items/search"


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


class CustomPresetPayload(BaseModel):
    name: str = ""
    description: str = ""
    weights: dict[str, float] = Field(default_factory=dict)
    hard_rejects: list[dict[str, str]] = Field(default_factory=list)
    must_have_equipment: list[str] = Field(default_factory=list)
    excluded_equipment: list[str] = Field(default_factory=list)


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

            # Use a dedicated raw file for scrapy feed export to avoid
            # conflicting with the spider's own sauto_interesting.json output
            command = [sys.executable, "-m", "scrapy", "crawl", "sauto", "-O", str(RAW_OUTPUT_PATH.relative_to(ROOT_DIR))]
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
    if not result_path.exists():
        return []
    try:
        with result_path.open("r", encoding="utf-8") as fh:
            raw = fh.read().strip()
    except OSError:
        return []
    if not raw:
        return []
    # Robust parser: trim trailing garbage until we get valid JSON
    attempt = raw
    for _ in range(30):
        try:
            data = json.loads(attempt)
            break
        except json.JSONDecodeError as exc:
            if "Extra data" in str(exc):
                attempt = attempt[: exc.pos].rstrip()
            else:
                return []
    else:
        return []
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def _load_catalog_cache() -> dict[str, Any]:
    data = load_json(CATALOG_CACHE_PATH, {})
    if not isinstance(data, dict):
        return {}
    return data


def _save_catalog_cache(data: dict[str, Any]) -> None:
    dump_json(CATALOG_CACHE_PATH, data)


def _is_fresh(ts: float | int | None, ttl_s: int = CATALOG_CACHE_TTL_S) -> bool:
    if ts is None:
        return False
    try:
        return (time.time() - float(ts)) < ttl_s
    except (TypeError, ValueError):
        return False


def _fetch_sauto_results(params: dict[str, Any]) -> list[dict[str, Any]]:
    query = urlencode(params)
    url = f"{SAUTO_SEARCH_API}?{query}"
    with urlopen(url, timeout=15) as response:
        payload = json.loads(response.read().decode("utf-8"))
    results = payload.get("results", []) if isinstance(payload, dict) else []
    return [item for item in results if isinstance(item, dict)]


def _collect_brands(max_pages: int = 25, page_size: int = 200) -> list[dict[str, str]]:
    brands: dict[str, str] = {}
    for page in range(max_pages):
        offset = page * page_size
        batch = _fetch_sauto_results({"category_id": 838, "limit": page_size, "offset": offset})
        if not batch:
            break
        for item in batch:
            manufacturer = item.get("manufacturer_cb") or {}
            seo_name = str(manufacturer.get("seo_name") or "").strip()
            name = str(manufacturer.get("name") or seo_name).strip()
            if seo_name:
                brands[seo_name] = name
    return [{"value": key, "label": brands[key]} for key in sorted(brands.keys())]


def _collect_models_for_brand(brand: str, max_pages: int = 8, page_size: int = 150) -> list[dict[str, str]]:
    models: dict[str, str] = {}
    for page in range(max_pages):
        offset = page * page_size
        batch = _fetch_sauto_results(
            {
                "category_id": 838,
                "manufacturer_seo_name": brand,
                "limit": page_size,
                "offset": offset,
            }
        )
        if not batch:
            break
        for item in batch:
            manufacturer = item.get("manufacturer_cb") or {}
            manufacturer_seo = str(manufacturer.get("seo_name") or "").strip().lower()
            if manufacturer_seo != brand:
                continue
            model = item.get("model_cb") or {}
            seo_name = str(model.get("seo_name") or "").strip()
            name = str(model.get("name") or seo_name).strip()
            if seo_name:
                models[seo_name] = name
    return [{"value": key, "label": models[key]} for key in sorted(models.keys())]


def _collect_equipment(max_pages: int = 20, page_size: int = 200) -> list[dict[str, str]]:
    equipment: dict[str, str] = {}
    # Primary: raw scraped data (equipment_cb in detail_raw.result)
    for data_path in [RAW_OUTPUT_PATH, DEFAULT_RESULTS_PATH]:
        if data_path.exists():
            try:
                data = load_json(data_path, [])
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            # sauto_interesting.json: offer_metrics.equipment_list
                            om = item.get("offer_metrics") or {}
                            el = om.get("equipment_list") or []
                            if isinstance(el, list):
                                for name in el:
                                    n = str(name).strip()
                                    if n and len(n) > 2:
                                        equipment[n.lower()] = n
                            # sauto_raw.json: detail_raw.result.equipment_cb
                            dr = item.get("detail_raw") or {}
                            res = dr.get("result") or {}
                            ecb = res.get("equipment_cb") or []
                            if isinstance(ecb, list):
                                for eq in ecb:
                                    n = str(eq.get("name", "") if isinstance(eq, dict) else eq).strip()
                                    if n and len(n) > 2:
                                        equipment[n.lower()] = n
                            # top-level equipment_list fallback
                            el_top = item.get("equipment_list") or []
                            if isinstance(el_top, list):
                                for name in el_top:
                                    n = str(name).strip()
                                    if n and len(n) > 2:
                                        equipment[n.lower()] = n
            except Exception:
                pass
    # Fallback: try Sauto search API
    if len(equipment) < 10:
        for page in range(max_pages):
            offset = page * page_size
            batch = _fetch_sauto_results({"category_id": 838, "limit": page_size, "offset": offset})
            if not batch:
                break
            for item in batch:
                equip_list = item.get("equipment_list") or item.get("equipment") or item.get("equipment_cb") or []
                if isinstance(equip_list, list):
                    for eq in equip_list:
                        name = str(eq).strip()
                        if name and len(name) > 2:
                            equipment[name.lower()] = name
    return [{"value": key, "label": equipment[key]} for key in sorted(equipment.keys())]


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

    # Prevent stale UI data: start each run with a clean target result file.
    dump_json(resolved, [])

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
    rel_path = normalize_relative_path(path, "data/sauto_interesting.json")
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
    return {
        "items": sorted_items,
        "path": str(result_path.relative_to(ROOT_DIR)),
        "count": len(sorted_items),
        "marked_ids": sorted(marked_ids),
        "scraper_running": runner.is_running(),
    }


@app.get("/api/catalog/brands")
def get_catalog_brands(force_refresh: bool = False) -> dict[str, Any]:
    cache = _load_catalog_cache()
    catalog = cache.get("brands", {}) if isinstance(cache, dict) else {}
    cached_items = catalog.get("items", []) if isinstance(catalog, dict) else []
    cached_ts = catalog.get("updated_at") if isinstance(catalog, dict) else None

    if not force_refresh and _is_fresh(cached_ts) and isinstance(cached_items, list):
        return {"items": cached_items, "cached": True, "updated_at": cached_ts}

    try:
        items = _collect_brands()
    except Exception as exc:
        if isinstance(cached_items, list) and cached_items:
            return {"items": cached_items, "cached": True, "updated_at": cached_ts, "warning": f"Using cache: {exc}"}
        raise HTTPException(status_code=502, detail=f"Unable to fetch Sauto brands: {exc}") from exc

    now = int(time.time())
    if not isinstance(cache, dict):
        cache = {}
    cache["brands"] = {"updated_at": now, "items": items}
    _save_catalog_cache(cache)
    return {"items": items, "cached": False, "updated_at": now}


@app.get("/api/catalog/models")
def get_catalog_models(brand: str, force_refresh: bool = False) -> dict[str, Any]:
    selected_brand = (brand or "").strip().lower()
    if not selected_brand:
        return {"brand": "", "items": [], "cached": True, "updated_at": None}

    cache = _load_catalog_cache()
    models_cache = cache.get("models", {}) if isinstance(cache, dict) else {}
    brand_cache = models_cache.get(selected_brand, {}) if isinstance(models_cache, dict) else {}
    cached_items = brand_cache.get("items", []) if isinstance(brand_cache, dict) else []
    cached_ts = brand_cache.get("updated_at") if isinstance(brand_cache, dict) else None
    cached_collector_version = brand_cache.get("collector_version") if isinstance(brand_cache, dict) else None

    # Per-brand cache gate to avoid serving stale entries from older buggy collector logic.
    cache_ok = cached_collector_version == 2

    if not force_refresh and cache_ok and _is_fresh(cached_ts) and isinstance(cached_items, list):
        return {"brand": selected_brand, "items": cached_items, "cached": True, "updated_at": cached_ts}

    try:
        items = _collect_models_for_brand(selected_brand)
    except Exception as exc:
        if isinstance(cached_items, list) and cached_items:
            return {
                "brand": selected_brand,
                "items": cached_items,
                "cached": True,
                "updated_at": cached_ts,
                "warning": f"Using cache: {exc}",
            }
        raise HTTPException(status_code=502, detail=f"Unable to fetch Sauto models for '{selected_brand}': {exc}") from exc

    now = int(time.time())
    if not isinstance(cache, dict):
        cache = {}
    if not isinstance(cache.get("models"), dict):
        cache["models"] = {}
    cache["models"][selected_brand] = {
        "updated_at": now,
        "items": items,
        "collector_version": 2,
    }
    _save_catalog_cache(cache)

    return {"brand": selected_brand, "items": items, "cached": False, "updated_at": now}


@app.get("/api/catalog/equipment")
def get_catalog_equipment(force_refresh: bool = False) -> dict[str, Any]:
    cache = _load_catalog_cache()
    equip_cache = cache.get("equipment", {}) if isinstance(cache, dict) else {}
    cached_items = equip_cache.get("items", []) if isinstance(equip_cache, dict) else []
    cached_ts = equip_cache.get("updated_at") if isinstance(equip_cache, dict) else None

    if not force_refresh and _is_fresh(cached_ts) and isinstance(cached_items, list) and cached_items:
        return {"items": cached_items, "cached": True, "updated_at": cached_ts}

    try:
        items = _collect_equipment()
    except Exception as exc:
        if isinstance(cached_items, list) and cached_items:
            return {"items": cached_items, "cached": True, "updated_at": cached_ts, "warning": f"Using cache: {exc}"}
        raise HTTPException(status_code=502, detail=f"Unable to fetch Sauto equipment: {exc}") from exc

    if not items:
        # Try supplementing from local data
        equipment: dict[str, str] = {}
        for data_path in [RAW_OUTPUT_PATH, DEFAULT_RESULTS_PATH]:
            if data_path.exists():
                try:
                    data = load_json(data_path, [])
                    if isinstance(data, list):
                        for item_dict in data:
                            if isinstance(item_dict, dict):
                                el = item_dict.get("equipment_list") or []
                                if isinstance(el, list):
                                    for name in el:
                                        n = str(name).strip()
                                        if n and len(n) > 2:
                                            equipment[n.lower()] = n
                except Exception:
                    pass
        items = [{"value": key, "label": equipment[key]} for key in sorted(equipment.keys())]

    now = int(time.time())
    if not isinstance(cache, dict):
        cache = {}
    cache["equipment"] = {"updated_at": now, "items": items}
    _save_catalog_cache(cache)
    return {"items": items, "cached": False, "updated_at": now}


@app.get("/api/catalog/bodies")
def get_catalog_bodies(force_refresh: bool = False) -> dict[str, Any]:
    cache = _load_catalog_cache()
    body_cache = cache.get("bodies", {}) if isinstance(cache, dict) else {}
    cached_items = body_cache.get("items", []) if isinstance(body_cache, dict) else []
    cached_ts = body_cache.get("updated_at") if isinstance(body_cache, dict) else None

    if not force_refresh and _is_fresh(cached_ts) and isinstance(cached_items, list) and cached_items:
        return {"items": cached_items, "cached": True, "updated_at": cached_ts}

    # Collect unique body types from locally scraped data and search API
    bodies: dict[str, str] = {}
    # From local data
    for data_path in [RAW_OUTPUT_PATH, DEFAULT_RESULTS_PATH]:
        if data_path.exists():
            try:
                data = load_json(data_path, [])
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            body = str(item.get("body_seo") or "").strip()
                            if body and len(body) > 1:
                                label = body[0].upper() + body[1:]
                                bodies[body] = label
            except Exception:
                pass
    # From Sauto search API
    if len(bodies) < 3:
        try:
            batch = _fetch_sauto_results({"category_id": 838, "limit": 200, "offset": 0})
            for item in batch:
                body_cb = item.get("vehicle_body_cb") or {}
                seo = str(body_cb.get("seo_name") or "").strip()
                name = str(body_cb.get("name") or seo).strip()
                if seo:
                    bodies[seo] = name
        except Exception:
            pass

    items = [{"value": key, "label": bodies[key]} for key in sorted(bodies.keys())]

    now = int(time.time())
    if not isinstance(cache, dict):
        cache = {}
    cache["bodies"] = {"updated_at": now, "items": items}
    _save_catalog_cache(cache)
    return {"items": items, "cached": False, "updated_at": now}


@app.get("/api/scoring/presets")
def get_scoring_presets() -> dict[str, Any]:
    """Return all available scoring presets (built-in + custom) for frontend use."""
    builtin = {}
    for preset_name, preset_config in CarEvaluator.SCORING_PRESETS.items():
        weights = preset_config.get("weights", {})
        builtin[preset_name] = {
            "name": preset_config.get("name", preset_name),
            "description": preset_config.get("description", ""),
            "weights": weights,
        }

    all_params = load_json(PARAMS_PATH, {})
    custom_presets = all_params.get("custom_presets", {})
    if not isinstance(custom_presets, dict):
        custom_presets = {}

    return {"builtin": builtin, "custom": custom_presets}


def _load_custom_presets() -> dict[str, Any]:
    all_params = load_json(PARAMS_PATH, {})
    custom = all_params.get("custom_presets", {})
    return custom if isinstance(custom, dict) else {}


def _save_custom_presets(presets: dict[str, Any]) -> None:
    all_params = load_json(PARAMS_PATH, {})
    if not isinstance(all_params, dict):
        all_params = {}
    # Keep custom_presets as a raw dict (not stringified), other params remain strings
    clean = {}
    for key, value in all_params.items():
        if key == "custom_presets":
            clean[key] = value if isinstance(value, dict) else {}
        else:
            clean[key] = "" if value is None else str(value)
    clean["custom_presets"] = presets
    dump_json(PARAMS_PATH, clean)


def _sanitize_preset_id(preset_id: str) -> str:
    import re
    sanitized = re.sub(r"[^a-z0-9_-]", "", preset_id.strip().lower())
    if not sanitized:
        raise HTTPException(status_code=400, detail="Preset ID must contain at least one alphanumeric character.")
    if sanitized in CarEvaluator.SCORING_PRESETS:
        raise HTTPException(status_code=400, detail=f"'{sanitized}' is a built-in preset name and cannot be overwritten.")
    return sanitized


@app.post("/api/scoring/presets/custom")
def create_custom_preset(payload: CustomPresetPayload) -> dict[str, Any]:
    """Create a new custom scoring preset."""
    if not payload.name or not payload.name.strip():
        raise HTTPException(status_code=400, detail="Preset name is required.")

    import re
    preset_id = re.sub(r"[^a-z0-9_-]", "", payload.name.strip().lower().replace(" ", "-"))
    if not preset_id:
        raise HTTPException(status_code=400, detail="Preset name must contain at least one alphanumeric character.")
    preset_id = _sanitize_preset_id(preset_id)

    custom_presets = _load_custom_presets()
    if preset_id in custom_presets:
        raise HTTPException(status_code=409, detail=f"Custom preset '{preset_id}' already exists.")

    weights = {str(k): float(v) for k, v in (payload.weights or {}).items()}
    custom_presets[preset_id] = {
        "name": payload.name.strip(),
        "description": (payload.description or "").strip(),
        "weights": weights,
        "hard_rejects": payload.hard_rejects or [],
        "must_have_equipment": payload.must_have_equipment or [],
        "excluded_equipment": payload.excluded_equipment or [],
        "created_at": time.time(),
    }
    _save_custom_presets(custom_presets)
    return {"preset_id": preset_id, "preset": custom_presets[preset_id]}


@app.put("/api/scoring/presets/custom/{preset_id}")
def update_custom_preset(preset_id: str, payload: CustomPresetPayload) -> dict[str, Any]:
    """Update an existing custom scoring preset."""
    custom_presets = _load_custom_presets()
    if preset_id not in custom_presets:
        raise HTTPException(status_code=404, detail=f"Custom preset '{preset_id}' not found.")

    existing = custom_presets[preset_id]
    existing["name"] = (payload.name or existing["name"]).strip()
    existing["description"] = (payload.description or existing.get("description", "")).strip()
    existing["weights"] = {str(k): float(v) for k, v in (payload.weights or existing.get("weights", {})).items()}
    existing["hard_rejects"] = payload.hard_rejects if payload.hard_rejects is not None else existing.get("hard_rejects", [])
    existing["must_have_equipment"] = payload.must_have_equipment if payload.must_have_equipment is not None else existing.get("must_have_equipment", [])
    existing["excluded_equipment"] = payload.excluded_equipment if payload.excluded_equipment is not None else existing.get("excluded_equipment", [])
    existing["updated_at"] = time.time()

    _save_custom_presets(custom_presets)
    return {"preset_id": preset_id, "preset": existing}


@app.delete("/api/scoring/presets/custom/{preset_id}")
def delete_custom_preset(preset_id: str) -> dict[str, Any]:
    """Delete a custom scoring preset."""
    custom_presets = _load_custom_presets()
    if preset_id not in custom_presets:
        raise HTTPException(status_code=404, detail=f"Custom preset '{preset_id}' not found.")

    deleted = custom_presets.pop(preset_id)
    _save_custom_presets(custom_presets)
    return {"deleted": True, "preset_id": preset_id, "name": deleted.get("name", preset_id)}


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
