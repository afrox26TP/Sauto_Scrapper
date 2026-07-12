from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
import base64
import hashlib
import hmac
import secrets
import ipaddress
import socket
from collections import deque
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode, urlsplit
from urllib.error import HTTPError
from urllib.request import Request as UrlRequest, urlopen, build_opener, ProxyHandler

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from starlette.responses import JSONResponse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sauto" / "spiders"))
from sauto_spider import CarEvaluator


ROOT_DIR = Path(__file__).resolve().parents[1]
PARAMS_PATH = ROOT_DIR / "params.json"
API_VERSION = "1.0.0"
API_START_TIME = time.time()
DEFAULT_RESULTS_PATH = ROOT_DIR / "data" / "sauto_interesting.json"
RAW_OUTPUT_PATH = ROOT_DIR / "data" / "sauto_raw.jl"
RAW_OUTPUT_JSON_PATH = ROOT_DIR / "data" / "sauto_raw.json"
MARKED_IDS_PATH = ROOT_DIR / "marked_ids.json"
CATALOG_CACHE_PATH = ROOT_DIR / "data" / "sauto_catalog_cache.json"
BILLING_LEDGER_PATH = ROOT_DIR / "data" / "billing_usage.json"
USERS_DB_PATH = ROOT_DIR / "data" / "users.json"
CATALOG_CACHE_TTL_S = 24 * 60 * 60
SAUTO_SEARCH_API = "https://www.sauto.cz/api/v1/items/search"
SAUTO_ITEM_DETAIL_API = "https://www.sauto.cz/api/v1/items/{}"
AUTH_TOKEN_TTL_S = int(os.getenv("AUTH_TOKEN_TTL_S", "604800"))
AUTH_SECRET = (os.getenv("AUTH_SECRET") or "dev-only-change-me").strip() or "dev-only-change-me"
PUBLIC_API_PATHS = {
    "/api/health",
    "/api/auth/signup",
    "/api/auth/login",
    "/api/results",
    "/api/results/export",
    "/api/catalog/brands",
    "/api/catalog/models",
    "/api/catalog/model-counts",
    "/api/catalog/estimate",
    "/api/catalog/equipment",
    "/api/catalog/bodies",
}
AUTH_REQUIRED_PATHS = {
    "/api/auth/me",
    "/api/pause",
    "/api/resume",
    "/api/stop",
    "/api/billing/access",
    "/api/billing/checkout-session",
    "/api/proxy/config",
    "/api/proxy/test",
}
LOCKED_SEARCH_DEFAULTS = {
    "category_id": "838",
    "limit": "100",
    "offset": "0",
}
MAX_REASONABLE_POWER_KW = 900
RESULT_TEXT_REJECT_PATTERNS = (
    re.compile(r"na\s*spl[aá]tk|spl[aá]tk(y|a|ove)|u[vě]r", re.IGNORECASE),
    re.compile(r"leasing|operativn[ií]\s*leasing", re.IGNORECASE),
)
EXCLUDED_MODEL_BODY_TYPES = {
    "silnicni",
    "enduro",
    "chopper",
    "ctyrkolka",
    "elektromotorka",
}
WRITE_HTTP_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
API_KEYS = {
    token.strip()
    for token in (os.getenv("SAUTO_API_KEYS") or "").split(",")
    if token.strip()
}
BILLING_RUN_BASE_CZK = float(os.getenv("BILLING_RUN_BASE_CZK", "5.0"))
BILLING_ITEM_CZK = float(os.getenv("BILLING_ITEM_CZK", "0.02"))
BILLING_API_CALL_CZK = float(os.getenv("BILLING_API_CALL_CZK", "0.05"))
BILLING_PROXY_RUN_CZK = float(os.getenv("BILLING_PROXY_RUN_CZK", "0.0"))
PAYMENT_PROVIDER = (os.getenv("PAYMENT_PROVIDER", "stripe") or "stripe").strip().lower()
STRIPE_SECRET_KEY = (os.getenv("STRIPE_SECRET_KEY") or "").strip()
STRIPE_PRICE_ID = (os.getenv("STRIPE_PRICE_ID") or "").strip()
STRIPE_SUCCESS_URL = (os.getenv("STRIPE_SUCCESS_URL") or "").strip()
STRIPE_CANCEL_URL = (os.getenv("STRIPE_CANCEL_URL") or "").strip()
STRIPE_PAYMENT_LINK_URL = (os.getenv("STRIPE_PAYMENT_LINK_URL") or "").strip()
STRIPE_WEBHOOK_SECRET = (os.getenv("STRIPE_WEBHOOK_SECRET") or "").strip()
STRIPE_DEFAULT_CURRENCY = (os.getenv("STRIPE_DEFAULT_CURRENCY") or "czk").strip().lower() or "czk"
STRIPE_DEFAULT_AMOUNT_CENTS = int(os.getenv("STRIPE_DEFAULT_AMOUNT_CENTS", "9900"))
CORS_ALLOW_ORIGINS = [
    origin.strip()
    for origin in (os.getenv("CORS_ALLOW_ORIGINS") or "http://localhost:5173,http://127.0.0.1:5173").split(",")
    if origin.strip()
]
CORS_ALLOW_ORIGIN_REGEX = (os.getenv("CORS_ALLOW_ORIGIN_REGEX") or r"https://.*\.trycloudflare\.com").strip()
CORS_ALLOW_ORIGIN_PATTERN = re.compile(CORS_ALLOW_ORIGIN_REGEX) if CORS_ALLOW_ORIGIN_REGEX else None
CORS_ALLOW_ANY_ORIGIN = "*" in CORS_ALLOW_ORIGINS
CORS_ALLOW_ORIGIN_SET = {origin for origin in CORS_ALLOW_ORIGINS if origin != "*"}
_NO_PROXY_OPENER = build_opener(ProxyHandler({}))
BLOCKED_PROXY_ENV_KEYS = {
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
}
ALLOWED_PROXY_SCHEMES = {"http", "https", "socks5h"}
DEFAULT_FREE_PROXY_PROFILE_ID = "free_proxy_default"
DEFAULT_PAID_PROXY_PROFILE_ID = "paid_proxy_default"

# Enforce zero implicit proxying in the API process itself.
for _proxy_env_key in BLOCKED_PROXY_ENV_KEYS:
    os.environ.pop(_proxy_env_key, None)


def _urlopen_no_proxy(url_or_request: str | UrlRequest, timeout: float):
    return _NO_PROXY_OPENER.open(url_or_request, timeout=timeout)


def _is_public_ip(ip_text: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_text)
    except ValueError:
        return False
    return bool(getattr(ip, "is_global", False))


def _validate_proxy_url(proxy_url: str) -> str:
    value = str(proxy_url or "").strip()
    if not value:
        raise ValueError("Nelze spustit úlohu: Chybí konfigurace proxy.")

    parsed = urlsplit(value)
    scheme = (parsed.scheme or "").lower()
    if scheme not in ALLOWED_PROXY_SCHEMES:
        raise ValueError("Proxy schema musí být http, https nebo socks5h.")

    host = str(parsed.hostname or "").strip().lower()
    if not host:
        raise ValueError("Proxy host chybí.")
    if host in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("Proxy host nesmí být localhost.")
    if host.endswith(".local"):
        raise ValueError("Lokální domény nejsou povoleny.")

    # If proxy host is an IP literal, only public addresses are allowed.
    try:
        ip_obj = ipaddress.ip_address(host)
        if not bool(getattr(ip_obj, "is_global", False)):
            raise ValueError("Privátní nebo lokální IP adresa proxy není povolena.")
        return value
    except ValueError:
        pass

    # For domain names, resolve and reject private/loopback/link-local targets.
    try:
        resolved = socket.getaddrinfo(host, parsed.port or 0, type=socket.SOCK_STREAM)
    except Exception as exc:
        raise ValueError(f"Proxy doména není resolvovatelná: {exc}") from exc

    if not resolved:
        raise ValueError("Proxy doména není resolvovatelná.")

    for addr in resolved:
        sockaddr = addr[4]
        ip_text = str(sockaddr[0]) if isinstance(sockaddr, tuple) and sockaddr else ""
        if not _is_public_ip(ip_text):
            raise ValueError("Proxy doména směřuje na privátní nebo lokální IP, což není povoleno.")

    return value


def _sanitize_subprocess_env(base_env: dict[str, str]) -> dict[str, str]:
    sanitized = dict(base_env)
    for key in BLOCKED_PROXY_ENV_KEYS:
        sanitized.pop(key, None)
    return sanitized


class ParamsPayload(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)


class RunPayload(BaseModel):
    output_file: str = "data/sauto_interesting.json"
    project_id: str = "default"
    run_mode: str = "free_proxy"
    proxy_profile_id: str = ""


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


class ModelCountsPayload(BaseModel):
    brand: str = ""
    config: dict[str, Any] = Field(default_factory=dict)


class CatalogEstimatePayload(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)


class SignupPayload(BaseModel):
    email: str = ""
    password: str = ""


class LoginPayload(BaseModel):
    email: str = ""
    password: str = ""


class CheckoutSessionPayload(BaseModel):
    success_url: str = ""
    cancel_url: str = ""


class ProxyConfigPayload(BaseModel):
    free_proxy_url: str | None = None
    paid_proxy_url: str | None = None
    profiles: list[dict[str, Any]] | None = None


class ProxyProfilePayload(BaseModel):
    name: str = ""
    kind: str = "free_proxy"
    proxy_url: str = ""


class ProxyProfileUpdatePayload(BaseModel):
    name: str | None = None
    kind: str | None = None
    proxy_url: str | None = None


class ProxyTestPayload(BaseModel):
    proxy_url: str = ""


def _is_paid_status(value: str) -> bool:
    return str(value or "").strip().lower() in {"paid", "active", "trialing", "lifetime"}


def _is_loopback_request(request: Request) -> bool:
    host = str(getattr(request.client, "host", "") or "").strip().lower()
    return host in {"127.0.0.1", "::1", "localhost"}


def _stripe_request(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not STRIPE_SECRET_KEY:
        raise RuntimeError("STRIPE_SECRET_KEY is not configured.")

    body = urlencode(payload).encode("utf-8")
    req = UrlRequest(
        url=f"https://api.stripe.com{path}",
        method="POST",
        data=body,
        headers={
            "Authorization": f"Bearer {STRIPE_SECRET_KEY}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    with _urlopen_no_proxy(req, timeout=20) as response:
        raw = response.read().decode("utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise RuntimeError("Invalid Stripe response.")
    return data


def _stripe_verify_signature(payload: bytes, header_value: str) -> bool:
    if not STRIPE_WEBHOOK_SECRET:
        return True
    signed_header = str(header_value or "")
    if not signed_header:
        return False

    timestamp = ""
    signature = ""
    for part in signed_header.split(","):
        key, _, value = part.strip().partition("=")
        if key == "t":
            timestamp = value
        elif key == "v1":
            signature = value

    if not timestamp or not signature:
        return False

    signed_payload = f"{timestamp}.{payload.decode('utf-8')}"
    expected = hmac.new(
        STRIPE_WEBHOOK_SECRET.encode("utf-8"),
        signed_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


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
        self.on_finished: Callable[[int, str], None] | None = None
        self.paused: bool = False

    def is_running(self) -> bool:
        with self.lock:
            return self.process is not None and self.process.poll() is None

    def _status_unlocked(self) -> dict[str, Any]:
        running = self.process is not None and self.process.poll() is None
        pid = self.process.pid if running and self.process else None
        return {
            "running": running,
            "paused": self.paused if running else False,
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

    def start(self, output_file: str, run_mode: str = "free_proxy", proxy_url: str = "") -> dict[str, Any]:
        with self.lock:
            if self.process is not None and self.process.poll() is None:
                raise RuntimeError("Scraper is already running.")

            # Use a dedicated raw file for scrapy feed export to avoid
            # conflicting with the spider's own sauto_interesting.json output
            command = [
                sys.executable,
                "-m",
                "scrapy",
                "crawl",
                "sauto",
                "-a",
                f"output_file={output_file}",
                "-O",
                str(RAW_OUTPUT_PATH.relative_to(ROOT_DIR)),
            ]

            run_mode_norm = "paid_proxy" if str(run_mode or "").strip().lower() == "paid_proxy" else "free_proxy"
            selected_url = str(proxy_url or "").strip()
            proxy_count = 1 if selected_url else 0
            subprocess_env = _sanitize_subprocess_env(dict(os.environ))
            subprocess_env["SAUTO_PROXY_LIST"] = ""
            subprocess_env["SAUTO_PROXY_URL"] = selected_url
            subprocess_env["SAUTO_PROXY_MODE"] = "round_robin"
            subprocess_env["SAUTO_PROXY_BAN_STATUSES"] = ""
            subprocess_env["SAUTO_PROXY_STRICT"] = "true"
            subprocess_env.setdefault("SAUTO_PROXY_TIMEOUT", "8")

            self.log_lines.clear()
            self.log_lines.append(f"[web-api] Spouštím: {' '.join(command)}")
            self.log_lines.append(
                f"[web-api] Proxy profil: {run_mode_norm} ({'aktivni' if proxy_count > 0 else 'bez proxy konfigurace'})."
            )
            popen_kwargs: dict[str, Any] = {
                "cwd": ROOT_DIR,
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,
                "text": True,
                "bufsize": 1,
                "env": subprocess_env,
            }
            if os.name == "nt":
                popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)

            self.process = subprocess.Popen(
                command,
                **popen_kwargs,
            )
            self.last_started_at = time.time()
            self.last_finished_at = None
            self.last_exit_code = None
            self.paused = False
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
        callback = None
        output_file = "data/sauto_interesting.json"

        with self.lock:
            self.last_exit_code = exit_code
            self.last_finished_at = time.time()
            self.paused = False
            output_file = self.last_output_file
            self.process = None
            self.log_lines.append(f"[web-api] Dokončeno s exit code {exit_code}")
            callback = self.on_finished

        if callback is not None:
            try:
                callback(exit_code, output_file)
            except Exception:
                pass

    def _run_ps_control(self, action: str, pid: int) -> None:
        cmdlet = "Suspend-Process" if action == "suspend" else "Resume-Process"
        script = f"{cmdlet} -Id {pid} -ErrorAction Stop"
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if completed.returncode != 0:
            err = (completed.stderr or completed.stdout or "").strip()
            raise OSError(err or f"{cmdlet} failed.")

    def _windows_process_tree(self, root_pid: int) -> list[int]:
        if os.name != "nt":
            return [root_pid]

        script = (
            "Get-CimInstance Win32_Process "
            "| Select-Object ProcessId,ParentProcessId "
            "| ConvertTo-Json -Compress"
        )
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if completed.returncode != 0:
            return [root_pid]

        raw = (completed.stdout or "").strip()
        if not raw:
            return [root_pid]

        try:
            snapshot = json.loads(raw)
        except Exception:
            return [root_pid]

        if isinstance(snapshot, dict):
            snapshot = [snapshot]
        if not isinstance(snapshot, list):
            return [root_pid]

        children_by_parent: dict[int, list[int]] = {}
        for row in snapshot:
            if not isinstance(row, dict):
                continue
            try:
                child_pid = int(row.get("ProcessId"))
                parent_pid = int(row.get("ParentProcessId"))
            except Exception:
                continue
            children_by_parent.setdefault(parent_pid, []).append(child_pid)

        tree: list[int] = []
        queue = [int(root_pid)]
        visited: set[int] = set()
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            tree.append(current)
            queue.extend(children_by_parent.get(current, []))

        return tree if tree else [root_pid]

    def _suspend_process_tree(self, pid: int) -> None:
        if os.name != "nt":
            self._suspend_process(pid)
            return

        tree = self._windows_process_tree(pid)
        errors: list[str] = []
        for target_pid in reversed(tree):
            try:
                self._suspend_process(target_pid)
            except Exception as ex:
                errors.append(f"{target_pid}: {ex}")
        if errors:
            raise RuntimeError("; ".join(errors))

    def _resume_process_tree(self, pid: int) -> None:
        if os.name != "nt":
            self._resume_process(pid)
            return

        tree = self._windows_process_tree(pid)
        errors: list[str] = []
        for target_pid in tree:
            try:
                self._resume_process(target_pid)
            except Exception as ex:
                errors.append(f"{target_pid}: {ex}")
        if errors:
            raise RuntimeError("; ".join(errors))

    def _suspend_process(self, pid: int) -> None:
        if os.name == "nt":
            try:
                self._run_ps_control("suspend", pid)
            except Exception:
                # Fallback: low-level suspend if PowerShell cmdlet is unavailable.
                import ctypes

                PROCESS_SUSPEND_RESUME = 0x0800
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                ntdll = ctypes.WinDLL("ntdll", use_last_error=True)

                handle = kernel32.OpenProcess(PROCESS_SUSPEND_RESUME, False, pid)
                if not handle:
                    raise OSError("Unable to open process for suspend.")

                try:
                    status = ntdll.NtSuspendProcess(handle)
                    if status != 0:
                        raise OSError(f"NtSuspendProcess failed with status {status}.")
                finally:
                    kernel32.CloseHandle(handle)
            return

        os.kill(pid, signal.SIGSTOP)

    def _resume_process(self, pid: int) -> None:
        if os.name == "nt":
            try:
                self._run_ps_control("resume", pid)
            except Exception:
                import ctypes

                PROCESS_SUSPEND_RESUME = 0x0800
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                ntdll = ctypes.WinDLL("ntdll", use_last_error=True)

                handle = kernel32.OpenProcess(PROCESS_SUSPEND_RESUME, False, pid)
                if not handle:
                    raise OSError("Unable to open process for resume.")

                try:
                    status = ntdll.NtResumeProcess(handle)
                    if status != 0:
                        raise OSError(f"NtResumeProcess failed with status {status}.")
                finally:
                    kernel32.CloseHandle(handle)
            return

        os.kill(pid, signal.SIGCONT)

    def _graceful_stop(self, process: subprocess.Popen[str], timeout_s: int = 20) -> bool:
        try:
            if os.name == "nt":
                process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                process.send_signal(signal.SIGINT)
            process.wait(timeout=timeout_s)
            return True
        except Exception:
            return False

    def _persist_partial_output(self, output_file: str) -> int:
        try:
            rel_path = str(output_file or "").strip() or "data/sauto_interesting.json"
            result_path = (ROOT_DIR / rel_path).resolve()
            if ROOT_DIR not in result_path.parents and result_path != ROOT_DIR:
                return 0

            # If target already has data, keep it as source of truth.
            existing_items = load_item_records(result_path)
            if existing_items:
                return 0

            raw_items = load_item_records(RAW_OUTPUT_PATH)
            if not raw_items:
                return 0

            dump_json(result_path, raw_items)
            return len(raw_items)
        except Exception:
            return 0

    def pause(self) -> dict[str, Any]:
        with self.lock:
            if self.process is None or self.process.poll() is not None:
                raise RuntimeError("No running scraper process to pause.")
            if self.paused:
                return self._status_unlocked()
            pid = self.process.pid

        self._suspend_process_tree(pid)

        with self.lock:
            self.paused = True
            self.log_lines.append("[web-api] Scraper pozastaven.")
            return self._status_unlocked()

    def resume(self) -> dict[str, Any]:
        with self.lock:
            if self.process is None or self.process.poll() is not None:
                raise RuntimeError("No running scraper process to resume.")
            if not self.paused:
                return self._status_unlocked()
            pid = self.process.pid

        self._resume_process_tree(pid)

        with self.lock:
            self.paused = False
            self.log_lines.append("[web-api] Scraper pokračuje.")
            return self._status_unlocked()

    def stop(self) -> dict[str, Any]:
        with self.lock:
            if self.process is None or self.process.poll() is not None:
                raise RuntimeError("No running scraper process to stop.")
            process = self.process
            was_paused = self.paused
            output_file = self.last_output_file

        # Process may be paused; resume before terminate so shutdown can complete.
        if was_paused:
            try:
                self._resume_process_tree(process.pid)
            except Exception:
                pass

        graceful = self._graceful_stop(process, timeout_s=20)
        if not graceful and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except Exception:
                process.kill()

        restored_count = self._persist_partial_output(output_file)

        with self.lock:
            self.paused = False
            if graceful:
                self.log_lines.append("[web-api] Scraper byl ukončen uživatelem (graceful stop).")
            else:
                self.log_lines.append("[web-api] Scraper byl ukončen uživatelem (force stop).")
            if restored_count > 0:
                self.log_lines.append(f"[web-api] Obnoveno {restored_count} průběžných výsledků z raw exportu.")
            return self._status_unlocked()


class RunQueue:
    def __init__(self, scraper_runner: ScraperRunner) -> None:
        self._runner = scraper_runner
        self._runner.on_finished = self._on_runner_finished
        self._lock = threading.Lock()
        self._pending: deque[dict[str, Any]] = deque()
        self._history: deque[dict[str, Any]] = deque(maxlen=200)
        self._jobs: dict[str, dict[str, Any]] = {}
        self._active_job_id: str | None = None
        self.on_job_finished: Callable[[dict[str, Any]], None] | None = None

    def _new_job(self, output_file: str, project_id: str, run_mode: str, billable: bool, proxy_url: str) -> dict[str, Any]:
        now = time.time()
        job = {
            "job_id": f"job_{uuid.uuid4().hex[:12]}",
            "project_id": project_id or "default",
            "output_file": output_file,
            "run_mode": run_mode,
            "proxy_url": str(proxy_url or ""),
            "billable": bool(billable),
            "status": "queued",
            "created_at": now,
            "started_at": None,
            "finished_at": None,
            "exit_code": None,
            "error": None,
        }
        self._jobs[job["job_id"]] = job
        return job

    def _public_job(self, job: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(job, dict):
            return None
        safe = dict(job)
        if "proxy_url" in safe:
            safe["proxy_url"] = "***" if str(safe.get("proxy_url") or "").strip() else ""
        return safe

    def enqueue(
        self,
        output_file: str,
        project_id: str,
        run_mode: str = "free_proxy",
        billable: bool = True,
        proxy_url: str = "",
    ) -> dict[str, Any]:
        with self._lock:
            job = self._new_job(
                output_file=output_file,
                project_id=project_id,
                run_mode=run_mode,
                billable=billable,
                proxy_url=proxy_url,
            )
            self._pending.append(job)
            started_now = self._try_start_next_unlocked()
            return {
                "accepted": True,
                "started": started_now,
                "job": self._public_job(job),
                "queue": self.summary_unlocked(),
            }

    def _try_start_next_unlocked(self) -> bool:
        if self._runner.is_running() or not self._pending:
            return False

        while self._pending:
            job = self._pending.popleft()
            job["status"] = "running"
            job["started_at"] = time.time()
            self._active_job_id = job["job_id"]

            # Ensure each job starts from clean output data.
            resolved = (ROOT_DIR / job["output_file"]).resolve()
            dump_json(resolved, [])

            try:
                self._runner.start(
                    output_file=job["output_file"],
                    run_mode=str(job.get("run_mode") or "free_proxy"),
                    proxy_url=str(job.get("proxy_url") or ""),
                )
                return True
            except Exception as exc:
                job["status"] = "failed"
                job["finished_at"] = time.time()
                job["error"] = str(exc)
                job["exit_code"] = -1
                self._history.append(dict(job))
                self._active_job_id = None

        return False

    def _on_runner_finished(self, exit_code: int, output_file: str) -> None:
        completed_job: dict[str, Any] | None = None
        with self._lock:
            finished_at = time.time()
            active_job = self._jobs.get(self._active_job_id or "") if self._active_job_id else None
            if active_job is not None:
                active_job["finished_at"] = finished_at
                active_job["exit_code"] = exit_code
                active_job["status"] = "finished" if exit_code == 0 else "failed"
                active_job["output_file"] = output_file or active_job["output_file"]
                completed_job = dict(active_job)
                self._history.append(dict(active_job))
            self._active_job_id = None
            self._try_start_next_unlocked()

        if completed_job is not None and self.on_job_finished is not None:
            try:
                self.on_job_finished(completed_job)
            except Exception:
                pass

    def summary_unlocked(self) -> dict[str, Any]:
        active = self._jobs.get(self._active_job_id or "") if self._active_job_id else None
        return {
            "active_job": self._public_job(active),
            "pending_count": len(self._pending),
            "pending_jobs": [self._public_job(job) for job in list(self._pending)[:20]],
            "history": [self._public_job(job) for job in list(self._history)[-20:]],
        }

    def summary(self) -> dict[str, Any]:
        with self._lock:
            return self.summary_unlocked()

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return self._public_job(job)


class BillingLedger:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._data = self._load()

    def _empty(self) -> dict[str, Any]:
        return {
            "updated_at": time.time(),
            "rates": {
                "run_base_czk": BILLING_RUN_BASE_CZK,
                "item_czk": BILLING_ITEM_CZK,
                "api_call_czk": BILLING_API_CALL_CZK,
                "proxy_run_czk": BILLING_PROXY_RUN_CZK,
            },
            "projects": {},
            "events": [],
        }

    def _load(self) -> dict[str, Any]:
        data = load_json(self._path, self._empty())
        if not isinstance(data, dict):
            return self._empty()
        data.setdefault("rates", self._empty()["rates"])
        data.setdefault("projects", {})
        data.setdefault("events", [])
        data.setdefault("updated_at", time.time())
        return data

    def _save_unlocked(self) -> None:
        self._data["updated_at"] = time.time()
        dump_json(self._path, self._data)

    def _ensure_project_unlocked(self, project_id: str) -> dict[str, Any]:
        projects = self._data.setdefault("projects", {})
        project_key = (project_id or "default").strip() or "default"
        if project_key not in projects or not isinstance(projects.get(project_key), dict):
            projects[project_key] = {
                "project_id": project_key,
                "runs_total": 0,
                "runs_success": 0,
                "runs_failed": 0,
                "items_total": 0,
                "api_calls": 0,
                "costs": {
                    "runs_czk": 0.0,
                    "items_czk": 0.0,
                    "api_calls_czk": 0.0,
                    "proxy_czk": 0.0,
                },
                "total_czk": 0.0,
                "last_event_at": None,
            }
        return projects[project_key]

    def _recompute_total_unlocked(self, project: dict[str, Any]) -> None:
        costs = project.get("costs", {})
        project["total_czk"] = round(
            float(costs.get("runs_czk", 0.0))
            + float(costs.get("items_czk", 0.0))
            + float(costs.get("api_calls_czk", 0.0))
            + float(costs.get("proxy_czk", 0.0)),
            4,
        )

    def _append_event_unlocked(self, event: dict[str, Any]) -> None:
        events = self._data.setdefault("events", [])
        events.append(event)
        if len(events) > 1000:
            del events[:-1000]

    def _rates_unlocked(self) -> dict[str, float]:
        rates = self._data.get("rates", {})
        return {
            "run_base_czk": float(rates.get("run_base_czk", BILLING_RUN_BASE_CZK)),
            "item_czk": float(rates.get("item_czk", BILLING_ITEM_CZK)),
            "api_call_czk": float(rates.get("api_call_czk", BILLING_API_CALL_CZK)),
            "proxy_run_czk": float(rates.get("proxy_run_czk", BILLING_PROXY_RUN_CZK)),
        }

    def record_job_usage(self, job: dict[str, Any]) -> None:
        project_id = str(job.get("project_id") or "default")
        output_rel = str(job.get("output_file") or "data/sauto_interesting.json")
        exit_code = int(job.get("exit_code") or -1)
        run_mode = str(job.get("run_mode") or "free_proxy")
        billable = bool(job.get("billable", True))

        result_path = (ROOT_DIR / output_rel).resolve()
        item_count = len(load_result_items(result_path)) if result_path.exists() else 0

        with self._lock:
            project = self._ensure_project_unlocked(project_id)
            project["runs_total"] += 1
            if exit_code == 0:
                project["runs_success"] += 1
            else:
                project["runs_failed"] += 1

            project["items_total"] += item_count
            costs = project["costs"]
            if billable:
                costs["runs_czk"] = round(float(costs.get("runs_czk", 0.0)) + BILLING_RUN_BASE_CZK, 4)
                costs["items_czk"] = round(float(costs.get("items_czk", 0.0)) + (item_count * BILLING_ITEM_CZK), 4)

                if os.getenv("SAUTO_PROXY_LIST") or os.getenv("SAUTO_PROXY_URL"):
                    costs["proxy_czk"] = round(float(costs.get("proxy_czk", 0.0)) + BILLING_PROXY_RUN_CZK, 4)

            project["last_event_at"] = time.time()
            self._recompute_total_unlocked(project)
            self._append_event_unlocked(
                {
                    "type": "run_usage" if billable else "run_usage_free",
                    "at": project["last_event_at"],
                    "project_id": project_id,
                    "job_id": job.get("job_id"),
                    "exit_code": exit_code,
                    "item_count": item_count,
                    "run_mode": run_mode,
                    "billable": billable,
                    "charges": {
                        "run_base_czk": BILLING_RUN_BASE_CZK if billable else 0.0,
                        "items_czk": round(item_count * BILLING_ITEM_CZK, 4) if billable else 0.0,
                        "proxy_czk": (BILLING_PROXY_RUN_CZK if (os.getenv("SAUTO_PROXY_LIST") or os.getenv("SAUTO_PROXY_URL")) else 0.0) if billable else 0.0,
                    },
                }
            )
            self._save_unlocked()

    def record_api_call(self, project_id: str, path: str, method: str, status_code: int) -> None:
        with self._lock:
            project = self._ensure_project_unlocked(project_id)
            project["api_calls"] += 1
            costs = project["costs"]
            costs["api_calls_czk"] = round(float(costs.get("api_calls_czk", 0.0)) + BILLING_API_CALL_CZK, 4)
            project["last_event_at"] = time.time()
            self._recompute_total_unlocked(project)
            self._append_event_unlocked(
                {
                    "type": "api_call",
                    "at": project["last_event_at"],
                    "project_id": project_id,
                    "path": path,
                    "method": method,
                    "status_code": int(status_code),
                    "charge_czk": BILLING_API_CALL_CZK,
                }
            )
            self._save_unlocked()

    def get_rates(self) -> dict[str, float]:
        with self._lock:
            return self._rates_unlocked()

    def get_usage(self, project_id: str | None = None) -> dict[str, Any]:
        with self._lock:
            projects = self._data.get("projects", {})
            if not project_id:
                return {
                    "rates": self._rates_unlocked(),
                    "projects": dict(projects),
                    "updated_at": self._data.get("updated_at"),
                }

            key = (project_id or "default").strip() or "default"
            return {
                "rates": self._rates_unlocked(),
                "project": dict(projects.get(key, self._ensure_project_unlocked(key))),
                "updated_at": self._data.get("updated_at"),
            }

    def get_events(self, project_id: str | None = None, limit: int = 50) -> dict[str, Any]:
        with self._lock:
            events = self._data.get("events", [])
            if project_id:
                key = (project_id or "default").strip() or "default"
                events = [ev for ev in events if str(ev.get("project_id") or "") == key]
            limit_n = max(1, min(int(limit or 50), 500))
            return {
                "events": events[-limit_n:],
                "count": len(events),
            }


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


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(text: str) -> bytes:
    pad = "=" * ((4 - (len(text) % 4)) % 4)
    return base64.urlsafe_b64decode((text + pad).encode("ascii"))


def _auth_sign(message: str) -> str:
    digest = hmac.new(AUTH_SECRET.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
    return _b64url_encode(digest)


def _issue_auth_token(user: dict[str, Any]) -> str:
    now = int(time.time())
    payload = {
        "sub": str(user.get("id") or ""),
        "email": str(user.get("email") or "").lower(),
        "iat": now,
        "exp": now + max(60, AUTH_TOKEN_TTL_S),
    }
    header = {"alg": "HS256", "typ": "JWT"}
    header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}"
    signature = _auth_sign(signing_input)
    return f"{signing_input}.{signature}"


def _decode_auth_token(token: str) -> dict[str, Any] | None:
    token = str(token or "").strip()
    if not token:
        return None
    parts = token.split(".")
    if len(parts) != 3:
        return None
    header_b64, payload_b64, signature = parts
    signing_input = f"{header_b64}.{payload_b64}"
    expected_signature = _auth_sign(signing_input)
    if not hmac.compare_digest(signature, expected_signature):
        return None
    try:
        payload_raw = _b64url_decode(payload_b64).decode("utf-8")
        payload = json.loads(payload_raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        exp = int(payload.get("exp") or 0)
    except (TypeError, ValueError):
        return None
    if exp <= int(time.time()):
        return None
    return payload


def _hash_password(password: str, salt_hex: str) -> str:
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(password or "").encode("utf-8"),
        salt,
        120000,
    )
    return digest.hex()


def _load_users() -> list[dict[str, Any]]:
    data = load_json(USERS_DB_PATH, [])
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, dict)]


def _save_users(users: list[dict[str, Any]]) -> None:
    dump_json(USERS_DB_PATH, users)


def _find_user_by_email(email: str) -> dict[str, Any] | None:
    target = str(email or "").strip().lower()
    if not target:
        return None
    users = _load_users()
    for user in users:
        if str(user.get("email") or "").strip().lower() == target:
            return user
    return None


def _find_user_by_id(user_id: str) -> dict[str, Any] | None:
    target = str(user_id or "").strip()
    if not target:
        return None
    users = _load_users()
    for user in users:
        if str(user.get("id") or "").strip() == target:
            return user
    return None


def _update_user(updated_user: dict[str, Any]) -> None:
    target_id = str(updated_user.get("id") or "").strip()
    if not target_id:
        return
    users = _load_users()
    for idx, user in enumerate(users):
        if str(user.get("id") or "").strip() == target_id:
            users[idx] = updated_user
            _save_users(users)
            return


def _mask_proxy_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlsplit(raw)
    host = str(parsed.hostname or "")
    scheme = str(parsed.scheme or "")
    port = f":{parsed.port}" if parsed.port else ""
    if not host or not scheme:
        return "<configured>"
    return f"{scheme}://***@{host}{port}"


def _normalize_proxy_kind(value: str) -> str:
    return "paid_proxy" if str(value or "").strip().lower() == "paid_proxy" else "free_proxy"


def _normalize_user_proxy_profiles(user: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(user, dict):
        return []

    now = time.time()
    profiles_raw = user.get("proxy_profiles")
    profiles: list[dict[str, Any]] = []

    if isinstance(profiles_raw, list):
        for entry in profiles_raw:
            if not isinstance(entry, dict):
                continue
            profile_id = str(entry.get("id") or "").strip()
            if not profile_id:
                continue
            profiles.append(
                {
                    "id": profile_id,
                    "name": str(entry.get("name") or "").strip() or profile_id,
                    "kind": _normalize_proxy_kind(str(entry.get("kind") or "free_proxy")),
                    "proxy_url": str(entry.get("proxy_url") or "").strip(),
                    "created_at": float(entry.get("created_at") or now),
                    "updated_at": float(entry.get("updated_at") or now),
                }
            )

    # Backward compatibility for legacy free/paid fields.
    free_proxy_url = str(user.get("free_proxy_url") or "").strip()
    paid_proxy_url = str(user.get("paid_proxy_url") or "").strip()

    profile_by_id = {str(p.get("id") or ""): p for p in profiles}

    free_profile = profile_by_id.get(DEFAULT_FREE_PROXY_PROFILE_ID)
    if free_profile is None:
        free_profile = {
            "id": DEFAULT_FREE_PROXY_PROFILE_ID,
            "name": "Proxy profil A",
            "kind": "free_proxy",
            "proxy_url": free_proxy_url,
            "created_at": now,
            "updated_at": now,
        }
        profiles.insert(0, free_profile)
    elif free_proxy_url and not str(free_profile.get("proxy_url") or "").strip():
        free_profile["proxy_url"] = free_proxy_url

    paid_profile = profile_by_id.get(DEFAULT_PAID_PROXY_PROFILE_ID)
    if paid_profile is None:
        paid_profile = {
            "id": DEFAULT_PAID_PROXY_PROFILE_ID,
            "name": "Proxy profil B",
            "kind": "paid_proxy",
            "proxy_url": paid_proxy_url,
            "created_at": now,
            "updated_at": now,
        }
        profiles.append(paid_profile)
    elif paid_proxy_url and not str(paid_profile.get("proxy_url") or "").strip():
        paid_profile["proxy_url"] = paid_proxy_url

    # Keep legacy fields synchronized for any old client paths.
    user["proxy_profiles"] = profiles
    user["free_proxy_url"] = str(free_profile.get("proxy_url") or "").strip()
    user["paid_proxy_url"] = str(paid_profile.get("proxy_url") or "").strip()
    return profiles


def _public_proxy_profiles(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    public_items: list[dict[str, Any]] = []
    for profile in profiles:
        proxy_url = str(profile.get("proxy_url") or "").strip()
        public_items.append(
            {
                "id": str(profile.get("id") or ""),
                "name": str(profile.get("name") or ""),
                "kind": _normalize_proxy_kind(str(profile.get("kind") or "free_proxy")),
                "has_proxy_url": bool(proxy_url),
                "proxy_preview": _mask_proxy_url(proxy_url),
            }
        )
    return public_items


def _get_user_proxy_profile(user: dict[str, Any], run_mode: str, proxy_profile_id: str) -> dict[str, Any] | None:
    if not isinstance(user, dict):
        return None

    profiles = _normalize_user_proxy_profiles(user)
    requested_profile_id = str(proxy_profile_id or "").strip()
    if requested_profile_id:
        for profile in profiles:
            if str(profile.get("id") or "").strip() == requested_profile_id:
                return profile
        return None

    mode = _normalize_proxy_kind(run_mode)
    fallback_id = DEFAULT_PAID_PROXY_PROFILE_ID if mode == "paid_proxy" else DEFAULT_FREE_PROXY_PROFILE_ID
    for profile in profiles:
        if str(profile.get("id") or "").strip() == fallback_id:
            return profile

    for profile in profiles:
        if _normalize_proxy_kind(str(profile.get("kind") or "free_proxy")) == mode:
            return profile
    return None


def _auth_user_public(user: dict[str, Any]) -> dict[str, Any]:
    payment_status = str(user.get("payment_status") or "none").strip().lower() or "none"
    profiles = _normalize_user_proxy_profiles(user)
    profile_by_id = {str(p.get("id") or ""): p for p in profiles}
    free_proxy_url = str((profile_by_id.get(DEFAULT_FREE_PROXY_PROFILE_ID) or {}).get("proxy_url") or "").strip()
    paid_proxy_url = str((profile_by_id.get(DEFAULT_PAID_PROXY_PROFILE_ID) or {}).get("proxy_url") or "").strip()
    return {
        "id": str(user.get("id") or ""),
        "email": str(user.get("email") or "").lower(),
        "created_at": user.get("created_at"),
        "payment_status": payment_status,
        "cloud_access": _is_paid_status(payment_status),
        "free_proxy_access": bool(user.get("local_free_access", True)),
        "local_free_access": bool(user.get("local_free_access", True)),
        "has_free_proxy_config": bool(free_proxy_url),
        "has_paid_proxy_config": bool(paid_proxy_url),
        "free_proxy_preview": _mask_proxy_url(free_proxy_url),
        "paid_proxy_preview": _mask_proxy_url(paid_proxy_url),
        "proxy_profiles": _public_proxy_profiles(profiles),
        "stripe_customer_id": str(user.get("stripe_customer_id") or ""),
    }


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


def _parse_power_kw(item: dict[str, Any]) -> float | None:
    for key in ("power_kw", "engine_power"):
        value = item.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _should_exclude_result_item(item: dict[str, Any]) -> bool:
    power_kw = _parse_power_kw(item)
    detail_raw = item.get("detail_raw") if isinstance(item.get("detail_raw"), dict) else {}
    nested_result = detail_raw.get("result") if isinstance(detail_raw.get("result"), dict) else {}

    # Fallback for records where power is only present in nested detail payload.
    if power_kw is None:
        power_kw = _parse_power_kw(nested_result)

    if power_kw is not None and power_kw > MAX_REASONABLE_POWER_KW:
        return True

    text_parts = [
        item.get("name"),
        item.get("title"),
        item.get("description"),
        item.get("price_note"),
        item.get("windshield_note"),
        item.get("note"),
        nested_result.get("name"),
        nested_result.get("description"),
        nested_result.get("price_note"),
        nested_result.get("windshield_note"),
        nested_result.get("note"),
    ]
    text = "\n".join(part for part in text_parts if isinstance(part, str) and part.strip())
    return any(pattern.search(text) for pattern in RESULT_TEXT_REJECT_PATTERNS)


def load_result_items(result_path: Path) -> list[dict[str, Any]]:
    data = load_item_records(result_path)
    if not data:
        return []
    return [
        item
        for item in data
        if isinstance(item, dict) and not _should_exclude_result_item(item)
    ]


def load_item_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    try:
        with path.open("r", encoding="utf-8") as fh:
            raw = fh.read()
    except OSError:
        return []

    raw = raw.strip()
    if not raw:
        return []

    # 1) Try regular JSON list first.
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            return [data]
    except json.JSONDecodeError:
        pass

    # 2) Salvage leading valid JSON in case of trailing garbage.
    attempt = raw
    for _ in range(30):
        try:
            data = json.loads(attempt)
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
            if isinstance(data, dict):
                return [data]
            return []
        except json.JSONDecodeError as exc:
            if "Extra data" in str(exc):
                attempt = attempt[: exc.pos].rstrip()
            else:
                break

    # 3) JSONL fallback (robust for partial force-stopped exports).
    items: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            items.append(row)
    return items


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


def _fetch_sauto_page(params: dict[str, Any]) -> tuple[list[dict[str, Any]], int | None]:
    query = urlencode(params)
    url = f"{SAUTO_SEARCH_API}?{query}"
    try:
        with _urlopen_no_proxy(url, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        # Sauto returns 422 for too large offsets. Treat as end of pagination.
        if exc.code == 422:
            return [], None
        raise
    results = payload.get("results", []) if isinstance(payload, dict) else []
    pagination = payload.get("pagination", {}) if isinstance(payload, dict) else {}
    total = pagination.get("total") if isinstance(pagination, dict) else None
    try:
        total_count = int(total) if total is not None else None
    except (TypeError, ValueError):
        total_count = None
    return [item for item in results if isinstance(item, dict)], total_count


def _fetch_sauto_results(params: dict[str, Any]) -> list[dict[str, Any]]:
    results, _ = _fetch_sauto_page(params)
    return results


def _fetch_sauto_item_detail(item_id: int | str) -> dict[str, Any]:
    try:
        item_id_int = int(item_id)
    except (TypeError, ValueError):
        return {}

    url = SAUTO_ITEM_DETAIL_API.format(item_id_int)
    with _urlopen_no_proxy(url, timeout=15) as response:
        payload = json.loads(response.read().decode("utf-8"))
    result = payload.get("result", {}) if isinstance(payload, dict) else {}
    return result if isinstance(result, dict) else {}


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


def _collect_models_for_brand(
    brand: str,
    max_pages: int = 50,
    page_size: int = 200,
    include_counts: bool = True,
) -> list[dict[str, Any]]:
    models: dict[str, dict[str, Any]] = {}
    selected_brand = (brand or "").strip().lower()
    if not selected_brand:
        return []

    def _extract_brand_model_and_body(item: dict[str, Any]) -> tuple[str, str, str, str]:
        """Return (manufacturer_seo, model_seo, model_name, body_seo) from mixed payload formats."""
        if not isinstance(item, dict):
            return "", "", "", ""

        manufacturer_seo = str(item.get("manufacturer_seo") or "").strip().lower()
        model_seo = str(item.get("model_seo") or "").strip()
        model_name = str(item.get("model_name") or "").strip()
        body_seo = str(item.get("body_seo") or "").strip().lower()

        if not manufacturer_seo:
            manufacturer = item.get("manufacturer_cb") or {}
            manufacturer_seo = str(manufacturer.get("seo_name") or "").strip().lower()

        if not model_seo:
            model = item.get("model_cb") or {}
            model_seo = str(model.get("seo_name") or "").strip()
            if not model_name:
                model_name = str(model.get("name") or "").strip()
        if not body_seo:
            body = item.get("body_cb") or {}
            body_seo = str(body.get("seo_name") or "").strip().lower()

        detail_result = (item.get("detail_raw") or {}).get("result") or {}
        if not manufacturer_seo:
            manufacturer = detail_result.get("manufacturer_cb") or {}
            manufacturer_seo = str(manufacturer.get("seo_name") or "").strip().lower()
        if not model_seo:
            model = detail_result.get("model_cb") or {}
            model_seo = str(model.get("seo_name") or "").strip()
            if not model_name:
                model_name = str(model.get("name") or "").strip()
        if not body_seo:
            body = detail_result.get("body_cb") or {}
            body_seo = str(body.get("seo_name") or "").strip().lower()

        if not model_name:
            model_name = model_seo

        return manufacturer_seo, model_seo, model_name, body_seo

    def _is_valid_model_row(item: dict[str, Any]) -> tuple[bool, str, str]:
        manufacturer_seo, model_seo, model_name, body_seo = _extract_brand_model_and_body(item)
        if manufacturer_seo != selected_brand or not model_seo:
            return False, "", ""
        if body_seo and body_seo in EXCLUDED_MODEL_BODY_TYPES:
            return False, "", ""
        return True, model_seo, (model_name or model_seo)

    # Collect from live API using a brand-specific query to avoid missing models
    # for less frequent manufacturers when scanning global pages.
    total_rows: int | None = None
    for page in range(max_pages):
        offset = page * page_size
        batch, page_total = _fetch_sauto_page(
            {
                "category_id": 838,
                "limit": page_size,
                "offset": offset,
                "manufacturer_model_seo": selected_brand,
            }
        )
        if page_total is not None:
            total_rows = page_total
        if not batch:
            break

        for item in batch:
            ok, model_seo, model_name = _is_valid_model_row(item)
            if not ok:
                continue
            if model_seo not in models:
                models[model_seo] = {"label": model_name or model_seo, "count": 0}
            if include_counts:
                models[model_seo]["count"] = int(models[model_seo].get("count", 0)) + 1

        if total_rows is not None and (offset + len(batch)) >= total_rows:
            break

    if not include_counts:
        return [
            {
                "value": key,
                "label": str(models[key].get("label") or key),
            }
            for key in sorted(models.keys())
        ]

    return [
        {
            "value": key,
            "label": str(models[key].get("label") or key),
            "count": int(models[key].get("count") or 0),
        }
        for key in sorted(models.keys())
        if int(models[key].get("count") or 0) > 0
    ]


def _split_csv_values(raw: Any) -> list[str]:
    return [part.strip().lower() for part in str(raw or "").split(",") if part and part.strip()]


def _collect_model_counts_for_brand_with_config(
    brand: str,
    config: dict[str, Any] | None,
    max_pages: int = 80,
    page_size: int = 200,
) -> list[dict[str, Any]]:
    selected_brand = (brand or "").strip().lower()
    if not selected_brand:
        return []

    cfg = config if isinstance(config, dict) else {}
    selected_brands = set(_split_csv_values(cfg.get("manufacturer_seo_name")))
    excluded_brands = set(_split_csv_values(cfg.get("exclude_manufacturer_seo_name")))
    active_brands = {b for b in selected_brands if b and b not in excluded_brands}

    # model_seo_name is a flat list without brand mapping. When multiple brands are
    # selected we must not force the current brand to match every selected model,
    # otherwise one brand can accidentally hide models of another brand.
    apply_selected_model_filter = (not active_brands) or (active_brands == {selected_brand})

    selected_models = _split_csv_values(cfg.get("model_seo_name"))
    excluded_models = set(_split_csv_values(cfg.get("exclude_model_seo_name")))
    excluded_bodies = set(_split_csv_values(cfg.get("exclude_body_seo")))

    pairs = [f"{selected_brand}:{m}" for m in selected_models if apply_selected_model_filter and m and m not in excluded_models]
    manufacturer_model_seo = "|".join(pairs) if pairs else selected_brand

    params: dict[str, Any] = {
        "category_id": 838,
        "limit": page_size,
        "offset": 0,
        "manufacturer_model_seo": manufacturer_model_seo,
    }

    body_seo = ",".join([b for b in _split_csv_values(cfg.get("body_seo")) if b not in excluded_bodies])
    if body_seo:
        params["vehicle_body_seo"] = body_seo

    fuel_seo = str(cfg.get("fuel_seo") or "").strip().lower()
    if fuel_seo:
        params["fuel_seo"] = fuel_seo

    condition_seo = str(cfg.get("condition_seo") or "").strip().lower()
    if condition_seo:
        params["condition_seo"] = condition_seo

    gearbox = str(cfg.get("gearbox_filter") or "").strip().lower()
    if gearbox:
        params["gearbox_seo"] = gearbox

    drive = str(cfg.get("drive_filter") or "").strip().lower()
    if drive:
        params["drive_seo"] = drive

    for src_key, dst_key in (("price_from", "price_from"), ("price_to", "price_to"), ("tachometer_from", "tachometer_from"), ("tachometer_to", "tachometer_to")):
        value = str(cfg.get(src_key) or "").strip()
        if value:
            params[dst_key] = value

    counts: dict[str, dict[str, Any]] = {}
    total_rows: int | None = None
    for page in range(max_pages):
        query = dict(params)
        query["offset"] = page * page_size
        batch, page_total = _fetch_sauto_page(query)
        if page_total is not None:
            total_rows = page_total
        if not batch:
            break

        for item in batch:
            manufacturer = item.get("manufacturer_cb") or {}
            model = item.get("model_cb") or {}
            body = item.get("body_cb") or item.get("vehicle_body_cb") or {}

            manufacturer_seo = str(manufacturer.get("seo_name") or "").strip().lower()
            model_seo = str(model.get("seo_name") or "").strip().lower()
            model_name = str(model.get("name") or model_seo).strip()
            body_seo_item = str(body.get("seo_name") or item.get("body_seo") or "").strip().lower()

            if manufacturer_seo != selected_brand or not model_seo:
                continue
            if model_seo in excluded_models:
                continue
            if apply_selected_model_filter and selected_models and model_seo not in selected_models:
                continue
            if body_seo_item and (body_seo_item in EXCLUDED_MODEL_BODY_TYPES or body_seo_item in excluded_bodies):
                continue

            if model_seo not in counts:
                counts[model_seo] = {"label": model_name or model_seo, "count": 0}
            counts[model_seo]["count"] = int(counts[model_seo].get("count") or 0) + 1

        if total_rows is not None and ((page * page_size) + len(batch)) >= total_rows:
            break

    return [
        {
            "value": key,
            "label": str(counts[key].get("label") or key),
            "count": int(counts[key].get("count") or 0),
        }
        for key in sorted(counts.keys())
        if int(counts[key].get("count") or 0) > 0
    ]


def _collect_equipment(max_pages: int = 20, page_size: int = 200) -> list[dict[str, str]]:
    equipment: dict[str, str] = {}
    # Primary: raw scraped data (equipment_cb in detail_raw.result)
    for data_path in [RAW_OUTPUT_PATH, DEFAULT_RESULTS_PATH]:
        if data_path.exists():
            try:
                data = load_item_records(data_path)
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
    allow_origins=["*"] if CORS_ALLOW_ANY_ORIGIN else CORS_ALLOW_ORIGINS,
    allow_origin_regex=CORS_ALLOW_ORIGIN_REGEX or None,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def api_key_guard(request: Request, call_next):
    path = request.url.path
    method = request.method.upper()
    api_key = request.headers.get("x-api-key", "").strip()
    project_header = request.headers.get("x-project-id", "").strip()
    auth_header = request.headers.get("authorization", "")

    def _cors_json(status_code: int, content: dict[str, Any]) -> JSONResponse:
        # Ensure browser clients receive auth errors as normal HTTP responses
        # instead of opaque CORS failures.
        headers: dict[str, str] = {}
        origin = request.headers.get("origin", "").strip()
        if CORS_ALLOW_ANY_ORIGIN:
            headers["Access-Control-Allow-Origin"] = "*"
        elif origin and (origin in CORS_ALLOW_ORIGIN_SET or (CORS_ALLOW_ORIGIN_PATTERN is not None and CORS_ALLOW_ORIGIN_PATTERN.match(origin))):
            headers["Access-Control-Allow-Origin"] = origin
            headers["Vary"] = "Origin"
        return JSONResponse(
            status_code=status_code,
            content=content,
            headers=headers,
        )

    # Let CORS middleware handle preflight requests. Blocking OPTIONS here
    # causes browser-side "Failed to fetch" before the actual API call.
    if method == "OPTIONS":
        return await call_next(request)

    if path in AUTH_REQUIRED_PATHS:
        token = ""
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
        claims = _decode_auth_token(token)
        if claims is None:
            return _cors_json(401, {"detail": "Missing or invalid auth token."})
        request.state.auth_user = claims

    if not API_KEYS:
        response = await call_next(request)
    else:
        if path.startswith("/api/") and method in WRITE_HTTP_METHODS and path != "/api/billing/webhook/stripe":
            if api_key not in API_KEYS:
                return _cors_json(401, {"detail": "Missing or invalid API key."})
        response = await call_next(request)

    # Bill only integration traffic: requests that carry x-api-key.
    if path.startswith("/api/") and api_key:
        project_id = project_header or f"integration-{api_key[:10]}"
        billing_ledger.record_api_call(
            project_id=project_id,
            path=path,
            method=method,
            status_code=int(response.status_code),
        )

    return response

runner = ScraperRunner()
run_queue = RunQueue(runner)
billing_ledger = BillingLedger(BILLING_LEDGER_PATH)
run_queue.on_job_finished = billing_ledger.record_job_usage


@app.get("/api/health")
def health() -> dict[str, Any]:
    uptime_s = int(time.time() - API_START_TIME)
    return {
        "status": "ok",
        "version": API_VERSION,
        "uptime_s": uptime_s,
        "python": sys.version.split()[0],
    }


@app.post("/api/auth/signup")
def auth_signup(payload: SignupPayload) -> dict[str, Any]:
    email = str(payload.email or "").strip().lower()
    password = str(payload.password or "")
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Valid email is required.")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Password must have at least 6 characters.")

    existing = _find_user_by_email(email)
    if existing is not None:
        raise HTTPException(status_code=409, detail="User with this email already exists.")

    salt_hex = secrets.token_hex(16)
    user = {
        "id": f"usr_{uuid.uuid4().hex[:12]}",
        "email": email,
        "password_hash": _hash_password(password, salt_hex),
        "password_salt": salt_hex,
        "created_at": time.time(),
        "payment_status": "none",
        "local_free_access": True,
        "free_proxy_url": "",
        "paid_proxy_url": "",
        "stripe_customer_id": "",
        "stripe_subscription_id": "",
        "stripe_checkout_session_id": "",
        "stripe_last_event_at": None,
    }

    users = _load_users()
    users.append(user)
    _save_users(users)

    token = _issue_auth_token(user)
    return {"token": token, "user": _auth_user_public(user)}


@app.post("/api/auth/login")
def auth_login(payload: LoginPayload) -> dict[str, Any]:
    email = str(payload.email or "").strip().lower()
    password = str(payload.password or "")
    user = _find_user_by_email(email)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    salt_hex = str(user.get("password_salt") or "")
    expected_hash = str(user.get("password_hash") or "")
    if not salt_hex or not expected_hash:
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    candidate_hash = _hash_password(password, salt_hex)
    if not hmac.compare_digest(candidate_hash, expected_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    token = _issue_auth_token(user)
    return {"token": token, "user": _auth_user_public(user)}


@app.get("/api/auth/me")
def auth_me(request: Request) -> dict[str, Any]:
    claims = getattr(request.state, "auth_user", None)
    if not isinstance(claims, dict):
        raise HTTPException(status_code=401, detail="Missing or invalid auth token.")
    user_id = str(claims.get("sub") or "").strip()
    db_user = _find_user_by_id(user_id) if user_id else None
    if db_user is not None:
        return {"user": _auth_user_public(db_user)}
    return {
        "user": {
            "id": str(claims.get("sub") or ""),
            "email": str(claims.get("email") or "").lower(),
            "payment_status": "none",
            "cloud_access": False,
            "free_proxy_access": True,
            "local_free_access": True,
        }
    }


@app.get("/api/proxy/config")
def get_proxy_config(request: Request) -> dict[str, Any]:
    user = _resolve_request_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Missing or invalid auth token.")

    profiles = _normalize_user_proxy_profiles(user)
    profile_by_id = {str(p.get("id") or ""): p for p in profiles}
    free_proxy_url = str((profile_by_id.get(DEFAULT_FREE_PROXY_PROFILE_ID) or {}).get("proxy_url") or "").strip()
    paid_proxy_url = str((profile_by_id.get(DEFAULT_PAID_PROXY_PROFILE_ID) or {}).get("proxy_url") or "").strip()
    return {
        "free_proxy_url": _mask_proxy_url(free_proxy_url),
        "paid_proxy_url": _mask_proxy_url(paid_proxy_url),
        "has_free_proxy_config": bool(free_proxy_url),
        "has_paid_proxy_config": bool(paid_proxy_url),
        "profiles": _public_proxy_profiles(profiles),
    }


@app.put("/api/proxy/config")
def set_proxy_config(payload: ProxyConfigPayload, request: Request) -> dict[str, Any]:
    user = _resolve_request_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Missing or invalid auth token.")

    existing_profiles = _normalize_user_proxy_profiles(user)
    existing_by_id = {str(p.get("id") or ""): p for p in existing_profiles}
    next_profiles = [dict(p) for p in existing_profiles]

    if payload.profiles is not None:
        next_profiles = []
        seen_ids: set[str] = set()
        now = time.time()
        for item in payload.profiles:
            item_data = item if isinstance(item, dict) else {
                "id": getattr(item, "id", ""),
                "name": getattr(item, "name", ""),
                "kind": getattr(item, "kind", "free_proxy"),
                "proxy_url": getattr(item, "proxy_url", None),
            }

            profile_id = str(item_data.get("id") or "").strip()
            if not profile_id:
                raise HTTPException(status_code=400, detail="Each proxy profile must include id.")
            if profile_id in seen_ids:
                raise HTTPException(status_code=400, detail=f"Duplicate proxy profile id '{profile_id}'.")
            seen_ids.add(profile_id)

            previous = existing_by_id.get(profile_id) or {}
            kind = _normalize_proxy_kind(str(item_data.get("kind") or "free_proxy"))
            if profile_id == DEFAULT_FREE_PROXY_PROFILE_ID:
                kind = "free_proxy"
            elif profile_id == DEFAULT_PAID_PROXY_PROFILE_ID:
                kind = "paid_proxy"
            name = str(item_data.get("name") or "").strip() or profile_id

            if item_data.get("proxy_url") is None:
                proxy_url = str(previous.get("proxy_url") or "").strip()
            else:
                proxy_url = str(item_data.get("proxy_url") or "").strip()

            if proxy_url:
                try:
                    proxy_url = _validate_proxy_url(proxy_url)
                except ValueError as exc:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid proxy_url for profile '{profile_id}': {exc}",
                    ) from exc

            next_profiles.append(
                {
                    "id": profile_id,
                    "name": name,
                    "kind": kind,
                    "proxy_url": proxy_url,
                    "created_at": float(previous.get("created_at") or now),
                    "updated_at": now,
                }
            )

    next_by_id = {str(p.get("id") or ""): p for p in next_profiles}
    now = time.time()

    if DEFAULT_FREE_PROXY_PROFILE_ID not in next_by_id:
        previous = existing_by_id.get(DEFAULT_FREE_PROXY_PROFILE_ID) or {}
        next_profiles.insert(
            0,
            {
                "id": DEFAULT_FREE_PROXY_PROFILE_ID,
                "name": str(previous.get("name") or "").strip() or "Proxy profil A",
                "kind": "free_proxy",
                "proxy_url": str(previous.get("proxy_url") or "").strip(),
                "created_at": float(previous.get("created_at") or now),
                "updated_at": now,
            },
        )
        next_by_id = {str(p.get("id") or ""): p for p in next_profiles}

    if DEFAULT_PAID_PROXY_PROFILE_ID not in next_by_id:
        previous = existing_by_id.get(DEFAULT_PAID_PROXY_PROFILE_ID) or {}
        next_profiles.append(
            {
                "id": DEFAULT_PAID_PROXY_PROFILE_ID,
                "name": str(previous.get("name") or "").strip() or "Proxy profil B",
                "kind": "paid_proxy",
                "proxy_url": str(previous.get("proxy_url") or "").strip(),
                "created_at": float(previous.get("created_at") or now),
                "updated_at": now,
            }
        )
        next_by_id = {str(p.get("id") or ""): p for p in next_profiles}

    if payload.free_proxy_url is not None:
        free_proxy_url = str(payload.free_proxy_url or "").strip()
        if free_proxy_url:
            try:
                free_proxy_url = _validate_proxy_url(free_proxy_url)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=f"Invalid free_proxy_url: {exc}") from exc
        free_profile = next_by_id[DEFAULT_FREE_PROXY_PROFILE_ID]
        free_profile["proxy_url"] = free_proxy_url
        free_profile["updated_at"] = time.time()

    if payload.paid_proxy_url is not None:
        paid_proxy_url = str(payload.paid_proxy_url or "").strip()
        if paid_proxy_url:
            try:
                paid_proxy_url = _validate_proxy_url(paid_proxy_url)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=f"Invalid paid_proxy_url: {exc}") from exc
        paid_profile = next_by_id[DEFAULT_PAID_PROXY_PROFILE_ID]
        paid_profile["proxy_url"] = paid_proxy_url
        paid_profile["updated_at"] = time.time()

    free_proxy_url = str(next_by_id[DEFAULT_FREE_PROXY_PROFILE_ID].get("proxy_url") or "").strip()
    paid_proxy_url = str(next_by_id[DEFAULT_PAID_PROXY_PROFILE_ID].get("proxy_url") or "").strip()

    user["proxy_profiles"] = next_profiles
    user["free_proxy_url"] = free_proxy_url
    user["paid_proxy_url"] = paid_proxy_url
    user["proxy_updated_at"] = time.time()
    _update_user(user)

    return {
        "saved": True,
        "free_proxy_url": _mask_proxy_url(free_proxy_url),
        "paid_proxy_url": _mask_proxy_url(paid_proxy_url),
        "has_free_proxy_config": bool(free_proxy_url),
        "has_paid_proxy_config": bool(paid_proxy_url),
        "profiles": _public_proxy_profiles(next_profiles),
    }


@app.post("/api/proxy/test")
def test_proxy_connection(payload: ProxyTestPayload, request: Request) -> dict[str, Any]:
    user = _resolve_request_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Missing or invalid auth token.")

    raw_proxy_url = str(payload.proxy_url or "").strip()
    if not raw_proxy_url:
        raise HTTPException(status_code=400, detail="Proxy URL je povinna.")

    try:
        proxy_url = _validate_proxy_url(raw_proxy_url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Neplatna proxy URL: {exc}") from exc

    proxy_handler = ProxyHandler({"http": proxy_url, "https": proxy_url})
    probe_targets = [
        "https://api.ipify.org?format=json",
        "https://ifconfig.me/all.json",
    ]
    last_error = "Proxy test selhal."

    for target in probe_targets:
        opener = build_opener(proxy_handler)
        req = UrlRequest(
            target,
            headers={
                "User-Agent": "sauto-proxy-check/1.0",
                "Accept": "application/json,text/plain,*/*",
            },
        )
        try:
            with opener.open(req, timeout=12) as response:
                body = response.read().decode("utf-8", errors="ignore")
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                external_ip = str(parsed.get("ip") or parsed.get("ip_addr") or parsed.get("ip_address") or "").strip()
            else:
                external_ip = ""
            return {
                "ok": True,
                "message": "Proxy je dostupna a odpovida.",
                "external_ip": external_ip,
            }
        except Exception as exc:
            last_error = str(exc)
            continue

    raise HTTPException(status_code=400, detail=f"Proxy test selhal: {last_error}")


def _resolve_request_user(request: Request) -> dict[str, Any] | None:
    claims = getattr(request.state, "auth_user", None)
    if not isinstance(claims, dict):
        auth_header = str(request.headers.get("authorization", "") or "")
        token = ""
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
        if token:
            claims = _decode_auth_token(token)
        if not isinstance(claims, dict):
            return None

    user_id = str(claims.get("sub") or "").strip()
    if user_id:
        by_id = _find_user_by_id(user_id)
        if by_id is not None:
            return by_id

    email = str(claims.get("email") or "").strip().lower()
    if email:
        return _find_user_by_email(email)
    return None


def _user_has_cloud_access(user: dict[str, Any] | None) -> bool:
    if not isinstance(user, dict):
        return False
    return _is_paid_status(str(user.get("payment_status") or "none"))


@app.get("/api/billing/access")
def get_billing_access(request: Request) -> dict[str, Any]:
    user = _resolve_request_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Missing or invalid auth token.")

    return {
        "provider": PAYMENT_PROVIDER,
        "can_run_cloud": _user_has_cloud_access(user),
        "can_run_free_proxy": bool(user.get("local_free_access", True)),
        "can_run_local_free": bool(user.get("local_free_access", True)),
        "payment_status": str(user.get("payment_status") or "none"),
        "checkout_available": bool(STRIPE_PAYMENT_LINK_URL or (STRIPE_SECRET_KEY and (STRIPE_PRICE_ID or STRIPE_DEFAULT_AMOUNT_CENTS > 0))),
    }


@app.post("/api/billing/checkout-session")
def create_checkout_session(payload: CheckoutSessionPayload, request: Request) -> dict[str, Any]:
    user = _resolve_request_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Missing or invalid auth token.")

    if STRIPE_PAYMENT_LINK_URL:
        return {"url": STRIPE_PAYMENT_LINK_URL, "provider": "stripe", "mode": "payment_link"}

    if PAYMENT_PROVIDER != "stripe":
        raise HTTPException(status_code=501, detail="Payment provider is not configured.")

    success_url = str(payload.success_url or STRIPE_SUCCESS_URL).strip()
    cancel_url = str(payload.cancel_url or STRIPE_CANCEL_URL).strip()
    if not success_url or not cancel_url:
        raise HTTPException(status_code=400, detail="Missing success/cancel redirect URL.")

    stripe_payload: dict[str, Any] = {
        "mode": "payment",
        "success_url": success_url,
        "cancel_url": cancel_url,
        "client_reference_id": str(user.get("id") or ""),
        "customer_email": str(user.get("email") or ""),
    }
    if STRIPE_PRICE_ID:
        stripe_payload["line_items[0][price]"] = STRIPE_PRICE_ID
        stripe_payload["line_items[0][quantity]"] = "1"
    else:
        stripe_payload["line_items[0][price_data][currency]"] = STRIPE_DEFAULT_CURRENCY
        stripe_payload["line_items[0][price_data][unit_amount]"] = str(max(100, STRIPE_DEFAULT_AMOUNT_CENTS))
        stripe_payload["line_items[0][price_data][product_data][name]"] = "Sauto Scraper Cloud credit"
        stripe_payload["line_items[0][quantity]"] = "1"

    try:
        session = _stripe_request("/v1/checkout/sessions", stripe_payload)
    except HTTPError as exc:
        detail = exc.read().decode("utf-8") if hasattr(exc, "read") else str(exc)
        raise HTTPException(status_code=502, detail=f"Stripe checkout failed: {detail}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Stripe checkout failed: {exc}") from exc

    checkout_url = str(session.get("url") or "").strip()
    session_id = str(session.get("id") or "").strip()
    customer_id = str(session.get("customer") or user.get("stripe_customer_id") or "").strip()
    if checkout_url:
        user["stripe_checkout_session_id"] = session_id
        if customer_id:
            user["stripe_customer_id"] = customer_id
        _update_user(user)
    return {"url": checkout_url, "provider": "stripe", "mode": "checkout_session", "session_id": session_id}


@app.post("/api/billing/webhook/stripe")
async def stripe_webhook(request: Request) -> dict[str, Any]:
    raw = await request.body()
    signature = request.headers.get("stripe-signature", "")
    if not _stripe_verify_signature(raw, signature):
        raise HTTPException(status_code=400, detail="Invalid Stripe signature.")

    try:
        event = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {exc}") from exc

    if not isinstance(event, dict):
        raise HTTPException(status_code=400, detail="Invalid Stripe payload.")

    event_type = str(event.get("type") or "")
    obj = (((event.get("data") or {}).get("object") or {}) if isinstance(event.get("data"), dict) else {})
    if not isinstance(obj, dict):
        obj = {}

    customer_id = str(obj.get("customer") or "").strip()
    reference_user_id = str(obj.get("client_reference_id") or "").strip()

    user = _find_user_by_id(reference_user_id) if reference_user_id else None
    if user is None and customer_id:
        for existing in _load_users():
            if str(existing.get("stripe_customer_id") or "").strip() == customer_id:
                user = existing
                break

    if user is None:
        return {"ok": True, "ignored": True}

    paid_event_types = {
        "checkout.session.completed",
        "invoice.payment_succeeded",
        "customer.subscription.created",
        "customer.subscription.updated",
    }
    unpaid_event_types = {
        "invoice.payment_failed",
        "customer.subscription.deleted",
    }

    if event_type in paid_event_types:
        user["payment_status"] = "paid"
    elif event_type in unpaid_event_types:
        user["payment_status"] = "none"

    if customer_id:
        user["stripe_customer_id"] = customer_id
    subscription_id = str(obj.get("subscription") or obj.get("id") or "").strip()
    if subscription_id and event_type.startswith("customer.subscription"):
        user["stripe_subscription_id"] = subscription_id
    user["stripe_last_event_at"] = time.time()
    _update_user(user)

    return {"ok": True}


@app.get("/api/params")
def get_params() -> dict[str, Any]:
    params = load_json(PARAMS_PATH, {})
    return {"params": params}


@app.put("/api/params")
def update_params(payload: ParamsPayload) -> dict[str, Any]:
    if not isinstance(payload.params, dict):
        raise HTTPException(status_code=400, detail="Invalid params payload.")

    normalized = {str(key): "" if value is None else str(value) for key, value in payload.params.items()}
    # Keep core paging/category settings stable for now. The frontend hides these
    # fields and the backend enforces them for all UI-saved runs.
    normalized.update(LOCKED_SEARCH_DEFAULTS)
    dump_json(PARAMS_PATH, normalized)
    return {"saved": True, "params": normalized}


@app.post("/api/run")
def run_scraper(payload: RunPayload, request: Request) -> dict[str, Any]:
    output_file = payload.output_file.strip() or "data/sauto_interesting.json"
    project_id = (payload.project_id or "default").strip() or "default"
    requested_mode = str(payload.run_mode or "free_proxy").strip().lower()
    run_mode = "paid_proxy" if requested_mode == "paid_proxy" else "free_proxy"

    if os.path.isabs(output_file):
        raise HTTPException(status_code=400, detail="Use a relative output_file path.")

    resolved = (ROOT_DIR / output_file).resolve()
    if ROOT_DIR not in resolved.parents and resolved != ROOT_DIR:
        raise HTTPException(status_code=400, detail="output_file must stay inside project directory.")

    user = _resolve_request_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Login is required for scraping jobs.")

    selected_profile = _get_user_proxy_profile(user, run_mode, payload.proxy_profile_id or "")
    if selected_profile is None:
        raise HTTPException(status_code=400, detail="Nelze spustit úlohu: Vybraný proxy profil neexistuje.")

    run_mode = _normalize_proxy_kind(str(selected_profile.get("kind") or run_mode))
    selected_proxy = str(selected_profile.get("proxy_url") or "").strip()
    if not selected_proxy:
        raise HTTPException(status_code=400, detail="Nelze spustit úlohu: Chybí konfigurace proxy.")

    try:
        validated_proxy = _validate_proxy_url(selected_proxy)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid proxy configuration: {exc}") from exc

    return run_queue.enqueue(
        output_file=output_file,
        project_id=project_id,
        run_mode=run_mode,
        billable=False,
        proxy_url=validated_proxy,
    )


@app.post("/api/pause")
def pause_scraper() -> dict[str, Any]:
    try:
        status = runner.pause()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to pause scraper: {exc}") from exc
    return {"paused": True, "status": status}


@app.post("/api/resume")
def resume_scraper() -> dict[str, Any]:
    try:
        status = runner.resume()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to resume scraper: {exc}") from exc
    return {"resumed": True, "status": status}


@app.post("/api/stop")
def stop_scraper() -> dict[str, Any]:
    try:
        status = runner.stop()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to stop scraper: {exc}") from exc
    return {"stopped": True, "status": status}


@app.get("/api/status")
def get_status() -> dict[str, Any]:
    runner_status = runner.status()
    return {
        **runner_status,
        "runner": runner_status,
        "queue": run_queue.summary(),
    }


@app.get("/api/jobs")
def get_jobs() -> dict[str, Any]:
    return run_queue.summary()


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    job = run_queue.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return {"job": job}


@app.get("/api/billing/rates")
def get_billing_rates() -> dict[str, Any]:
    return {"rates": billing_ledger.get_rates()}


@app.get("/api/billing/usage")
def get_billing_usage(project_id: str | None = None) -> dict[str, Any]:
    return billing_ledger.get_usage(project_id=project_id)


@app.get("/api/billing/events")
def get_billing_events(project_id: str | None = None, limit: int = 50) -> dict[str, Any]:
    return billing_ledger.get_events(project_id=project_id, limit=limit)


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
    cache_ok = cached_collector_version == 7

    if not force_refresh and cache_ok and _is_fresh(cached_ts) and isinstance(cached_items, list):
        return {"brand": selected_brand, "items": cached_items, "cached": True, "updated_at": cached_ts}

    try:
        items = _collect_models_for_brand(selected_brand, include_counts=False)
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
        "collector_version": 7,
    }
    _save_catalog_cache(cache)

    return {"brand": selected_brand, "items": items, "cached": False, "updated_at": now}


@app.get("/api/catalog/model-counts")
def get_catalog_model_counts(brand: str, force_refresh: bool = False) -> dict[str, Any]:
    selected_brand = (brand or "").strip().lower()
    if not selected_brand:
        return {"brand": "", "items": [], "cached": True, "updated_at": None}

    cache = _load_catalog_cache()
    counts_cache = cache.get("model_counts", {}) if isinstance(cache, dict) else {}
    brand_cache = counts_cache.get(selected_brand, {}) if isinstance(counts_cache, dict) else {}
    cached_items = brand_cache.get("items", []) if isinstance(brand_cache, dict) else []
    cached_ts = brand_cache.get("updated_at") if isinstance(brand_cache, dict) else None

    if not force_refresh and _is_fresh(cached_ts) and isinstance(cached_items, list) and cached_items:
        return {"brand": selected_brand, "items": cached_items, "cached": True, "updated_at": cached_ts}

    try:
        items = _collect_models_for_brand(selected_brand, include_counts=True)
    except Exception as exc:
        if isinstance(cached_items, list) and cached_items:
            return {
                "brand": selected_brand,
                "items": cached_items,
                "cached": True,
                "updated_at": cached_ts,
                "warning": f"Using cache: {exc}",
            }
        raise HTTPException(status_code=502, detail=f"Unable to fetch Sauto model counts for '{selected_brand}': {exc}") from exc

    now = int(time.time())
    if not isinstance(cache, dict):
        cache = {}
    if not isinstance(cache.get("model_counts"), dict):
        cache["model_counts"] = {}
    cache["model_counts"][selected_brand] = {
        "updated_at": now,
        "items": items,
    }
    _save_catalog_cache(cache)

    return {"brand": selected_brand, "items": items, "cached": False, "updated_at": now}


@app.post("/api/catalog/model-counts")
def get_catalog_model_counts_with_config(payload: ModelCountsPayload) -> dict[str, Any]:
    selected_brand = (payload.brand or "").strip().lower()
    if not selected_brand:
        return {"brand": "", "items": [], "cached": False, "updated_at": int(time.time())}

    try:
        items = _collect_model_counts_for_brand_with_config(selected_brand, payload.config)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Unable to fetch Sauto model counts for '{selected_brand}' with config: {exc}") from exc

    return {
        "brand": selected_brand,
        "items": items,
        "cached": False,
        "updated_at": int(time.time()),
    }
@app.post("/api/catalog/estimate")
def get_catalog_estimate(payload: CatalogEstimatePayload) -> dict[str, Any]:
    cfg = payload.config if isinstance(payload.config, dict) else {}

    def _csv_set(key: str) -> set[str]:
        raw = str(cfg.get(key) or "")
        return {part.strip().lower() for part in raw.split(",") if part.strip()}

    selected_brands = _csv_set("manufacturer_seo_name")
    excluded_brands = _csv_set("exclude_manufacturer_seo_name")
    selected_models = _csv_set("model_seo_name")
    excluded_models = _csv_set("exclude_model_seo_name")

    brands = [b for b in selected_brands if b and b not in excluded_brands]
    if not brands:
        return {
            "count": 0,
            "note": "No selected brands. Count is based on live active-ad model catalog.",
            "brands": [],
        }

    try:
        total_count = 0
        by_brand: dict[str, int] = {}
        for brand in sorted(brands):
            items = _collect_models_for_brand(brand)
            brand_count = 0
            for item in items:
                model_value = str(item.get("value") or "").strip().lower()
                if not model_value:
                    continue
                if selected_models and model_value not in selected_models:
                    continue
                if model_value in excluded_models:
                    continue
                brand_count += int(item.get("count") or 0)
            by_brand[brand] = brand_count
            total_count += brand_count

        return {
            "count": int(total_count),
            "by_brand": by_brand,
            "note": "Count is based on live active ads from Sauto model catalog (brand/model filters supported; other filters are not provided by Sauto catalog API).",
        }
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Unable to estimate Sauto ad count: {exc}") from exc


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

    # Collect unique body types strictly from Sauto category_id=838 (automobily).
    # Search endpoint does not include body fields reliably, so we resolve bodies
    # from item detail endpoint for ids returned by category-filtered search.
    bodies: dict[str, str] = {}
    item_ids: list[int] = []

    max_pages = 35
    page_size = 200
    total_rows: int | None = None
    for page in range(max_pages):
        offset = page * page_size
        try:
            batch, page_total = _fetch_sauto_page({"category_id": 838, "limit": page_size, "offset": offset})
        except Exception:
            break
        if page_total is not None:
            total_rows = page_total
        if not batch:
            break

        for item in batch:
            if not isinstance(item, dict):
                continue
            category = item.get("category") or {}
            try:
                category_id = int(category.get("id")) if isinstance(category, dict) else 838
            except (TypeError, ValueError):
                category_id = 838
            if category_id != 838:
                continue

            try:
                item_id = int(item.get("id"))
            except (TypeError, ValueError):
                continue
            item_ids.append(item_id)

        # Hard limit so body refresh remains responsive.
        if len(item_ids) >= 800:
            break

        if total_rows is not None and (offset + len(batch)) >= total_rows:
            break

    # Deduplicate while keeping order.
    seen_ids: set[int] = set()
    unique_item_ids: list[int] = []
    for item_id in item_ids:
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)
        unique_item_ids.append(item_id)

    for item_id in unique_item_ids:
        try:
            detail = _fetch_sauto_item_detail(item_id)
        except Exception:
            continue

        category = detail.get("category") or {}
        try:
            detail_category_id = int(category.get("id")) if isinstance(category, dict) else None
        except (TypeError, ValueError):
            detail_category_id = None
        if detail_category_id is not None and detail_category_id != 838:
            continue

        body_cb = detail.get("vehicle_body_cb") or {}
        seo = str(body_cb.get("seo_name") or "").strip().lower()
        name = str(body_cb.get("name") or seo).strip()
        if seo:
            bodies[seo] = name or seo

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
