from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import app
from sauto.spiders.sauto_spider import SautoSpider
from starlette.requests import Request
from starlette.responses import JSONResponse


class TrialCreditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_users_path = app.USERS_DB_PATH
        app.USERS_DB_PATH = Path(self.temp_dir.name) / "users.json"
        self.user_id = "usr_test"
        app._save_users(
            [
                {
                    "id": self.user_id,
                    "email": "test@example.com",
                    "payment_status": "none",
                    "trial_runs_remaining": 2,
                    "credit_balance": 0,
                }
            ]
        )

    def tearDown(self) -> None:
        app.USERS_DB_PATH = self.original_users_path
        self.temp_dir.cleanup()

    def test_two_trial_runs_then_access_is_denied(self) -> None:
        first = app._reserve_run_access(self.user_id)
        second = app._reserve_run_access(self.user_id)
        third = app._reserve_run_access(self.user_id)

        self.assertEqual(first, {"kind": "trial", "max_items": app.TRIAL_MAX_ITEMS})
        self.assertEqual(second, {"kind": "trial", "max_items": app.TRIAL_MAX_ITEMS})
        self.assertIsNone(third)
        user = app._find_user_by_id(self.user_id)
        self.assertIsNotNone(user)
        self.assertEqual(app._trial_runs_remaining(user or {}), 0)

    def test_credit_is_used_after_trial_and_failed_run_can_be_refunded(self) -> None:
        users = app._load_users()
        users[0]["trial_runs_remaining"] = 0
        users[0]["credit_balance"] = app.SCRAPE_RUN_CREDIT_COST
        app._save_users(users)

        access = app._reserve_run_access(self.user_id)
        self.assertEqual(access, {"kind": "credit", "max_items": None, "credit_cost": app.SCRAPE_RUN_CREDIT_COST})
        self.assertIsNone(app._reserve_run_access(self.user_id))

        app._refund_run_access(self.user_id, "credit")
        user = app._find_user_by_id(self.user_id)
        self.assertEqual(app._credit_balance(user or {}), app.SCRAPE_RUN_CREDIT_COST)

    def test_global_kill_switch_blocks_run_without_consuming_access(self) -> None:
        request = Request({
            "type": "http",
            "method": "POST",
            "path": "/api/run",
            "query_string": b"",
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("127.0.0.1", 12345),
        })

        with patch.object(app, "SCRAPER_RUNS_ENABLED", False):
            with self.assertRaises(app.HTTPException) as raised:
                app.run_scraper(app.RunPayload(), request)

        self.assertEqual(raised.exception.status_code, 503)
        user = app._find_user_by_id(self.user_id) or {}
        self.assertEqual(app._trial_runs_remaining(user), 2)
        self.assertEqual(app._credit_balance(user), 0)

    def test_public_job_does_not_expose_user_id(self) -> None:
        runner = app.ScraperRunner()
        queue = app.RunQueue(runner)
        job = queue._new_job(
            output_file="data/test.json",
            project_id="project",
            run_mode="free_proxy",
            billable=False,
            proxy_url="http://user:pass@example.com:8080",
            user_id=self.user_id,
            access_kind="trial",
            max_items=100,
        )

        public = queue._public_job(job)
        self.assertIsNotNone(public)
        self.assertNotIn("user_id", public or {})
        self.assertEqual((public or {}).get("proxy_url"), "***")

    def test_paid_checkout_webhook_credits_only_once(self) -> None:
        event = {
            "type": "checkout.session.completed",
            "data": {
                "object": {
                    "id": "cs_test",
                    "client_reference_id": self.user_id,
                    "payment_status": "paid",
                    "metadata": {"credits": str(app.BASIC_BUNDLE_CREDITS)},
                }
            },
        }
        raw = json.dumps(event).encode("utf-8")

        async def invoke_webhook() -> None:
            sent = False

            async def receive() -> dict:
                nonlocal sent
                if sent:
                    return {"type": "http.disconnect"}
                sent = True
                return {"type": "http.request", "body": raw, "more_body": False}

            request = Request({"type": "http", "method": "POST", "path": "/api/billing/webhook/stripe", "headers": []}, receive)
            await app.stripe_webhook(request)

        with patch.object(app, "_stripe_verify_signature", return_value=True):
            asyncio.run(invoke_webhook())
            asyncio.run(invoke_webhook())

        user = app._find_user_by_id(self.user_id)
        self.assertEqual(app._credit_balance(user or {}), app.BASIC_BUNDLE_CREDITS)

    def test_personal_api_key_is_hashed_and_rotation_revokes_old_key(self) -> None:
        first = app._replace_personal_api_key(self.user_id)["api_key"]
        stored = app._find_user_by_id(self.user_id) or {}
        self.assertNotEqual(stored.get("personal_api_key_hash"), first)
        self.assertNotIn(first, json.dumps(stored))
        self.assertEqual((app._find_user_by_api_key(first) or {}).get("id"), self.user_id)

        second = app._replace_personal_api_key(self.user_id)["api_key"]
        self.assertIsNone(app._find_user_by_api_key(first))
        self.assertEqual((app._find_user_by_api_key(second) or {}).get("id"), self.user_id)

    def test_concurrent_api_calls_cannot_overdraw_or_clone_credits(self) -> None:
        users = app._load_users()
        users[0]["credit_balance"] = 25
        app._save_users(users)

        def consume(_: int) -> bool:
            return app._consume_api_call_credit(self.user_id, "/api/health", "GET")[0]

        with ThreadPoolExecutor(max_workers=20) as pool:
            results = list(pool.map(consume, range(100)))

        self.assertEqual(sum(results), 25)
        user = app._find_user_by_id(self.user_id) or {}
        self.assertEqual(app._credit_balance(user), 0)
        event_ids = [event["id"] for event in user.get("credit_events", [])]
        self.assertEqual(len(event_ids), 25)
        self.assertEqual(len(set(event_ids)), 25)

    def test_personal_api_key_middleware_charges_every_api_call(self) -> None:
        users = app._load_users()
        users[0]["credit_balance"] = 2
        app._save_users(users)
        api_key = app._replace_personal_api_key(self.user_id)["api_key"]

        async def invoke(key: str, path: str = "/api/health") -> JSONResponse:
            headers = [(b"x-api-key", key.encode("utf-8"))] if key else []
            request = Request({
                "type": "http",
                "method": "GET",
                "path": path,
                "query_string": b"",
                "headers": headers,
                "scheme": "http",
                "server": ("testserver", 80),
                "client": ("127.0.0.1", 12345),
            })

            async def call_next(_: Request) -> JSONResponse:
                return JSONResponse({"status": "ok"})

            return await app.api_key_guard(request, call_next)

        with patch.object(app.billing_ledger, "record_api_call"):
            first = asyncio.run(invoke(api_key))
            second = asyncio.run(invoke(api_key))
            exhausted = asyncio.run(invoke(api_key))
            invalid = asyncio.run(invoke("autoidx_sk_fake"))
            anonymous_private = asyncio.run(invoke("", "/api/results"))

        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.headers.get("x-credit-balance"), "1")
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.headers.get("x-credit-balance"), "0")
        self.assertEqual(exhausted.status_code, 402)
        self.assertEqual(invalid.status_code, 401)
        self.assertEqual(anonymous_private.status_code, 401)

    def test_spider_does_not_schedule_more_than_max_items(self) -> None:
        spider = SautoSpider(max_items="2")
        spider._passes_strict_filter = lambda _: True
        response = SimpleNamespace(
            text=json.dumps(
                {
                    "results": [
                        {"id": index, "manufacturer_cb": {"seo_name": "skoda"}, "model_cb": {"seo_name": "octavia"}}
                        for index in range(1, 5)
                    ],
                    "total": 4,
                }
            ),
            meta={"params": {"limit": "100", "offset": "0"}},
        )

        requests = list(spider.parse_search(response))
        self.assertEqual(spider.detail_requests_sent, 2)
        self.assertEqual(len(requests), 2)


if __name__ == "__main__":
    unittest.main()
