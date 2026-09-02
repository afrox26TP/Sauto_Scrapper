from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import app
from sauto.spiders.sauto_spider import SautoSpider
from starlette.requests import Request


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
                    "run_credits": 0,
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
        users[0]["run_credits"] = 1
        app._save_users(users)

        access = app._reserve_run_access(self.user_id)
        self.assertEqual(access, {"kind": "credit", "max_items": None})
        self.assertIsNone(app._reserve_run_access(self.user_id))

        app._refund_run_access(self.user_id, "credit")
        user = app._find_user_by_id(self.user_id)
        self.assertEqual(app._run_credits(user or {}), 1)

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
        self.assertEqual(app._run_credits(user or {}), app.BASIC_BUNDLE_CREDITS)

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
