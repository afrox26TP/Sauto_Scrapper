# Web API for Sauto Scraper

This FastAPI service wraps the existing Scrapy spider.

## Run

```bash
cd sauto-scraper-main
uvicorn web-api.app:app --reload --port 8000
```

The API will be available at `http://localhost:8000`.

## Endpoints

- `GET /api/health`
- `GET /api/params`
- `PUT /api/params`
- `POST /api/run` (queue run job)
- `POST /api/pause` (pause active scraper)
- `POST /api/resume` (resume paused scraper)
- `POST /api/stop` (early stop active scraper)
- `GET /api/status` (runner + queue summary)
- `GET /api/jobs`
- `GET /api/jobs/{job_id}`
- `GET /api/billing/rates`
- `GET /api/billing/usage`
- `GET /api/billing/events`
- `GET /api/results`

## Run Queue

`POST /api/run` now enqueues a scrape job instead of failing when another run is active.

Request body:

```json
{
	"output_file": "data/sauto_interesting.json",
	"project_id": "default"
}
```

Response includes whether the job started immediately or is waiting in queue.

## API Key Guard (Optional)

For SaaS deployments, you can protect all write endpoints (`POST`, `PUT`, `PATCH`, `DELETE`) with API keys.

Set environment variable:

```bash
export SAUTO_API_KEYS="key_a,key_b,key_c"
```

Then include header on write requests:

```http
x-api-key: key_a
```

If `SAUTO_API_KEYS` is empty, guard is disabled.

## Usage Billing (No Plans)

Backend uses pure pay-as-you-go billing:

- scraper usage: charged per run + per result item
- integration API usage: charged per API call with `x-api-key`

Rates are controlled by env vars:

```bash
export BILLING_RUN_BASE_CZK="5.0"
export BILLING_ITEM_CZK="0.02"
export BILLING_API_CALL_CZK="0.05"
export BILLING_PROXY_RUN_CZK="0.0"
```

`project_id` for scraper usage is taken from `POST /api/run` payload.

`project_id` for integration API calls is taken from header `x-project-id`.
If missing, fallback ID `integration-<api_key_prefix>` is used.
