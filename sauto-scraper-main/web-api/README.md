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
- `POST /api/run`
- `GET /api/status`
- `GET /api/results`
