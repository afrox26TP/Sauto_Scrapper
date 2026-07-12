# Sauto Scraper

> Used-car deal finder for the Czech marketplace [Sauto.cz](https://www.sauto.cz) — scrapes, scores, and notifies.

## Quick Links

- **User Documentation**: [`sauto-scraper-main/README.md`](sauto-scraper-main/README.md)
- **Cloudflare Deploy**: [`sauto-scraper-main/CLOUDFLARE_DEPLOY.md`](sauto-scraper-main/CLOUDFLARE_DEPLOY.md)
- **AI/Developer Context**: [`CODEBUDDY.md`](CODEBUDDY.md)

Original version by [karlosmatos](https://github.com/karlosmatos/sauto-scraper). This fork adds a web dashboard, multi-preset scoring engine, market analysis, and Discord notifications.

## Requirements

- Python 3.9+
- Node.js 18+ (for web UI)
- Pip packages: `pip install -r sauto-scraper-main/requirements.txt`

## Start

```bash
# Terminal 1 — Backend
cd sauto-scraper-main
uvicorn web-api.app:app --reload --port 8000

# Terminal 2 — Frontend
cd sauto-scraper-main/web-ui
npm install && npm run dev
```

Open [http://localhost:5173](http://localhost:5173).

## BYOP Model

This project uses `BYOP` (Bring Your Own Proxy):

- Each user supplies their own proxy credentials (for example Webshare or Smartproxy).
- Proxy traffic billing (GB/data transfer) is handled by the proxy provider, not by this app.
- The app works as an orchestrator: run control, validation, scheduling, scoring, and API/UI management.

## Proxy Benchmark (BYOP Profile A vs B)

Use this helper to run one `free_proxy` (BYOP profile A) and one `paid_proxy` (BYOP profile B) job and compare duration and item count:

```powershell
cd sauto-scraper-main
.\deploy\benchmark-proxy-modes.ps1 -ApiBase "http://127.0.0.1:8000" -ProjectId "proxy-benchmark"
```

If API key auth is enabled, add `-ApiKey "<your-key>"`.

Before benchmarking, set proxy profiles with:

```powershell
.\deploy\set-proxy-profiles-windows.ps1
```

## Cloudflare CI Note

Repository root includes `wrangler.toml` for static frontend deploy from CI using:

```bash
npx wrangler deploy
```

It builds `sauto-scraper-main/web-ui` and deploys assets from `sauto-scraper-main/web-ui/dist`.

## License

MIT