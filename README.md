# Sauto Scraper

> Used-car deal finder for the Czech marketplace [Sauto.cz](https://www.sauto.cz) — scrapes, scores, and notifies.

## Quick Links

- **User Documentation**: [`sauto-scraper-main/README.md`](sauto-scraper-main/README.md)
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

## License

MIT