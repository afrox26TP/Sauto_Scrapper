# React Web UI

## Run

```bash
cd sauto-scraper-main/web-ui
npm install
npm run dev
```

The app runs on `http://localhost:5173` and in local dev uses API `http://127.0.0.1:8000`.

## Environment

Create `.env` from `.env.example` when you need a custom API host:

```bash
cp .env.example .env
```

Variables:

- `VITE_API_BASE_URL`
	- Empty (default): in production, UI calls same-origin `/api/*`
	- Example: `https://api.example.com` for split frontend/backend deployment

## Cloudflare Pages

When deploying this frontend to Cloudflare Pages:

- Build command: `npm run build`
- Build output directory: `dist`
- Env var (Pages > Settings > Environment variables):
	- `VITE_API_BASE_URL=https://api.your-domain.com`
