# Cloudflare Deployment Guide

This project is deployed as two services:

- `web-ui` -> Cloudflare Pages
- `web-api` (FastAPI) -> Python host (VM/container) exposed through Cloudflare Tunnel

Reusable files in repo:

- `deploy/cloudflare/cloudflared-config.example.yml`
- `deploy/cloudflare/setup-tunnel-windows.ps1`

## 1) Deploy API (FastAPI)

Run API on your server:

```bash
cd sauto-scraper-main/web-api
cp .env.example .env
# edit .env (AUTH_SECRET, CORS_ALLOW_ORIGINS, optional SAUTO_API_KEYS)

# Example run command
python -m uvicorn app:app --host 127.0.0.1 --port 8000
```

Recommended production values in `.env`:

```env
AUTH_SECRET=<long-random-secret>
CORS_ALLOW_ORIGINS=https://app.your-domain.com
SAUTO_API_KEYS=<optional-comma-separated-keys>
```

## 2) Expose API with Cloudflare Tunnel

Install and login `cloudflared`, then create tunnel and DNS route.

### Quick commands (Windows PowerShell)

```powershell
cd sauto-scraper-main
.\deploy\cloudflare\setup-tunnel-windows.ps1 -Zone "your-domain.com" -TunnelName "sauto-api" -ApiHostname "api.your-domain.com"
```

Then run tunnel:

```powershell
cloudflared tunnel run sauto-api
```

Example tunnel config (`~/.cloudflared/config.yml`):

```yaml
tunnel: sauto-api
credentials-file: /root/.cloudflared/<tunnel-id>.json

ingress:
  - hostname: api.your-domain.com
    service: http://127.0.0.1:8000
  - service: http_status:404
```

Start tunnel:

```bash
cloudflared tunnel run sauto-api
```

If you prefer manual setup:

```bash
cloudflared tunnel login
cloudflared tunnel create sauto-api
cloudflared tunnel route dns sauto-api api.your-domain.com
```

## 3) Deploy Frontend to Cloudflare Pages

Project path: `sauto-scraper-main/web-ui`

Pages settings:

- Build command: `npm run build`
- Build output directory: `dist`
- Environment variable:

```env
VITE_API_BASE_URL=https://api.your-domain.com
```

Add custom domain in Cloudflare Pages, for example `app.your-domain.com`.

## Alternative: Wrangler Deploy from Repository Root

If your CI runs `npx wrangler deploy` from repo root, use `wrangler.toml` in root.
This repo already includes:

- `wrangler.toml` with:
  - build command: `cd sauto-scraper-main/web-ui && npm install && npm run build`
  - assets directory: `./sauto-scraper-main/web-ui/dist`
  - SPA fallback: `not_found_handling = "single-page-application"`

In that case, keep deploy command as:

```bash
npx wrangler deploy
```

## 4) Verification Checklist

- `https://api.your-domain.com/api/health` returns `200`
- `https://app.your-domain.com` loads without console CORS errors
- Login/signup works from frontend
- `POST /api/run` works (with auth and optional `x-api-key`)

## 5) Exact Production Values

`web-api/.env`:

```env
AUTH_SECRET=<long-random-secret>
CORS_ALLOW_ORIGINS=https://app.your-domain.com
SAUTO_API_KEYS=<optional-comma-separated-keys>
```

Cloudflare Pages env:

```env
VITE_API_BASE_URL=https://api.your-domain.com
```

## 6) Optional Hardening

- Restrict `CORS_ALLOW_ORIGINS` to exact frontend domains only
- Set `SAUTO_API_KEYS` for integration clients
- Run API behind a process manager (systemd, pm2, supervisor, docker restart policy)
- Add TLS/HTTP headers at Cloudflare edge (WAF, rate limiting, bot protection)
