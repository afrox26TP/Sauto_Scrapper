# Sauto Scraper — Used Car Deal Finder for Sauto.cz

A full-stack web scraper and deal analyzer for the Czech car marketplace [Sauto.cz](https://www.sauto.cz). It scrapes listings, evaluates car value using a sophisticated scoring engine, and notifies you via Discord when underpriced cars appear.

## Features

- **Scrapes Sauto.cz API** — search listings and full detail pages
- **Multi-brand & multi-model** filtering with live catalog browser
- **Smart car evaluation** — scores cars based on price, power, mileage, age, fuel costs, equipment, and more
- **Market analysis** — compares each car against similar listings (cohort-based pricing)
- **4 scoring presets**: Value, Balanced, Sport, Luxury
- **Discord notifications** — get pinged when under-priced deals appear
- **Web dashboard** — React UI for live param editing, scraper control, and results browsing
- **Result management** — sort, filter, mark, export, import, and delete results

Deployment guide: [`CLOUDFLARE_DEPLOY.md`](CLOUDFLARE_DEPLOY.md)

---

## Project Structure

```
sauto-scraper-main/
├── sauto/                          # Scrapy spider package
│   ├── spiders/
│   │   └── sauto_spider.py         # Main spider (1511 lines) — scraping + evaluation logic
│   ├── items.py                    # Scrapy item definitions (placeholder)
│   ├── middlewares.py              # Random User-Agent rotation middleware
│   ├── pipelines.py                # Item pipeline (placeholder)
│   └── settings.py                 # Scrapy settings (concurrency, delays, headers)
├── web-api/
│   └── app.py                      # FastAPI backend (569 lines) — wraps spider, serves API
├── web-ui/                         # React (Vite) frontend
│   ├── src/
│   │   ├── App.jsx                 # Main React component (1247 lines)
│   │   ├── App.css                 # Styles
│   │   └── main.jsx                # Entry point
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
├── data/                           # Output & cache files
│   ├── sauto_raw.json              # Raw scraped listings
│   ├── sauto_interesting.json      # Scored/interesting deals
│   ├── sauto_catalog_cache.json    # Cached brand/model catalog
│   ├── sauto.json                  # Alternative output
│   └── ... (test files)
├── params.json                     # Search parameters config
├── notified_ids.json               # IDs already sent to Discord
├── marked_ids.json                 # User-marked result IDs
├── requirements.txt                # Python dependencies
├── scrapy.cfg                      # Scrapy project config
└── startup.txt                     # Startup notes
```

---

## How It Works

### 1. Scraping Flow

```
params.json → Search API → Parse listings → Detail API → Evaluate → Score → Sort → Notify
```

1. **Read `params.json`** for search filters (brand, model, price, year, fuel, gearbox, etc.)
2. **Query Sauto.cz search API** (`/api/v1/items/search`) page by page
3. **Apply strict post-filters** — the API sometimes returns broader results, so the spider double-checks brand, model, seller type
4. **Fetch detail API** (`/api/v1/items/{ad_id}`) for each matching listing to get full specs (VIN, equipment list, STK dates, service history, images, etc.)
5. **Evaluate each car** — `CarEvaluator` class computes:
   - Price per kW, price per km, annual costs (fuel, insurance, maintenance)
   - Flags: first owner, service book, tuning, equipment count
   - Brand tier (premium/budget/mainstream)
   - Drive type, gearbox type inference
   - Hard rejection patterns (engine issues, parts-only, legal problems, total loss)
   - Bonus/penalty text patterns in descriptions
6. **Market context** — groups scored cars into cohorts by brand+generation+fuel+gearbox, computes median price/kW and price/km, flags undervalued/deep undervalued/overpriced
7. **Output** — saves `data/sauto_interesting.json` with all scored offers (+ market context)
8. **Discord notification** — sends top N offers to configured webhook (only new ones if enabled)

### 2. Car Scoring Engine

The `CarEvaluator` class (inside `sauto_spider.py`) contains:

| Component | What it scores | Best score |
|---|---|---|
| `age` | Age in years (≤2y best) | 78 |
| `mileage` | Tachometer km (≤50k best) | 72 |
| `price` | Price in CZK (≤120k best) | 56 |
| `price_power` | Price per kW (≤1200 best) | 72 |
| `power` | Power in kW (≥220 best) | 72 |
| `cost` | Annual total cost (≤35k best) | 48 |
| `consumption` | L/100km or kWh/100km | 34 |
| `equipment` | Equipment count + key features | 0–70 |
| `flags` | Service book (+14), first owner (+9), tuning (-28) | varies |
| `sport` | Power, RWD/AWD, manual, price/kW | 0–73 |
| `luxury` | Premium brand, auto, leather, pano roof, young | -20–75 |

**4 presets** weigh these components differently:
- **Value (Cena/výkon)**: heavy on price, cost, price/kW
- **Balanced**: equal weight across all factors
- **Sport**: heavy on power, sportiness, price/kW
- **Luxury**: heavy on equipment, premium brand, comfort

### 3. Market Valuation

The spider groups cars into "cohorts" (same brand tier + model family + generation + fuel + gearbox). For each car, it compares:
- **Price/kW** vs cohort median → undervalue/deep_undervalue/overprice label
- **Price/km** vs cohort median
- **Model family avg price** (when enough samples)

### 4. Web Dashboard

The React UI at `http://localhost:5173` provides:

- **Search params panel** — all filters with sliders, selects, checkboxes
- **Brand/Model browser** — multi-select from live Sauto.cz catalog (via API proxy)
- **Scoring preset selector** — switch between Value/Balanced/Sport/Luxury (scoring applied client-side)
- **Run button** — saves params, launches the scraper subprocess
- **Live logs** — streaming log viewer during scrape
- **Results table** — sortable by score, price, power, mileage, age, annual cost
- **Result actions** — mark, delete, clear, export JSON, import JSON
- **Dark/light theme** toggle
- **Sidebar hide/show**

---

## Installation

### Prerequisites

- **Python 3.9+** (with pip)
- **Node.js 18+** (with npm) — for web UI
- **Git** (optional)

### 1. Clone & Setup

```bash
git clone https://github.com/karlosmatos/sauto-scraper.git
cd sauto-scraper-main
```

### 2. Install Python Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Linux/Mac)
source venv/bin/activate

# Install packages
pip install -r requirements.txt
```

### 3. Install Web UI Dependencies

```bash
cd web-ui
npm install
cd ..
```

---

## Usage

### Quick Start — Web Mode (Recommended)

Two terminals needed:

**Terminal 1 — Backend API:**
```bash
cd sauto-scraper-main
uvicorn web-api.app:app --reload --port 8000
```

**Terminal 2 — Frontend:**
```bash
cd sauto-scraper-main/web-ui
npm run dev
```

Open `http://localhost:5173` in your browser.

### CLI Mode — Run Scraper Directly

```bash
# JSON output (default)
python -m scrapy crawl sauto -O data/sauto.json

# CSV output
python -m scrapy crawl sauto -O data/sauto.csv

# JSON Lines output
python -m scrapy crawl sauto -O data/sauto.jl
```

### Configuration

All search parameters live in `params.json`. You can edit it directly or use the web UI.

#### Parameter Reference

| Key | Type | Description | Default |
|---|---|---|---|
| `limit` | int | Results per page (max 1000) | `"35"` |
| `offset` | int | Starting offset for pagination | `"0"` |
| `category_id` | string | Vehicle category (`838` = personal cars) | `"838"` |
| `manufacturer_seo_name` | string | Brand SEO name (comma-separated for multiple) | `""` |
| `model_seo_name` | string | Model SEO name (comma-separated) | `""` |
| `condition_seo` | string | Condition: `nove,ojete,predvadeci` (new/used/demo) | `"nove,ojete,predvadeci"` |
| `seller_type` | string | `soukromy` (private) or `bazar` (dealer); empty = all sellers | `""` |
| `operating_lease` | string | `"true"` / `"false"` | `"false"` |
| `price_from` | int | Min price (CZK, 0 = no limit) | `"0"` |
| `price_to` | int | Max price (CZK, 0 = no limit) | `"0"` |
| `year_from` | int | Min year | `""` |
| `year_to` | int | Max year | `""` |
| `tachometer_from` | int | Min mileage (km) | `""` |
| `tachometer_to` | int | Max mileage (km) | `""` |
| `power_from` | int | Min power (kW) | `""` |
| `power_to` | int | Max power (kW) | `""` |
| `fuel_seo` | string | Fuel type (comma-separated): `benzin,nafta,hybrid,elektro,lpg-benzin,cng-benzin` | `""` |
| `gearbox_filter` | string | `manual` or `automatic` | `""` |
| `drive_filter` | string | `fwd`, `rwd`, or `awd` | `""` |
| `body_seo` | string | Body type (comma-separated): `suv,kombi,hatchback,sedan,coupe,...` | `""` |
| `required_equipment` | string | Must-have equipment keywords (comma-separated) | `""` |

#### Scoring & Evaluation Parameters

| Key | Type | Description | Default |
|---|---|---|---|
| `interesting_min_score` | int | Minimum score to include (-1000 = all) | `"-1000"` |
| `interesting_top_n` | int | Max results to keep/notify | `"5000"` |
| `interesting_min_price` | int | Min price to evaluate (CZK) | `"0"` |
| `allow_automatic` | bool | Allow automatic transmissions | `"true"` |
| `prefer_gearbox` | string | `any`, `manual`, or `automatic` | `"any"` |
| `prefer_drive` | string | `any`, `fwd`, `rwd`, or `awd` | `"any"` |
| `target_annual_km` | int | Expected annual km for cost calculation | `"15000"` |

#### Market Analysis Parameters

| Key | Type | Description | Default |
|---|---|---|---|
| `market_min_cohort_size` | int | Min cars for a valid comparison cohort | `"6"` |
| `market_expected_km_per_year` | int | Expected km/year for age-normalized comparisons | `"16000"` |
| `model_price_min_samples` | int | Min samples for model-level price reference | `"5"` |
| `undervalue_ratio_threshold` | float | Price/median ratio ≤ this = undervalued | `"0.88"` |
| `deep_undervalue_ratio_threshold` | float | Price/median ratio ≤ this = deeply undervalued | `"0.75"` |
| `overprice_ratio_threshold` | float | Price/median ratio ≥ this = overpriced | `"1.18"` |

#### Notification Parameters

| Key | Type | Description | Default |
|---|---|---|---|
| `discord_webhook_url` | string | Discord webhook URL for notifications | `""` |
| `discord_notify_only_new` | bool | Only notify for newly seen cars | `"true"` |

---

## API Endpoints

The FastAPI backend serves on `http://localhost:8000`:

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Health check (uptime, version) |
| `GET` | `/api/params` | Load current params |
| `PUT` | `/api/params` | Save params |
| `POST` | `/api/run` | Enqueue scraper job (starts now or waits in queue) |
| `POST` | `/api/pause` | Pause active scraper run |
| `POST` | `/api/resume` | Resume paused scraper run |
| `POST` | `/api/stop` | Stop active scraper early |
| `GET` | `/api/status` | Scraper + queue status |
| `GET` | `/api/jobs` | Queue summary (active/pending/history) |
| `GET` | `/api/jobs/{job_id}` | Single job detail |
| `GET` | `/api/logs` | Live scraper logs (last 250 lines) |
| `GET` | `/api/results` | Load scored results with marked status |
| `GET` | `/api/catalog/brands` | Fetch brand list from Sauto.cz (24h cache) |
| `GET` | `/api/catalog/models?brand=...` | Fetch models for a brand (24h cache) |
| `GET` | `/api/scoring/presets` | Get all scoring preset definitions |
| `GET` | `/api/results/export` | Export results JSON |
| `POST` | `/api/results/delete` | Delete selected results by ID |
| `POST` | `/api/results/clear` | Clear all results |
| `POST` | `/api/results/import` | Import results JSON |
| `POST` | `/api/results/mark` | Mark/unmark results by ID |

---

## Proxy Rotation (Production)

The scraper now supports environment-driven proxy rotation in downloader middleware.

Supported variables:

- `SAUTO_PROXY_LIST`: comma-separated or newline-separated proxy URLs
- `SAUTO_PROXY_URL`: single proxy URL fallback
- `SAUTO_PROXY_MODE`: `round_robin` (default) or `random`
- `SAUTO_PROXY_BAN_STATUSES`: comma-separated HTTP codes that trigger proxy retry, default `403,407,429,500,502,503,504`

Examples:

```bash
# Single proxy
export SAUTO_PROXY_URL="http://user:pass@proxy1.example.com:10000"

# Proxy pool
export SAUTO_PROXY_LIST="http://user:pass@proxy1.example.com:10000,http://user:pass@proxy2.example.com:10000"
export SAUTO_PROXY_MODE="round_robin"
```

Notes:

- If no proxy env var is provided, middleware stays disabled and scraper runs as before.
- Keep request rate low even with proxies (`DOWNLOAD_DELAY`, `AUTOTHROTTLE`) to reduce blocking.

---

## API Security (Production)

To protect write operations in SaaS mode, set:

- `SAUTO_API_KEYS`: comma-separated API keys.

When configured, all `/api/*` write endpoints (`POST`, `PUT`, `PATCH`, `DELETE`) require header:

- `x-api-key: <your-key>`

---

## Billing Model (Usage Only)

No plans/tiers are required.

- Scraper billing: per run + per output item.
- Integration billing: per API call when request contains `x-api-key`.

Billing endpoints:

- `GET /api/billing/rates`
- `GET /api/billing/usage?project_id=<id>`
- `GET /api/billing/events?project_id=<id>&limit=100`

Default rates are configurable with env vars:

- `BILLING_RUN_BASE_CZK`
- `BILLING_ITEM_CZK`
- `BILLING_API_CALL_CZK`
- `BILLING_PROXY_RUN_CZK`

---

## Data Files

| File | Description |
|---|---|
| `data/sauto_raw.json` | All raw scraped listings (full detail) |
| `data/sauto_interesting.json` | Scored, filtered, sorted interesting deals |
| `data/sauto_catalog_cache.json` | Cached brand/model catalog (24h TTL) |
| `notified_ids.json` | Car ad IDs already sent to Discord |
| `marked_ids.json` | User-marked result IDs (from web UI) |

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Scraper** | Python 3, Scrapy 2.13, Twisted |
| **Backend API** | FastAPI, Uvicorn, Pydantic |
| **Frontend** | React 18, Vite, Lucide icons |
| **User-Agent rotation** | fake-useragent |
| **HTTP (catalog fetch)** | requests, urllib |
| **Notifications** | Discord webhooks |

---

## License

[MIT](https://choosealicense.com/licenses/mit/)

---

## Credits

Original scraper by [karlosmatos](https://github.com/karlosmatos/sauto-scraper). Extended with web dashboard, scoring engine, market analysis, and Discord notifications.