# CODEBUDDY.md — Sauto Scraper AI Context

> **Purpose**: This file gives AI coding assistants a complete mental model of the project so they can start contributing immediately without re-reading every file.

---

## Project Overview

**Sauto Scraper** is a full-stack used-car deal finder for the Czech marketplace Sauto.cz. It scrapes listings via the Sauto.cz JSON API, scores each car with a sophisticated evaluation engine, performs market-cohort analysis to flag undervalued cars, and sends Discord notifications for deals.

- **Original author**: karlosmatos (GitHub)
- **This fork**: Extended with web dashboard, scoring engine, market analysis, Discord notifications
- **Language**: Python 3 (scraper + API) + JavaScript/React (UI)
- **License**: MIT

---

## Architecture — 3 Layers

```
┌─────────────────────────────────────────────────┐
│  web-ui/  (React + Vite, port 5173)              │
│  App.jsx — params panel, scoring, results, logs  │
└──────────────┬──────────────────────────────────┘
               │ FETCH (CORS open)
               ▼
┌─────────────────────────────────────────────────┐
│  web-api/app.py  (FastAPI, port 8000)            │
│  Wraps spider as subprocess, serves REST API     │
│  Imports CarEvaluator from sauto_spider.py       │
└──────────────┬──────────────────────────────────┘
               │ subprocess.Popen
               ▼
┌─────────────────────────────────────────────────┐
│  sauto/  (Scrapy spider)                         │
│  sauto_spider.py — SautoSpider + CarEvaluator    │
│  settings.py, middlewares.py, items.py           │
└─────────────────────────────────────────────────┘
```

### Data Flow

```
params.json
  → SautoSpider.start_requests() reads params, builds search URL
  → parse_search() — iterates search API pages, yields detail requests
  → parse_detail() — CarEvaluator.evaluate() scores each car
  → closed() — market context, sort, save, Discord notify
  → data/sauto_interesting.json (scored results)
  → data/sauto_raw.json (raw feed export)
  → notified_ids.json (prevent duplicate Discord pings)
  → marked_ids.json (user bookmarks from web UI)
```

---

## Key Files — What Each Does

### Core Scraper

| File | Lines | Purpose |
|---|---|---|
| `sauto/spiders/sauto_spider.py` | 1511 | Main spider + `CarEvaluator` class. All scraping logic and scoring engine. |
| `sauto/settings.py` | 107 | Scrapy config: 4 concurrent requests, 2s delay, AutoThrottle, retry 3x |
| `sauto/middlewares.py` | 117 | `RandomUserAgentMiddleware` — rotates UA via `fake-useragent` |
| `sauto/items.py` | 12 | Placeholder Scrapy Item (unused — spider yields dicts) |
| `sauto/pipelines.py` | 13 | Placeholder pipeline (unused) |

### Backend API

| File | Lines | Purpose |
|---|---|---|
| `web-api/app.py` | 569 | FastAPI app. `ScraperRunner` class manages spider subprocess. All CRUD for params/results/catalog/scoring. |

### Frontend

| File | Lines | Purpose |
|---|---|---|
| `web-ui/src/App.jsx` | 1247 | Single-page React app. Params editor, scoring (client-side), results table, logs modal. |
| `web-ui/src/App.css` | ~large | All styles (dark/light theme via CSS variables) |
| `web-ui/src/main.jsx` | ~few | React entry point |
| `web-ui/vite.config.js` | — | Vite proxy config (if any) |

### Config / Data

| File | Purpose |
|---|---|
| `params.json` | Search parameters (all values stored as strings) |
| `requirements.txt` | scrapy, Twisted, fake-useragent, requests, fastapi, uvicorn |
| `scrapy.cfg` | Points to `sauto.settings` module |
| `notified_ids.json` | JSON array of ad IDs already sent to Discord |
| `data/sauto_catalog_cache.json` | Cached brand/model list from Sauto (24h TTL) |

---

## CarEvaluator Class — Scoring Engine Deep Dive

Located in `sauto_spider.py` lines 32–1291. This is the heart of the project.

### Class Attributes

```python
HARD_REJECT_PATTERNS  # Regex + reason: engine issues, parts-only, legal, total loss
BONUS_PATTERNS        # Regex + score + label: service history, garaged, timing belt, etc.
PENALTY_PATTERNS      # Regex + score + label: rust, needs investment, tuning, sounds
EQUIPMENT_BONUS       # Regex + score + label: cruise, parking, CarPlay, heated seats, LED
SCORING_PRESETS       # Dict of 4 presets: value, balanced, sport, luxury
PREMIUM_BRANDS        # Set: audi, bmw, mercedes-benz, porsche, volvo, etc.
BUDGET_BRANDS         # Set: dacia, skoda, kia, hyundai, etc.
```

### Key Static Methods

| Method | What It Does |
|---|---|
| `evaluate(item, ...)` | Entry point. Parses raw search+detail data into scored offer dict. Returns None if hard-rejected. |
| `_estimate_consumption_per_100km(...)` | Smart estimation: uses reported value if plausible, else fallback formula based on fuel+power+drive |
| `_estimate_annual_fuel_cost(...)` | Fuel price × consumption × annual km |
| `_estimate_insurance(...)` | Power + age based insurance cost model (Czech market) |
| `_estimate_maintenance(...)` | Age + mileage based maintenance cost model |
| `_build_market_context(offers)` | Groups offers into cohorts, computes medians for price/kW, price/km, annual costs |
| `_market_adjustment_for_offer(...)` | Compares offer vs cohort → undervalue/deep_undervalue/overprice labels |
| `_apply_advanced_sorting(...)` | Sorts offers by price/kW, annual cost, price |

### Scoring Components (per car)

Each car gets 10 component scores:
- `age` — scored by age bands (younger = better)
- `mileage` — scored by km bands
- `price` — scored by CZK bands
- `price_power` — price per kW (lower = better)
- `power` — kW (higher = better)
- `cost` — annual total cost (lower = better)
- `consumption` — L/100km or kWh/100km
- `equipment` — count + key features (adaptive cruise, cameras, CarPlay, etc.)
- `flags` — service book +14, first owner +9, tuning -28
- `sport` — power, RWD/AWD, manual gearbox, low price/kW
- `luxury` — premium brand, auto gearbox, leather, pano roof, young car

### Preset Weights

The 4 presets (`SCORING_PRESETS`) multiply each component to produce a weighted total:

| Component | Value | Balanced | Sport | Luxury |
|---|---|---|---|---|
| age | 0.75 | 1.00 | 1.05 | 1.35 |
| mileage | 1.10 | 1.00 | 0.75 | 0.90 |
| price | 1.40 | 1.00 | 0.55 | 0.25 |
| consumption | 1.15 | 1.00 | 0.35 | 0.25 |
| cost | 1.45 | 1.00 | 0.55 | 0.35 |
| price_power | 1.85 | 1.00 | 1.30 | 0.45 |
| power | 0.85 | 0.75 | 2.10 | 0.80 |
| equipment | 0.45 | 0.85 | 0.45 | 2.10 |
| flags | 1.15 | 1.00 | 0.80 | 0.90 |
| sport | 0.25 | 0.35 | 1.45 | 0.25 |
| luxury | 0.15 | 0.35 | 0.20 | 1.90 |

**Final score** = sum(component × weight) × 0.55 (rounded to int). Applied client-side in App.jsx `calculateScoreComponents()` and `getItemScore()`.

---

## SautoSpider — Spider Flow

### Initialization (`__init__`)
- Reads params, sets up runtime options
- Loads `notified_ids.json` to track already-pinged cars
- Initializes `searched_manufacturer_model_seos` (dedup)
- Sets `seen_ad_ids` set

### `start_requests()`
1. Reads `params.json`
2. Translates `manufacturer_seo_name` + `model_seo_name` → Sauto's `manufacturer_model_seo` format
3. Builds search params dict
4. Yields first search page request to `https://www.sauto.cz/api/v1/items/search?{query}`

### `parse_search(response)`
1. Parses JSON response
2. Iterates `results`, applies strict brand/model/seller post-filters
3. For each passing result → yields detail API request `https://www.sauto.cz/api/v1/items/{ad_id}`
4. Handles pagination (checks total count, increments offset)

### `parse_detail(response)`
1. Parses detail JSON from API
2. Calls `CarEvaluator.evaluate(base_item, ...)` to score the car
3. Applies detail-level filters (min price, gearbox preference, etc.)
4. Appends to `self.scored_cars` list
5. Yields the enriched item (for feed export)

### `closed(reason)`
1. Runs `_apply_advanced_sorting(scored_cars)` — market context + sorting
2. Takes top N, checks which are "new" (not in `notified_ids.json`)
3. Saves `data/sauto_interesting.json`
4. Formats and sends Discord message (if webhook configured)
5. Updates `notified_ids.json`

---

## Web API — app.py Structure

### ScraperRunner Class
- Manages spider as `subprocess.Popen` with stdout capture
- `start(output_file)` — launches `python -m scrapy crawl sauto -O data/sauto_raw.json`
- `_read_output()` — thread reads stdout line by line into `deque(maxlen=250)`
- `_watch_process()` — thread waits for process exit, records exit code + finish time
- `status()` — returns running, PID, exit_code, start/finish times
- `logs(limit)` — returns last N log lines
- Thread-safe via `threading.Lock`

### Key API Routes

| Route | Key Behavior |
|---|---|
| `GET /api/params` | Loads `params.json` → `{"params": {...}}` |
| `PUT /api/params` | Validates + normalizes (all values → strings), saves `params.json` |
| `POST /api/run` | Clears target result file, then starts scraper via `ScraperRunner.start()` |
| `GET /api/results?path=...` | Loads JSON file, annotates with `is_marked`, sorts by score desc |
| `GET /api/catalog/brands` | Fetches from Sauto API (with 24h JSON cache) |
| `GET /api/catalog/models?brand=...` | Fetches models for brand (24h cache, collector v2) |
| `GET /api/scoring/presets` | Returns `CarEvaluator.SCORING_PRESETS` as JSON |
| `POST /api/results/delete` | Removes items by ID, updates `marked_ids.json` |
| `POST /api/results/mark` | Toggles IDs in `marked_ids.json` |

### Helper Functions
- `load_json(path, fallback)` — safe JSON loader
- `dump_json(path, data)` — safe JSON writer (creates dirs)
- `load_result_items(path)` — robust parser with trailing-garbage trimming
- `normalize_relative_path(path, fallback)` — path traversal protection
- `_fetch_sauto_results(params)` — direct Sauto API call via `urllib`
- `_collect_brands(max_pages=25)` — paginates through Sauto search to gather all brands
- `_collect_models_for_brand(brand, ...)` — paginates through Sauto search for a brand's models

---

## Web UI — App.jsx Structure

### State Variables
```javascript
params, theme, status, items, logs, markedIds, selectedIds, resultsPath
showAdvanced, message, loading, initialLoading, runPhase, showLogsModal
brandOptions, selectedBrands, selectedModels, modelsByBrand
sortConfig, isSidebarHidden, scoringPresets, selectedPreset
```

### Key Functions

| Function | Purpose |
|---|---|
| `calculateScoreComponents(item)` | Client-side scoring (mirrors CarEvaluator but simplified) |
| `getItemScore(item, preset)` | Applies preset weights to components → final score |
| `sortValue(item, key)` | Extracts sortable value (handles score specially) |
| `fetchParams/Save/Run/Status/Results/Logs` | API call wrappers |
| `syncFilterParams(brands, models)` | Updates params when brand/model checkboxes change |
| `deleteSelected()`, `clearAllResults()`, `markSelected()` | Batch result actions |
| `exportResults(scope)`, `importResultsFile(file)` | JSON export/import |

### UI Structure
```
App
├── Topbar (status, theme toggle, sidebar toggle, API health)
├── Layout
│   ├── Sidebar (params editor, brand/model browser)
│   │   ├── Search params (BASIC_GROUPS)
│   │   ├── Advanced params (ADVANCED_GROUPS) — collapsible
│   │   └── Extra params (anything not in PARAM_GROUPS)
│   └── Main Content
│       ├── Results toolbar (sort, select, export, import, delete, mark)
│       └── Results table (sortable columns, checkboxes)
└── Logs Modal (streaming log viewer)
```

### Client-Side Scoring vs Server-Side

**Important distinction**: The spider (`sauto_spider.py`) does NOT compute final scores anymore (as noted in Discord message: *"Scoring and preset selection moved to frontend (Varianta A)"*). The spider only:
1. Evaluates raw metrics (price/kW, price/km, annual costs, flags, etc.)
2. Computes market context (cohort medians, valuation labels)
3. Saves everything to `sauto_interesting.json`

The **frontend** (`App.jsx`) then reads these raw metrics and applies scoring component bands + preset weights client-side. This means:
- `sauto_spider.py` ~lines 1005-1140: `_evaluate_single()` assigns raw component values but the weighted total is 0
- `App.jsx` `calculateScoreComponents()` and `getItemScore()` compute the actual display score
- Both use the same scoring bands and preset weights (duplicated between Python and JS)

---

## Common Patterns & Conventions

### Param Handling
- All param values in `params.json` are **strings** (even booleans: `"true"`/`"false"`)
- The API normalizes to `str` on save: `{str(key): "" if value is None else str(value)}`
- The spider converts strings to appropriate types when reading

### File Paths
- API uses `ROOT_DIR` (parent of `web-api/`) as base
- All paths validated with `normalize_relative_path()` — blocked if absolute or outside project
- Spider uses relative paths from working directory

### Error Handling
- Spider: `handle_detail_error()` catches failed detail requests, still yields base item
- API: HTTPException with detail messages, try/except with cached fallbacks for catalog
- UI: try/catch on all fetch calls, `message` state for user feedback

### Threading
- `ScraperRunner` uses `threading.Lock` for process state
- Two daemon threads: stdout reader + process watcher
- Frontend polls status every 2s, logs every 1.5s, results every 3s when running

---

## Dependencies

```txt
scrapy==2.13.3
Twisted<26
fake-useragent==2.2.0
requests>=2.31.0
fastapi>=0.115.0
uvicorn>=0.30.0
```

Frontend: React 18, Vite, lucide-react (icons)

---

## Troubleshooting

| Issue | Check |
|---|---|
| "API není dostupné" in UI | Is backend running on port 8000? |
| Scraper won't start | Is another scraper already running? (409 Conflict) |
| No Discord notifications | Is `discord_webhook_url` set in params? |
| Stale catalog | Force refresh: `?force_refresh=true` on catalog endpoints |
| Result file can't be read | Check `load_result_items()` — trims trailing garbage from partial writes |
| "Use a relative path" error | All paths must be relative to project root |

---

## Git Info

- Remote: `https://github.com/afrox26TP/Sauto_Scrapper.git`
- Original upstream: `https://github.com/karlosmatos/sauto-scraper.git`