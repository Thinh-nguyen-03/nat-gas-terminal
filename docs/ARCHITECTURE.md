# Architecture

## System Overview

The terminal is built as three independent processes that share a single DuckDB database file on disk.

```
+------------------+       +------------------+       +------------------+
|  Python Plane    |  -->  |     DuckDB       |  <--  |    Go API        |
|                  |       |  (shared file)   |       |  (read-only)     |
|  collectors/     |       |                  |       |  REST + SSE      |
|  transforms/     |       |  facts_time_series|      +------------------+
|  scheduler/      |       |  features_daily  |              |
+------------------+       |  summary_outputs |              v
                            |  ...             |    +------------------+
                            +------------------+    |  Next.js Frontend|
                                                    |  SSE-driven panels|
                                                    +------------------+
```

### Why this split

- **Python** has the best libraries for data collection (requests, pandas, yfinance, fredapi) and scheduling (APScheduler).
- **Go** provides a fast, low-memory read-only HTTP server. DuckDB's Go driver opens the file in READ_ONLY mode so it never blocks the Python writer.
- **DuckDB** is an embedded analytical database with no server process. A single file is shared between processes; concurrent reads are safe; only one writer is allowed at a time (Python scheduler serializes writes via job ordering).
- **Next.js** with SSE avoids WebSocket complexity. The Go API holds an in-memory SSE broker; Python POSTs a notify event after each successful collection; the browser panel re-fetches its panel endpoint.

---

## Data Flow

```
External APIs
    |
    v
collectors/*.py          -- HTTP fetch, validate, write raw JSON snapshot to disk
    |
    v
facts_time_series        -- normalized time series (source, series, region, time, value)
    |
    v
transforms/features_*.py -- compute engineered features, write to features_daily
    |
    v
transforms/features_summary.py -- composite score, what-changed, write to summary_outputs
    |
    v
Go API endpoints         -- serve JSON to frontend panels
    |
    v
Next.js panels           -- subscribe to SSE, re-fetch panel data on notify event
```

---

## Components

### collectors/

Each file handles one data source. All collectors inherit `CollectorBase` which provides:
- `save_raw()` — writes the raw API response to `data/raw/<source>/<date>/<time>.json`
- `record_health()` — upserts a row in `collector_health` with last status
- `_notify()` — POST to Go API SSE broker (fire-and-forget)
- `run()` — wraps `collect()` with health recording and error logging

| File | Source | Frequency | Key data |
|---|---|---|---|
| `eia_storage.py` | EIA v2 seriesid | Weekly (Thu 10:45am) | Working gas storage by region, 104 weeks |
| `eia_storage_stats.py` | EIA ngsstats.xls | Weekly (Thu 11:00am) | 5yr avg/max/min by region and week |
| `eia_supply.py` | EIA v2 seriesid | Daily 8am | Dry production, LNG exports, power burn, Mexico pipeline, total imports, total pipeline exports (monthly) |
| `price.py` | yfinance + FRED | Every 30min (market hours) | Front-month OHLCV, 13-month forward curve, Henry Hub + heating oil spot history |
| `weather.py` | NWS weather.gov | Every 6h | 7-day forecasts for 8 cities, HDD/CDD base-65F, population-weighted |
| `cpc_outlook.py` | NOAA CPC GIS FTP | Daily 7am | 6-10 and 8-14 day temperature probability outlook per city via shapefile point-in-polygon |
| `power_burn.py` | EIA-930 | Hourly at :05 | Gas-fired generation (MWh) for 8 balancing authorities, 72h lookback |
| `cftc.py` | CFTC disaggregated COT | Friday 4pm | Managed money long/short, producer, swap, open interest |
| `rig_count.py` | Baker Hughes XLSB | Friday 2pm | U.S. natural gas rig count, 104 weeks; page-scraped UUID URL |

### transforms/

Each file reads from `facts_time_series`, computes derived features, and upserts to `features_daily`. All computations are idempotent — re-running overwrites today's row.

| File | Features computed |
|---|---|
| `features_storage.py` | total Bcf, WoW change, deficit vs 5yr avg, YoY, EOS projection, weekly pace |
| `features_price.py` | current price, daily/weekly/monthly % change, Nov-Jan spread, 12m strip avg |
| `features_weather.py` | 7-day population-weighted HDD sum, day-over-day forecast revision delta |
| `features_cpc.py` | population-weighted prob_below for 6-10 and 8-14 day CPC windows; interpretation (bullish → bearish) |
| `features_cot.py` | MM net contracts, MM net % of OI, WoW change, open interest |
| `features_summary.py` | composite fundamental score (-100 to +100), what-changed table |

### scheduler/jobs.py

APScheduler `BlockingScheduler` with US/Eastern timezone. Runs as a standalone process (`python -m scheduler.jobs`). Jobs are ordered so collectors fire before transforms. Feature transforms are staggered across the hour to avoid concurrent DuckDB writes. The summary job runs at :30 after all features are fresh.

**Collector schedule:**

| Job | Schedule | misfire_grace_time |
|-----|----------|--------------------|
| `price` | Mon–Fri 9–17h at :00 and :30 | 5 min |
| `weather` | Every 6h (0, 6, 12, 18 ET) | 10 min |
| `cpc_outlook` | Daily 7:00am ET | 15 min |
| `power_burn` | Every hour at :05 | 5 min |
| `eia_storage` | Thursday 10:45am ET | 15 min |
| `eia_storage_stats` | Thursday 11:00am ET | 15 min |
| `eia_supply` | Daily 8:00am ET | 15 min |
| `cftc_cot` | Friday 4:00pm ET | 30 min |
| `rig_count` | Friday 2:00pm ET | 30 min |

**Transform schedule (every hour, staggered):**

| Job | Minute | Depends on |
|-----|--------|------------|
| `feat_price` | :10 | price collector |
| `feat_storage` | :15 | eia_storage |
| `feat_weather` | :20 | weather |
| `feat_cpc` | :22 | cpc_outlook |
| `feat_cot` | :25 | cftc |
| `summary` | :30 | all features |

### api/ (Go)

Read-only HTTP server serving JSON to the Next.js frontend. Opens DuckDB with `access_mode=READ_ONLY` — never blocks the Python writer.

**Endpoints and response shapes:**

| Endpoint | Description | History included |
|----------|-------------|-----------------|
| `GET /api/score` | Composite score, label, drivers, what-changed | 90 days |
| `GET /api/storage` | Storage level, 5yr band, WoW change | 104 weeks, with aligned band |
| `GET /api/price` | OHLCV, forward curve, Henry Hub spot, heating oil spot | 90 days OHLCV, all curve months, 90 days spot |
| `GET /api/weather` | 7-day HDD summary, city breakdown, CPC 6-10/8-14 day outlook | 90 days weighted HDD/CDD |
| `GET /api/supply` | Dry gas production, LNG exports, power burn, Mexico pipeline, total imports, total pipeline exports, gas rig count | 12 months per EIA series; 104 weeks rig count |
| `GET /api/cot` | MM net positioning, OI | 52 weeks |
| `GET /api/health` | DB reachability, per-collector last status | — |
| `GET /api/stream` | SSE event stream | — |
| `POST /internal/notify` | Python → Go push; triggers SSE fan-out | — |

**Internal notify flow:** After each successful collection run, `CollectorBase._notify()` POSTs the `source_name` to `/internal/notify`. The Go SSE broker fans out a `collection_complete` event to all connected browser clients, which re-fetch their panel endpoint.

### db/schema.py

Defines all tables via `CREATE TABLE IF NOT EXISTS`. Run once to initialize. The Python plane is the sole writer; Go opens DuckDB with `access_mode=READ_ONLY`.

---

## Database Schema

### facts_time_series
Primary key: `(source_name, series_name, region, observation_time)`

The natural composite key enables `ON CONFLICT DO UPDATE` deduplication — re-running any collector is always safe.

### features_daily
Primary key: `(feature_date, feature_name, region)`

One row per computed feature per day. Recomputing overwrites via upsert.

### summary_outputs
Primary key: `(summary_date, summary_type)`

Stores two rows per day: `fundamental_score` (JSON with score/label/drivers) and `what_changed` (JSON array of feature deltas).

### collector_health
Primary key: `source_name`

One row per collector, updated after every run. Used by the Go API health endpoint and future alerting.

---

## Key Design Decisions

**DuckDB over PostgreSQL**: No server process to manage. Single file. Excellent analytical query performance. Sufficient for this workload (single Python writer, single Go reader, ~millions of rows max).

**Composite natural key over UUID**: The `(source_name, series_name, region, observation_time)` key reflects the true identity of a data point. UUID PKs would require a separate unique constraint anyway and add no value here.

**Per-source error isolation**: Every collector wraps each sub-request (per region, per series) in its own try/except. One failed EIA series does not abort the entire collection run.

**Observation time as TIMESTAMPTZ**: All times stored as timezone-aware. Weekly EIA data stored as `YYYY-MM-DDT00:00:00Z` (date-only periods). Hourly EIA-930 data stored as `YYYY-MM-DDTHH:00:00Z`. This allows consistent ordering and range queries without ambiguity.

**SSE over WebSocket**: Simpler server implementation, works through HTTP/1.1 proxies and load balancers without upgrade negotiation, sufficient for once-per-collection-run push cadence.

---

## Security

**Timing-safe key comparison**: The `/internal/notify` endpoint compares the `X-Internal-Key` header using `crypto/subtle.ConstantTimeCompare`, not string equality. String equality short-circuits on the first differing byte, leaking timing information about the key. Constant-time comparison always takes the same time regardless of where the strings differ.

**API keys out of URLs**: All EIA and FRED API keys are passed via `requests.get(params={...})`, not f-string URLs. Keys in URL strings appear in HTTP proxy logs, access logs, and browser history. The `requests` library appends `params` as a query string but many logging configurations only log the base URL.

**Panic recovery middleware**: The Go server wraps all handlers in a `recoverPanic` middleware that catches panics, logs the full stack trace via `slog.Error`, and returns HTTP 500 without crashing the server process. If an SSE stream is already open (i.e., `Content-Type: text/event-stream` is set), the error write is skipped since headers are already flushed.

**CORS `Vary: Origin` header**: The CORS middleware sets `Vary: Origin` on every response. This tells HTTP caches that the response depends on the `Origin` header. Without it, a cache could serve a response with `Access-Control-Allow-Origin: https://example.com` to a request from a different origin, causing incorrect CORS behavior.

**DB health check**: The `/api/health` endpoint calls `DB.PingContext()` to verify the DuckDB connection is live. If the ping fails it returns HTTP 503 with `db_ok: false`, enabling upstream health checks and load balancers to route traffic away from a degraded instance.

**Single Python writer**: APScheduler's `BlockingScheduler` serializes all collector and transform jobs. Feature transform jobs are staggered across the hour (:10/:15/:20/:25) so no two transforms write to DuckDB simultaneously. This avoids the `database is locked` IOException that DuckDB raises when multiple writers contend on the file.
