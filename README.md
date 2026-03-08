# Natural Gas Intelligence Terminal

A self-hosted trading intelligence terminal for swing and position trading of Henry Hub natural gas futures (NYMEX: NG). The system continuously collects fundamental data, computes engineered features, and surfaces a real-time composite score and dashboard.

## What It Does

- Collects weekly EIA storage, supply, and price data; NWS weather forecasts; CFTC COT positioning; and EIA-930 power burn
- Computes a composite fundamental score from -100 (max bearish) to +100 (max bullish)
- Tracks daily changes and flags when interpretations shift
- Serves data via a Go REST/SSE API to a Next.js dashboard

## Architecture

```
collectors/ (Python)  -->  DuckDB  <--  api/ (Go, read-only)  -->  frontend/ (Next.js)
transforms/ (Python)  -->  DuckDB
scheduler/  (Python)  -->  triggers collectors + transforms on cron
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full system design.

## Prerequisites

- Python 3.11+
- Go 1.22+
- Node.js 20+ (for the frontend)
- API keys: EIA Open Data, FRED

## Setup

```bash
git clone <repo>
cd nat-gas-terminal

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env and fill in EIA_API_KEY and FRED_API_KEY
```

### .env file

```
EIA_API_KEY=your_key_here
FRED_API_KEY=your_key_here
DATA_BASE_DIR=./data             # optional, defaults to ./data
NOTIFY_API_URL=http://localhost:8080/internal/notify  # optional
INTERNAL_API_KEY=                # optional
```

Get a free EIA key at https://www.eia.gov/opendata/
Get a free FRED key at https://fred.stlouisfed.org/docs/api/api_key.html

## Initialize the Database

```bash
python -m db.schema
```

## Populate Data (One-Time Seed)

Run collectors in order. Each prints a result dict when done.

```bash
python -c "from collectors.eia_storage import EIAStorageCollector; print(EIAStorageCollector().run())"
python -c "from collectors.eia_storage_stats import EIAStorageStatsCollector; print(EIAStorageStatsCollector().run())"
python -c "from collectors.eia_supply import EIASupplyCollector; print(EIASupplyCollector().run())"
python -c "from collectors.price import PriceCollector; print(PriceCollector().run())"
python -c "from collectors.weather import WeatherCollector; print(WeatherCollector().run())"
python -c "from collectors.power_burn import PowerBurnCollector; print(PowerBurnCollector().run())"
python -c "from collectors.cftc import CFTCCollector; print(CFTCCollector().run())"
```

Then compute features:

```bash
python -c "
from transforms.features_storage import compute_storage_features
from transforms.features_price   import compute_price_features
from transforms.features_weather import compute_weather_features
from transforms.features_cot     import compute_cot_features
from transforms.features_summary import save_summary
compute_storage_features()
compute_price_features()
compute_weather_features()
compute_cot_features()
save_summary()
print('done')
"
```

## Run the Scheduler

The scheduler runs all collectors and transforms on cron triggers:

```bash
python -m scheduler.jobs
```

Key schedules (all US/Eastern):
- Price: every 30 min, Mon-Fri 9am-5pm
- Weather: every 6 hours
- Power burn: hourly at :05
- EIA storage: Thursday 10:45am (15 min after release)
- EIA storage stats: Thursday 11:00am
- EIA supply: daily 8:00am
- CFTC COT: Friday 4:00pm
- Feature transforms: every hour at :10/:15/:20/:25
- Summary: every hour at :30

## Run the Go API

```bash
cd api
DB_PATH=../data/db/terminal.duckdb \
INTERNAL_API_KEY=your_internal_key \
ALLOWED_ORIGIN=http://localhost:3000 \
PORT=8080 \
go run ./main.go
```

The API opens DuckDB in READ_ONLY mode — it never blocks the Python writer. Set `INTERNAL_API_KEY` to a random secret in production; without it any caller can trigger SSE broadcasts.

Available endpoints: `/api/score`, `/api/storage`, `/api/price`, `/api/weather`, `/api/supply`, `/api/cot`, `/api/health`, `/api/stream` (SSE).

## Run Tests

```bash
pytest tests/ -v
```

All tests are fully isolated — no real API keys or network access required. Each test uses a temporary DuckDB instance.

## Project Structure

```
collectors/          Data collection modules (one file per source)
transforms/          Feature engineering (one file per feature group)
scheduler/           APScheduler cron jobs
db/                  DuckDB schema and connection helpers
config/              Settings loaded from .env
tests/               pytest tests + fixtures
  fixtures/          Static JSON/XLS fixtures for mocking HTTP responses
data/                Runtime data directory (gitignored)
  db/                DuckDB database file
  raw/               Raw JSON snapshots per source per day
  forecasts_archive/ Daily NWS forecast JSON archive (for revision delta)
logs/                Collector log file
docs/                Architecture, data dictionary, runbook
api/                 Go REST + SSE API server
```

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — system design and data flow
- [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md) — all series names, units, features, and their trading interpretation
- [docs/RUNBOOK.md](docs/RUNBOOK.md) — operational procedures, failure recovery, DB maintenance
