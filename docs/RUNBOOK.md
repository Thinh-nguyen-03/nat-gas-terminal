# Runbook

Operational procedures for the Natural Gas Intelligence Terminal.

---

## Starting the Python Scheduler

```bash
cd nat-gas-terminal
source .venv/bin/activate   # Windows: .venv\Scripts\activate
python -m scheduler.jobs
```

The scheduler is a blocking process. Run it in a terminal, screen session, or as a system service. It logs to both console and `logs/collectors.log`.

To run it in the background on Windows:
```bash
pythonw -m scheduler.jobs
# or configure Task Scheduler pointing to python.exe -m scheduler.jobs
```

---

## Starting the Go API

```bash
cd api
DB_PATH=../data/db/terminal.duckdb \
INTERNAL_API_KEY=your_internal_key \
ALLOWED_ORIGIN=http://localhost:3000 \
PORT=8080 \
go run ./main.go
```

Or with a built binary:
```bash
go build -o terminal-api ./main.go
DB_PATH=../data/db/terminal.duckdb INTERNAL_API_KEY=... ./terminal-api
```

The API opens DuckDB in `READ_ONLY` mode and never blocks the Python writer. Set `INTERNAL_API_KEY` in production — without it, any process that can reach the server can trigger SSE broadcasts.

---

## Running Collectors Manually

Run any collector on demand from the project root with the venv active:

```bash
python -c "from collectors.eia_storage import EIAStorageCollector; print(EIAStorageCollector().run())"
python -c "from collectors.eia_storage_stats import EIAStorageStatsCollector; print(EIAStorageStatsCollector().run())"
python -c "from collectors.eia_supply import EIASupplyCollector; print(EIASupplyCollector().run())"
python -c "from collectors.price import PriceCollector; print(PriceCollector().run())"
python -c "from collectors.weather import WeatherCollector; print(WeatherCollector().run())"
python -c "from collectors.cpc_outlook import CPCOutlookCollector; print(CPCOutlookCollector().run())"
python -c "from collectors.power_burn import PowerBurnCollector; print(PowerBurnCollector().run())"
python -c "from collectors.cftc import CFTCCollector; print(CFTCCollector().run())"
python -c "from collectors.rig_count import RigCountCollector; print(RigCountCollector().run())"
```

Each prints `{'status': 'ok', 'records_written': N}` on success.

All collectors are idempotent — re-running writes no duplicate rows due to `ON CONFLICT DO UPDATE`.

---

## Running Transforms Manually

```bash
python -c "
from transforms.features_storage import compute_storage_features
from transforms.features_price   import compute_price_features
from transforms.features_weather import compute_weather_features
from transforms.features_cpc     import compute_cpc_features
from transforms.features_cot     import compute_cot_features
from transforms.features_summary import save_summary
compute_storage_features()
compute_price_features()
compute_weather_features()
compute_cpc_features()
compute_cot_features()
save_summary()
print('done')
"
```

Transforms are idempotent. Re-running overwrites today's feature rows.

---

## Checking Database State

```bash
python -c "
from dotenv import load_dotenv; load_dotenv()
from config.settings import DB_PATH
import duckdb
conn = duckdb.connect(DB_PATH, read_only=True)
print('--- facts_time_series ---')
for r in conn.execute(
    'SELECT source_name, COUNT(*) FROM facts_time_series GROUP BY source_name ORDER BY source_name'
).fetchall():
    print(f'  {r[0]}: {r[1]}')
print()
print('--- features_daily ---', conn.execute('SELECT COUNT(*) FROM features_daily').fetchone()[0], 'rows')
print()
print('--- collector_health ---')
for r in conn.execute(
    'SELECT source_name, last_status, consecutive_failures, last_attempt FROM collector_health ORDER BY source_name'
).fetchall():
    print(f'  {r[0]}: {r[1]} (failures: {r[2]}) last: {r[3]}')
conn.close()
"
```

Or query the API health endpoint:
```bash
curl http://localhost:8080/api/health
```

Returns `db_ok: true` and per-collector last status. Returns HTTP 503 if the database is unreachable.

---

## Full Reseed (Wipe and Repopulate)

Use this when starting fresh after a schema change or data corruption.

```bash
python -c "
from dotenv import load_dotenv; load_dotenv()
from config.settings import DB_PATH
import duckdb
conn = duckdb.connect(DB_PATH)
for t in ['facts_time_series','features_daily','features_intraday',
          'summary_outputs','collector_health','raw_ingest',
          'catalyst_calendar','consensus_inputs','events']:
    conn.execute(f'DELETE FROM {t}')
    print('cleared', t)
conn.close()
"
```

Then run all collectors and transforms in order (see above).

To also reset the schema, delete `data/db/terminal.duckdb` and run `python -m db.schema`.

---

## Common Failure Scenarios

### EIA API timeout or 429

**Symptom:** `[eia_storage] total failed: Read timed out. (read timeout=90)`

The EIA API is intermittently slow. Per-region isolation means other regions still succeed. The failed region is retried on the next scheduled run. If all regions fail consistently, check https://www.eia.gov/opendata/ and verify your API key.

Note: EIA API keys are passed via `params=` (not embedded in URLs), so they will not appear in request logs.

### Baker Hughes rig count timeout

**Symptom:** `[baker_hughes] failed: Read timed out. (read timeout=300)`

The `rigcount.bakerhughes.com` domain occasionally rate-limits or throttles connections, especially after a recent large download (~8 MB Excel file). This is transient — the scheduler retries on the next Friday run.

If it fails consistently: the HTML page scrape extracts a UUID-based file URL that changes each week. If the URL format changes, update `_get_file_url()` in `collectors/rig_count.py`. Check the page source at `https://rigcount.bakerhughes.com/na-rig-count` and update the regex pattern for the static-files UUID.

To run manually after the domain recovers:
```bash
python -c "from collectors.rig_count import RigCountCollector; print(RigCountCollector().run())"
```

### yfinance returns empty data

**Symptom:** `[price] yfinance returned no data for NG=F`

Yahoo Finance occasionally rejects requests outside US market hours or rate-limits. Retry during market hours (9am–5pm ET Mon–Fri). FRED spot price writes succeed regardless.

If persistent: `pip install --upgrade yfinance` — Yahoo Finance changes its auth mechanism periodically.

### CFTC ZIP download fails

**Symptom:** `[cftc_cot] failed: 404`

The CFTC URL format is `https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip`. If the year just changed, the new file may not be available yet — manually override:

```python
from collectors.cftc import CFTCCollector, CFTC_URL
import requests, io, zipfile, pandas as pd
url = CFTC_URL.format(year=2025)  # prior year
resp = requests.get(url, timeout=120, headers={"User-Agent": "NatGasTerminal/1.0"})
with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
    with z.open(z.namelist()[0]) as f:
        df = pd.read_csv(f)
print(df[df["CFTC_Contract_Market_Code"] == "023651"].tail(3))
```

### NWS forecast URL missing for a city

**Symptom:** `[nws_weather] chicago failed: NWS points response missing forecast URL for chicago`

The NWS two-step API (`/points` → `/forecast`) can return a response with no forecast URL transiently. The city is skipped; remaining cities still process. No action needed — it retries on the next 6-hour run.

### DuckDB write conflict

**Symptom:** `duckdb.duckdb.IOException: database is locked`

Only one Python writer is allowed at a time. This occurs if two scheduler processes are running simultaneously, or a manual collector runs while the scheduler runs the same job.

Stop all Python processes, then restart:
```bash
# Windows — check for running processes
tasklist | findstr python
# Restart
python -m scheduler.jobs
```

### Go API panic

The API has panic recovery middleware. If a handler panics, the server returns HTTP 500 and logs the full stack trace with `"msg":"panic recovered"`. The server process does **not** exit. Check `stdout` or your process supervisor logs for the stack trace.

---

## Updating EIA Series IDs

EIA occasionally reorganizes their v2 API series paths without notice.

If a collector returns 404:
1. Go to https://www.eia.gov/opendata/browser/natural-gas
2. Find the correct series ID
3. Update the series dict in the collector file
4. Update the test fixture if the series name or expected record count changes

Known series IDs (as of March 2026):

| Series | ID |
|--------|----|
| Storage total (Lower 48) | `NG.NW2_EPG0_SWO_R48_BCF.W` |
| Storage east | `NG.NW2_EPG0_SWO_R31_BCF.W` |
| Storage midwest | `NG.NW2_EPG0_SWO_R32_BCF.W` |
| Storage south central | `NG.NW2_EPG0_SWO_R33_BCF.W` |
| Storage mountain | `NG.NW2_EPG0_SWO_R34_BCF.W` |
| Storage pacific | `NG.NW2_EPG0_SWO_R35_BCF.W` |
| Dry gas production | `NG.N9050US2.M` |
| LNG exports | `NG.N9133US2.M` |
| Power sector burn | `NG.N3045US2.M` |
| Mexico pipeline exports | `NG.N9132MX2.M` |
| Total US imports | `NG.N9100US2.M` |
| Total pipeline exports | `NG.N9130US2.M` |

Note: Canada-specific import series do not exist in EIA v2 seriesid. `NG.N9100US2.M` (total US imports) is used as a proxy since Canada accounts for ~99% of US gas imports.

---

## Updating CFTC Column Names

The CFTC CSV column names have changed subtly across years (e.g. `_ALL` vs `_All`). If the CFTC collector writes 0 rows for MM positions, inspect the actual columns:

```python
import io, zipfile, requests, pandas as pd
resp = requests.get(
    'https://www.cftc.gov/files/dea/history/fut_disagg_txt_2026.zip',
    timeout=120, headers={'User-Agent': 'NatGasTerminal/1.0'}
)
with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
    with z.open(z.namelist()[0]) as f:
        df = pd.read_csv(f)
print([c for c in df.columns if 'Money' in c])
```

Update `MM_POSITION_COLUMNS` in `collectors/cftc.py` to match.

---

## Adding a New Collector

1. Create `collectors/<name>.py` inheriting `CollectorBase`
2. Set `source_name` class attribute
3. Implement `collect() -> dict` returning `{"status": "ok", "records_written": N}`
4. Use `requests.get(..., params={"api_key": KEY, ...})` — never embed keys in URL strings
5. Add a scheduled job in `scheduler/jobs.py`
6. Add tests in `tests/test_collectors.py` with a fixture in `tests/fixtures/`
7. Add series to `docs/DATA_DICTIONARY.md`

---

## Log File

Logs are written to `logs/collectors.log` in addition to stdout.

```bash
# Unix
tail -f logs/collectors.log

# PowerShell
Get-Content logs/collectors.log -Wait
```

The Go API logs structured JSON to stdout. Each log line includes `level`, `msg`, and relevant context fields.

---

## Running Tests

```bash
pytest tests/ -v
```

All tests are fully isolated — no real API keys or network access required. Each test uses a temporary DuckDB instance via pytest fixtures.
