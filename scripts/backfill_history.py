"""
Historical data backfill script.

Populates facts_time_series with historical data needed for:
  - Feature 6 (analog finder): needs years of weekly feature snapshots
  - Feature 9 (fair value model): needs training data for OLS regression

Run once from the project root after the schema is initialized:

    python -m scripts.backfill_history [--start 2010-01-01] [--section storage|cot|prices|all]

Sections:
  storage  — EIA weekly natural gas storage back to 2010 (default start)
  cot      — CFTC disaggregated COT history back to 2010
  prices   — NG=F daily OHLCV (yfinance) + FRED spot/TTF back to 2010

The script writes to facts_time_series using the same source_name / series_name
conventions as the live collectors, so existing feature transforms will pick up
the backfilled history automatically on their next run.

Progress is printed to stdout. The script is idempotent — re-running it will
update existing rows (ON CONFLICT DO UPDATE) without creating duplicates.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests

# ---------------------------------------------------------------------------
# Bootstrap path so we can import project modules when run as a script
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DB_PATH, EIA_API_KEY, FRED_API_KEY, NOAA_CDO_TOKEN  # noqa: E402

logger = logging.getLogger("backfill")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

_UPSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
"""

# ---------------------------------------------------------------------------
# EIA v2 storage backfill
# ---------------------------------------------------------------------------

# Same series IDs as the live EIAStorageCollector — ensures backfilled rows
# are written with the same source_name/series_name/region keys.
_EIA_STORAGE_SERIES: dict[str, tuple[str, str]] = {
    "NG.NW2_EPG0_SWO_R48_BCF.W": ("storage_total",         "total"),
    "NG.NW2_EPG0_SWO_R31_BCF.W": ("storage_east",          "east"),
    "NG.NW2_EPG0_SWO_R32_BCF.W": ("storage_midwest",       "midwest"),
    "NG.NW2_EPG0_SWO_R33_BCF.W": ("storage_south_central", "south_central"),
    "NG.NW2_EPG0_SWO_R34_BCF.W": ("storage_mountain",      "mountain"),
    "NG.NW2_EPG0_SWO_R35_BCF.W": ("storage_pacific",       "pacific"),
}

_EIA_SERIESID_URL = "https://api.eia.gov/v2/seriesid/{series_id}"

# Max history the seriesid endpoint returns per call — use a large number
# to get everything in one shot (EIA series go back to mid-1990s, ~1600 weeks)
_EIA_MAX_LENGTH = 2000


def backfill_eia_storage(conn, start: str = "2010-01-01") -> None:
    """Pull EIA weekly storage stocks (Bcf) using the same seriesid endpoint
    as the live EIAStorageCollector, but with extended history."""
    logger.info("=== EIA storage backfill (start=%s) ===", start)
    now_str = datetime.now(timezone.utc).isoformat()
    total_rows = 0

    for series_id, (series_name, region) in _EIA_STORAGE_SERIES.items():
        logger.info("  Fetching %s → %s (%s)...", series_id, series_name, region)
        resp = requests.get(
            _EIA_SERIESID_URL.format(series_id=series_id),
            params={"api_key": EIA_API_KEY, "data[0]": "value", "length": _EIA_MAX_LENGTH},
            timeout=60,
        )
        resp.raise_for_status()
        payload = resp.json()

        area_rows = 0
        for row in payload.get("response", {}).get("data", []):
            period = row.get("period", "")
            value  = row.get("value")
            if not period or value is None:
                continue
            if period < start:
                continue
            obs_time = f"{period}T00:00:00Z"
            conn.execute(_UPSERT_SQL, [
                "eia_storage", f"storage_{region}", region,
                obs_time, now_str, float(value), "Bcf", "weekly",
            ])
            area_rows += 1

        logger.info("  %s: %d rows written", series_name, area_rows)
        total_rows += area_rows

    logger.info("EIA storage backfill complete: %d rows total", total_rows)


# ---------------------------------------------------------------------------
# CFTC COT backfill
# ---------------------------------------------------------------------------

_CFTC_ZIP_URL = "https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"

# Henry Hub Natural Gas CFTC contract market code
_NG_CFTC_CODE = "023651"

# Column indices in the CFTC disaggregated futures CSV (0-based).
# These are stable across years but verify against the header row.
_CFTC_COLS = {
    "report_date":     "Report_Date_as_MM_DD_YYYY",
    "open_interest":   "Open_Interest_All",
    "mm_long":         "M_Money_Positions_Long_All",
    "mm_short":        "M_Money_Positions_Short_All",
    "mm_spread":       "M_Money_Positions_Spread_All",
    "prod_long":       "Prod_Merc_Positions_Long_All",
    "prod_short":      "Prod_Merc_Positions_Short_All",
    "swap_long":       "Swap_Positions_Long_All",
    "swap_short":      "Swap__Positions_Short_All",
    "contract_code":   "CFTC_Contract_Market_Code",
}


def backfill_cftc_cot(conn, start_year: int = 2010) -> None:
    """Pull CFTC disaggregated COT history from annual ZIP files."""
    import csv

    logger.info("=== CFTC COT backfill (start_year=%d) ===", start_year)
    now_str  = datetime.now(timezone.utc).isoformat()
    end_year = datetime.now().year
    total_rows = 0

    for year in range(start_year, end_year + 1):
        url = _CFTC_ZIP_URL.format(year=year)
        logger.info("  Fetching %s ...", url)

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
        except requests.HTTPError as e:
            logger.warning("  Skipping %d: %s", year, e)
            continue

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            # The ZIP contains one TXT/CSV file per year
            csv_name = next(
                (n for n in zf.namelist() if n.lower().endswith((".txt", ".csv"))),
                None,
            )
            if not csv_name:
                logger.warning("  No CSV found in ZIP for %d", year)
                continue

            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="latin-1"))
                year_rows = 0

                for row in reader:
                    if row.get(_CFTC_COLS["contract_code"], "").strip() != _NG_CFTC_CODE:
                        continue

                    # CFTC changed column name across years:
                    # older files: Report_Date_as_MM_DD_YYYY
                    # 2014+ files: Report_Date_as_YYYY-MM-DD
                    date_str = (
                        row.get("Report_Date_as_MM_DD_YYYY") or
                        row.get("Report_Date_as_YYYY-MM-DD") or
                        ""
                    ).strip()
                    if not date_str:
                        continue
                    # CFTC changed date format across years: try MM/DD/YYYY then YYYY-MM-DD
                    dt = None
                    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                        try:
                            dt = datetime.strptime(date_str, fmt)
                            break
                        except ValueError:
                            continue
                    if dt is None:
                        logger.warning("  Unrecognised date format: %s", date_str)
                        continue
                    obs_time = dt.strftime("%Y-%m-%dT00:00:00Z")

                    def _int(col: str) -> float | None:
                        v = row.get(_CFTC_COLS[col], "").strip().replace(",", "")
                        try:
                            return float(v) if v else None
                        except ValueError:
                            return None

                    oi      = _int("open_interest")
                    mm_long = _int("mm_long")
                    mm_short= _int("mm_short")

                    series_values = [
                        ("cot_open_interest",      oi,                "contracts"),
                        ("cot_mm_long",            mm_long,           "contracts"),
                        ("cot_mm_short",           mm_short,          "contracts"),
                        ("cot_mm_net",
                            (mm_long - mm_short) if (mm_long and mm_short) else None,
                            "contracts"),
                        ("cot_mm_net_pct_oi",
                            ((mm_long - mm_short) / oi * 100)
                            if (mm_long and mm_short and oi and oi > 0) else None,
                            "pct_of_oi"),
                        ("cot_prod_long",          _int("prod_long"),  "contracts"),
                        ("cot_prod_short",         _int("prod_short"), "contracts"),
                        ("cot_swap_long",          _int("swap_long"),  "contracts"),
                        ("cot_swap_short",         _int("swap_short"), "contracts"),
                    ]

                    for series_name, value, unit in series_values:
                        if value is None:
                            continue
                        conn.execute(_UPSERT_SQL, [
                            "cftc", series_name, "US",
                            obs_time, now_str, value, unit, "weekly",
                        ])
                        year_rows += 1

                logger.info("  %d: %d rows written", year, year_rows)
                total_rows += year_rows

    logger.info("CFTC COT backfill complete: %d rows total", total_rows)


# ---------------------------------------------------------------------------
# Price history backfill  (yfinance NG=F + FRED spot)
# ---------------------------------------------------------------------------

# Same series names as live PriceCollector — backfilled rows are
# indistinguishable from live data so Feature 9 training can use both.
_FRED_PRICE_SERIES: dict[str, tuple[str, str]] = {
    "DHHNGSP":     ("ng_spot_price",    "USD/MMBtu"),  # Henry Hub daily spot
    "DHOILNYH":    ("heating_oil_spot", "USD/gal"),    # NY harbor heating oil
    "PNGASEUUSDM": ("ttf_spot",         "USD/MMBtu"),  # European TTF (monthly)
}

_OHLCV_FIELDS = ["Open", "High", "Low", "Close", "Volume"]


def backfill_prices(conn, start: str = "2010-01-01") -> None:
    """Download full NG=F OHLCV history from yfinance and FRED spot series."""
    import pandas as pd
    import yfinance as yf
    from fredapi import Fred

    logger.info("=== Price backfill (start=%s) ===", start)
    now_str = datetime.now(timezone.utc).isoformat()
    total_rows = 0

    # --- NG=F front-month continuous contract ---
    logger.info("  Fetching NG=F from yfinance (%s → today)...", start)
    hist = yf.download("NG=F", start=start, interval="1d",
                       progress=False, auto_adjust=True)
    if hist.empty:
        logger.warning("  yfinance returned no data for NG=F — check network / API")
    else:
        # yfinance ≥1.0 returns MultiIndex columns (Price, Ticker); flatten.
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)

        ng_rows = 0
        for idx, row in hist.iterrows():
            ts = idx
            if hasattr(ts, "tz_localize"):
                ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
            obs_time = ts.isoformat()
            for field in _OHLCV_FIELDS:
                val = row.get(field)
                if val is None or (isinstance(val, float) and val != val):
                    continue
                unit = "contracts" if field == "Volume" else "USD/MMBtu"
                conn.execute(_UPSERT_SQL, [
                    "yfinance",
                    f"ng_front_{field.lower()}",
                    "US",
                    obs_time,
                    now_str,
                    float(val),
                    unit,
                    "daily",
                ])
                ng_rows += 1
        logger.info("  NG=F: %d rows written (%d trading days × 5 fields)",
                    ng_rows, ng_rows // len(_OHLCV_FIELDS) or ng_rows)
        total_rows += ng_rows

    # --- FRED spot / TTF series ---
    if not FRED_API_KEY:
        logger.warning("  FRED_API_KEY not set — skipping FRED series")
    else:
        fred = Fred(api_key=FRED_API_KEY)
        for fred_id, (series_name, unit) in _FRED_PRICE_SERIES.items():
            try:
                logger.info("  Fetching FRED %s → %s...", fred_id, series_name)
                series = fred.get_series(fred_id, observation_start=start)
                fred_rows = 0
                for date, value in series.items():
                    if value != value:  # NaN check
                        continue
                    obs_time = date.strftime("%Y-%m-%dT00:00:00Z")
                    conn.execute(_UPSERT_SQL, [
                        "fred", series_name, "US",
                        obs_time, now_str, float(value), unit, "daily",
                    ])
                    fred_rows += 1
                logger.info("  %s: %d rows written", series_name, fred_rows)
                total_rows += fred_rows
            except Exception as e:
                logger.warning("  FRED %s failed: %s", fred_id, e)

    logger.info("Price backfill complete: %d rows total", total_rows)


# ---------------------------------------------------------------------------
# NOAA CDO historical HDD backfill
# ---------------------------------------------------------------------------

# GHCND station IDs matching the 8 cities in collectors/weather.py.
# Verified against NOAA station listings for the nearest climate-grade station
# to each city's lat/lon.
_NOAA_STATIONS: dict[str, str] = {
    "new_york":     "GHCND:USW00094728",  # NYC Central Park
    "chicago":      "GHCND:USW00094846",  # Chicago O'Hare
    "boston":       "GHCND:USW00014739",  # Boston Logan
    "philadelphia": "GHCND:USW00013739",  # Philadelphia Intl
    "houston":      "GHCND:USW00012918",  # Houston Hobby
    "atlanta":      "GHCND:USW00013874",  # Atlanta Hartsfield-Jackson
    "minneapolis":  "GHCND:USW00014922",  # Minneapolis-St Paul
    "detroit":      "GHCND:USW00094847",  # Detroit Metro
}

_NOAA_CDO_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
_HDD_BASE_F   = 65.0


def backfill_noaa_hdd(conn, start_year: int = 2010) -> None:
    """
    Fetch GHCND daily TMAX/TMIN for each of the 8 gas-demand cities, compute
    population-weighted HDD, and write a national daily weighted HDD series to
    facts_time_series (source_name='noaa_hdd_historical').

    Requires NOAA_CDO_TOKEN in .env (free token from ncdc.noaa.gov/cdo-web/token).
    One request per city per year; well within the 1000 req/day free-tier limit.
    """
    import time

    if not NOAA_CDO_TOKEN:
        logger.error("NOAA_CDO_TOKEN not set — skipping HDD backfill")
        return

    from collectors.weather import WEATHER_POINTS

    logger.info("=== NOAA HDD backfill (start_year=%d) ===", start_year)
    now_str  = datetime.now(timezone.utc).isoformat()
    end_year = datetime.now().year

    # daily_hdd[date_str] = {"weighted_sum": float, "weight_total": float}
    daily_hdd: dict[str, dict] = {}

    for city, station_id in _NOAA_STATIONS.items():
        weight = WEATHER_POINTS[city]["pop_weight"]
        logger.info("  %s (weight=%.2f) ...", city, weight)

        for year in range(start_year, end_year + 1):
            start_date = f"{year}-01-01"
            end_date   = (
                f"{year}-12-31"
                if year < end_year
                else datetime.now().strftime("%Y-%m-%d")
            )
            try:
                resp = requests.get(
                    _NOAA_CDO_URL,
                    params={
                        "datasetid":  "GHCND",
                        "stationid":  station_id,
                        "datatypeid": "TMAX,TMIN",
                        "startdate":  start_date,
                        "enddate":    end_date,
                        "limit":      1000,
                    },
                    headers={"token": NOAA_CDO_TOKEN},
                    timeout=30,
                )
                resp.raise_for_status()
            except requests.HTTPError as exc:
                logger.warning("  %s %d: HTTP %s — skipping", city, year, exc)
                time.sleep(0.5)
                continue

            results = resp.json().get("results", [])
            # Pair TMAX and TMIN by date
            by_date: dict[str, dict] = {}
            for rec in results:
                d = rec["date"][:10]           # "2010-01-01T00:00:00" -> "2010-01-01"
                dtype = rec["datatype"]
                value = rec["value"]           # tenths of degrees Celsius
                by_date.setdefault(d, {})[dtype] = value

            year_rows = 0
            for d, vals in by_date.items():
                tmax_raw = vals.get("TMAX")
                tmin_raw = vals.get("TMIN")
                if tmax_raw is None or tmin_raw is None:
                    continue
                tmax_f = (tmax_raw / 10.0) * 9 / 5 + 32
                tmin_f = (tmin_raw / 10.0) * 9 / 5 + 32
                avg_f  = (tmax_f + tmin_f) / 2.0
                hdd    = max(0.0, _HDD_BASE_F - avg_f)

                entry = daily_hdd.setdefault(d, {"weighted_sum": 0.0, "weight_total": 0.0})
                entry["weighted_sum"]   += hdd * weight
                entry["weight_total"]   += weight
                year_rows += 1

            logger.info("  %s %d: %d days processed", city, year, year_rows)
            time.sleep(0.25)   # stay well under 5 req/s limit

    # Write national weighted HDD to facts_time_series
    total_rows = 0
    for d_str, entry in sorted(daily_hdd.items()):
        if entry["weight_total"] < 0.5:   # skip days with most cities missing
            continue
        # Normalise by actual weight coverage in case some cities had no data
        weighted_hdd = entry["weighted_sum"] / entry["weight_total"] * sum(
            WEATHER_POINTS[c]["pop_weight"] for c in WEATHER_POINTS
        )
        obs_time = f"{d_str}T00:00:00Z"
        conn.execute(_UPSERT_SQL, [
            "noaa_hdd_historical", "hdd_weighted_national", "US",
            obs_time, now_str, weighted_hdd, "degree-days", "daily",
        ])
        total_rows += 1

    logger.info("NOAA HDD backfill complete: %d daily rows written", total_rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--section",
        choices=["storage", "cot", "prices", "hdd", "all"],
        default="all",
        help="Which data section to backfill (default: all)",
    )
    parser.add_argument(
        "--start",
        default="2010-01-01",
        help="Start date for EIA storage backfill (default: 2010-01-01)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2010,
        dest="start_year",
        help="Start year for CFTC COT backfill (default: 2010)",
    )
    args = parser.parse_args()

    logger.info("Connecting to DuckDB at %s", DB_PATH)
    conn = duckdb.connect(DB_PATH)

    try:
        if args.section in ("storage", "all"):
            backfill_eia_storage(conn, start=args.start)

        if args.section in ("cot", "all"):
            backfill_cftc_cot(conn, start_year=args.start_year)

        if args.section in ("prices", "all"):
            backfill_prices(conn, start=args.start)

        if args.section in ("hdd", "all"):
            backfill_noaa_hdd(conn, start_year=args.start_year)
    finally:
        conn.close()

    logger.info("Backfill complete. Run the feature transforms to rebuild features_daily:")
    logger.info("  python -c \"from transforms.features_storage import compute_storage_features; compute_storage_features()\"")
    logger.info("  python -c \"from transforms.features_cot import compute_cot_features; compute_cot_features()\"")
    logger.info("  python -c \"from transforms.features_analog import compute_analog_features; compute_analog_features()\"")


if __name__ == "__main__":
    main()
