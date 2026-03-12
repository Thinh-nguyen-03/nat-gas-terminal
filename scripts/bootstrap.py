"""
One-time bootstrap script — run this after initial setup to populate the
database with historical data and seed every panel on the dashboard.

    python -m scripts.bootstrap [--start 2010-01-01]

Steps:
  1. Historical backfill  (storage, COT, prices, NOAA HDD — back to --start)
  2. Live collectors      (weather, power, LNG, CPC, calendar, supply, rig count, ISO LMP)
  3. All feature transforms in dependency order
  4. Final composite score + analog finder

Re-running is safe — all writes are ON CONFLICT DO UPDATE (idempotent).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("bootstrap")

# ---------------------------------------------------------------------------
# Step 1 — Historical backfill
# ---------------------------------------------------------------------------

def step_backfill(start: str, start_year: int) -> None:
    import duckdb
    from config.settings import DB_PATH
    from scripts.backfill_history import (
        backfill_eia_storage,
        backfill_cftc_cot,
        backfill_prices,
        backfill_noaa_hdd,
    )

    logger.info("STEP 1 — Historical backfill (start=%s)", start)

    conn = duckdb.connect(DB_PATH)
    try:
        backfill_prices(conn, start=start)
        backfill_eia_storage(conn, start=start)
        backfill_cftc_cot(conn, start_year=start_year)
        backfill_noaa_hdd(conn, start_year=start_year)
    finally:
        conn.close()

    logger.info("Step 1 complete.")


# ---------------------------------------------------------------------------
# Step 2 — Run live collectors once (current snapshots only)
# ---------------------------------------------------------------------------

def step_live_collectors() -> None:
    logger.info("STEP 2 — Live collectors (current snapshots)")

    collectors = [
        ("WeatherCollector",          "collectors.weather",           "WeatherCollector"),
        ("CPCOutlookCollector",       "collectors.cpc_outlook",       "CPCOutlookCollector"),
        ("PowerBurnCollector",        "collectors.power_burn",        "PowerBurnCollector"),
        ("EIASupplyCollector",        "collectors.eia_supply",        "EIASupplyCollector"),
        ("EIAStorageCollector",       "collectors.eia_storage",       "EIAStorageCollector"),
        ("EIAStorageStatsCollector",  "collectors.eia_storage_stats", "EIAStorageStatsCollector"),
        ("CFTCCollector",             "collectors.cftc",              "CFTCCollector"),
        ("RigCountCollector",         "collectors.rig_count",         "RigCountCollector"),
        # LNGVesselsCollector omitted — AIS data is now collected by the Go
        # cmd/ais binary which runs as a persistent process alongside the API.
        ("ISOLMPCollector",           "collectors.iso_lmp",           "ISOLMPCollector"),
        ("CatalystCalendarCollector", "collectors.catalyst_calendar", "CatalystCalendarCollector"),
        ("NewsWireCollector",         "collectors.news_wire",         "NewsWireCollector"),
        ("PriceCollector",            "collectors.price",             "PriceCollector"),
    ]

    for name, module_path, class_name in collectors:
        logger.info("Running %s ...", name)
        try:
            import importlib
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            result = cls().run()
            logger.info("  %s → %s", name, result)
        except Exception as e:
            logger.warning("  %s failed (non-fatal): %s", name, e)

    logger.info("Step 2 complete.")


# ---------------------------------------------------------------------------
# Step 3 — Feature transforms in dependency order
# ---------------------------------------------------------------------------

def step_transforms() -> None:
    logger.info("STEP 3 — Feature transforms")

    from transforms.features_price        import compute_price_features
    from transforms.features_storage      import compute_storage_features
    from transforms.features_weather      import compute_weather_features
    from transforms.features_cpc          import compute_cpc_features
    from transforms.features_cot          import compute_cot_features
    from transforms.features_power_demand import compute_power_demand_features
    from transforms.features_lng          import compute_lng_features, backfill_lng_from_eia
    from transforms.features_fairvalue    import compute_fairvalue_features
    from transforms.features_summary      import save_summary
    from transforms.features_analog       import compute_analog_features
    from transforms.market_brief          import compute_market_brief

    transforms = [
        ("feat_price",        compute_price_features),
        ("feat_storage",      compute_storage_features),
        ("feat_weather",      compute_weather_features),
        ("feat_cpc",          compute_cpc_features),
        ("feat_cot",          compute_cot_features),
        ("feat_power_demand", compute_power_demand_features),
        ("lng_eia_backfill",  backfill_lng_from_eia),
        ("feat_lng",          compute_lng_features),
        ("feat_fairvalue",    compute_fairvalue_features),
        ("summary",           save_summary),
        ("feat_analog",       compute_analog_features),
        ("market_brief",      compute_market_brief),
    ]

    for name, fn in transforms:
        logger.info("Running %s ...", name)
        try:
            fn()
            logger.info("  %s done", name)
        except Exception as e:
            logger.warning("  %s failed (non-fatal): %s", name, e)

    logger.info("Step 3 complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--start",
        default="2010-01-01",
        help="Start date for historical backfill (default: 2010-01-01)",
    )
    parser.add_argument(
        "--skip-backfill", action="store_true",
        help="Skip step 1 (historical backfill) — only run live collectors + transforms",
    )
    parser.add_argument(
        "--skip-collectors", action="store_true",
        help="Skip step 2 (live collectors)",
    )
    parser.add_argument(
        "--transforms-only", action="store_true",
        help="Only run feature transforms (step 3)",
    )
    args = parser.parse_args()

    try:
        start_year = int(args.start[:4])
    except ValueError:
        start_year = 2010

    if args.transforms_only:
        step_transforms()
        return

    if not args.skip_backfill:
        step_backfill(args.start, start_year)

    if not args.skip_collectors:
        step_live_collectors()

    step_transforms()

    logger.info("Bootstrap complete. Refresh the dashboard.")


if __name__ == "__main__":
    main()
