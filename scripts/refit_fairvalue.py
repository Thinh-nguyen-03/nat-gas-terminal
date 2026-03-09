"""
Refit the fair value OLS regression model and write calibrated coefficients.

    python -m scripts.refit_fairvalue [--start 2010-01-01] [--window-years 3]

Uses features already in facts_time_series (storage, price, COT) plus
historical HDD from noaa_hdd_historical (requires --section hdd backfill).
Falls back to features_daily HDD for recent weeks not yet in the historical
archive.

Writes transforms/fairvalue_coefficients.json on success. The transform
(transforms/features_fairvalue.py) loads this file and switches from lookup-
table mode to OLS mode automatically.

Run quarterly, or whenever the backfill data is materially updated.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DB_PATH  # noqa: E402

logger = logging.getLogger("refit_fairvalue")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

_WINTER_MONTHS = frozenset({10, 11, 12, 1, 2, 3})
_COEFF_PATH    = PROJECT_ROOT / "transforms" / "fairvalue_coefficients.json"


# ---------------------------------------------------------------------------
# Data assembly
# ---------------------------------------------------------------------------

def _build_training_set(
    conn,
    start: str,
    window_years: int,
) -> tuple[list, list, list[str]]:
    """
    Assemble weekly (X, y) pairs aligned to EIA storage report dates.
    Returns (X_rows, y_rows, feature_names).

    Features:
        storage_deficit_vs_5yr_bcf  — actual minus 5yr same-week rolling avg
        hdd_7d_weighted             — 7-day realized weighted HDD ending on obs_date
        cot_mm_net_pct_oi           — managed money net % of open interest
        season_winter               — 1 if Oct-Mar, else 0

    Target:
        y — weekly average Henry Hub spot price (USD/MMBtu)
    """
    cutoff = (datetime.now() - timedelta(days=window_years * 365)).strftime("%Y-%m-%d")
    effective_start = max(start, cutoff)

    # --- Storage: weekly ---
    storage_rows = conn.execute("""
        SELECT observation_time::DATE AS d, value
        FROM facts_time_series
        WHERE source_name = 'eia_storage' AND series_name = 'storage_total'
          AND observation_time::DATE >= ?
        ORDER BY d
    """, [effective_start]).fetchall()

    # Need full history to compute rolling avg (go back 5 extra years)
    all_storage_rows = conn.execute("""
        SELECT observation_time::DATE AS d, value
        FROM facts_time_series
        WHERE source_name = 'eia_storage' AND series_name = 'storage_total'
        ORDER BY d
    """).fetchall()
    all_storage = {r[0]: r[1] for r in all_storage_rows}
    all_dates   = sorted(all_storage.keys())

    # --- Daily prices ---
    price_rows = conn.execute("""
        SELECT observation_time::DATE AS d, value, source_name
        FROM facts_time_series
        WHERE (source_name = 'fred'     AND series_name = 'ng_spot_price')
           OR (source_name = 'yfinance' AND series_name = 'ng_front_close')
        ORDER BY d
    """).fetchall()
    prices: dict[date, float] = {}
    for d, v, src in price_rows:
        if d not in prices or src == "fred":
            prices[d] = v

    # --- COT: weekly ---
    cot_rows = conn.execute("""
        SELECT observation_time::DATE AS d, value
        FROM facts_time_series
        WHERE source_name = 'cftc' AND series_name = 'cot_mm_net_pct_oi'
        ORDER BY d
    """).fetchall()
    cot_by_date = {r[0]: r[1] for r in cot_rows}

    # --- Historical HDD: daily national weighted ---
    hdd_rows = conn.execute("""
        SELECT observation_time::DATE AS d, value
        FROM facts_time_series
        WHERE source_name = 'noaa_hdd_historical'
          AND series_name  = 'hdd_weighted_national'
        ORDER BY d
    """).fetchall()
    hdd_hist = {r[0]: r[1] for r in hdd_rows}

    # Fallback: live features_daily HDD (forecast, used for recent weeks)
    hdd_live_rows = conn.execute("""
        SELECT feature_date, value FROM features_daily
        WHERE feature_name = 'hdd_7d_weighted' AND region = 'US'
        ORDER BY feature_date
    """).fetchall()
    hdd_live = {r[0]: r[1] for r in hdd_live_rows}

    # --- Assemble weekly rows ---
    feature_names = [
        "storage_deficit_vs_5yr_bcf",
        "hdd_7d_weighted",
        "cot_mm_net_pct_oi",
        "season_winter",
    ]
    X_rows: list[list[float]] = []
    y_rows: list[float]       = []

    for obs_date, _ in storage_rows:
        val = all_storage.get(obs_date)
        if val is None:
            continue

        iso_week = obs_date.isocalendar()[1]
        comparables = [
            all_storage[d] for d in all_dates
            if 1 <= obs_date.year - d.year <= 5
            and d.isocalendar()[1] == iso_week
        ]
        if len(comparables) < 3:
            continue
        deficit = val - sum(comparables) / len(comparables)

        # 7-day sum of daily HDD ending on obs_date
        hdd_7d = 0.0
        hdd_days = 0
        for delta in range(7):
            d = obs_date - timedelta(days=delta)
            v = hdd_hist.get(d) or hdd_live.get(d)
            if v is not None:
                hdd_7d += v
                hdd_days += 1
        if hdd_days < 4:
            continue

        # Nearest COT within ±5 days
        cot = None
        for delta in range(6):
            for sign in (0, 1, -1):
                cot = cot_by_date.get(obs_date + timedelta(days=delta * sign))
                if cot is not None:
                    break
            if cot is not None:
                break
        if cot is None:
            continue

        # Weekly average price (obs_date ± 3 days)
        week_prices = [
            prices[obs_date + timedelta(days=d)]
            for d in range(-3, 4)
            if (obs_date + timedelta(days=d)) in prices
        ]
        if not week_prices:
            continue
        price = sum(week_prices) / len(week_prices)

        season_winter = 1.0 if obs_date.month in _WINTER_MONTHS else 0.0

        X_rows.append([deficit, hdd_7d, cot, season_winter])
        y_rows.append(price)

    return X_rows, y_rows, feature_names


# ---------------------------------------------------------------------------
# OLS fit
# ---------------------------------------------------------------------------

def _fit_ols(X_rows, y_rows, feature_names):
    try:
        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import r2_score
        import numpy as np
    except ImportError as exc:
        logger.error("sklearn / numpy required for refit: %s", exc)
        sys.exit(1)

    X = np.array(X_rows)
    y = np.array(y_rows)

    model = LinearRegression()
    model.fit(X, y)

    y_pred  = model.predict(X)
    r2      = float(r2_score(y, y_pred))
    resids  = y - y_pred
    sigma   = float(np.std(resids, ddof=len(feature_names) + 1))

    coefficients = {name: float(coef) for name, coef in zip(feature_names, model.coef_)}

    return {
        "version":        "1",
        "fitted_at":      datetime.now(timezone.utc).isoformat(),
        "n_obs":          len(y_rows),
        "r_squared":      round(r2, 4),
        "residual_sigma": round(sigma, 4),
        "intercept":      float(model.intercept_),
        "coefficients":   coefficients,
    }, resids


def _print_diagnostics(result: dict, resids) -> None:
    import numpy as np

    print("\n=== Fair Value OLS Regression ===")
    print(f"  n_obs:           {result['n_obs']}")
    print(f"  R-squared:       {result['r_squared']:.3f}")
    print(f"  Residual sigma:  {result['residual_sigma']:.3f} USD/MMBtu")
    print(f"  Intercept:       {result['intercept']:.4f}")
    print("\n  Coefficients:")
    for name, coef in result["coefficients"].items():
        print(f"    {name:<35} {coef:+.6f}")
    print(f"\n  Residual min/max:  {resids.min():.2f} / {resids.max():.2f}")
    print(f"  Mean absolute error: {float(np.mean(np.abs(resids))):.3f} USD/MMBtu")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--start",
        default="2010-01-01",
        help="Earliest date to include in training data (default: 2010-01-01)",
    )
    parser.add_argument(
        "--window-years",
        type=int,
        default=3,
        dest="window_years",
        help="Rolling window: only use data from the last N years (default: 3)",
    )
    args = parser.parse_args()

    logger.info("Connecting to DuckDB at %s", DB_PATH)
    conn = duckdb.connect(DB_PATH)

    logger.info("Assembling training data (start=%s, window=%d yr)...", args.start, args.window_years)
    X_rows, y_rows, feature_names = _build_training_set(conn, args.start, args.window_years)
    conn.close()

    if len(X_rows) < 52:
        logger.error(
            "Only %d training rows — need at least 52. "
            "Run backfill first: python -m scripts.backfill_history --section hdd",
            len(X_rows),
        )
        sys.exit(1)

    logger.info("Fitting OLS on %d weekly observations...", len(X_rows))
    result, resids = _fit_ols(X_rows, y_rows, feature_names)

    _print_diagnostics(result, resids)

    _COEFF_PATH.write_text(json.dumps(result, indent=2))
    logger.info("Coefficients written to %s", _COEFF_PATH)
    logger.info(
        "Restart the scheduler or run compute_fairvalue_features() to switch to OLS mode."
    )


if __name__ == "__main__":
    main()
