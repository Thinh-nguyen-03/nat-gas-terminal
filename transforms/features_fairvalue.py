import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from config.settings import DB_PATH

logger = logging.getLogger("transforms")

_WINTER_MONTHS = frozenset({10, 11, 12, 1, 2, 3})
_MIN_BIN_SAMPLES = 5
_MIN_HISTORY_WEEKS = 104      # 2 years before lookup table is reliable
_MAX_COEFF_AGE_DAYS = 120     # refit coefficients if older than this

_COEFF_PATH = Path(__file__).parent / "fairvalue_coefficients.json"

_UPSERT_SQL = """
    INSERT INTO features_daily
        (feature_date, feature_name, region, value,
         interpretation, confidence, computed_at)
    VALUES (?, ?, 'US', ?, ?, ?, ?)
    ON CONFLICT (feature_date, feature_name, region)
    DO UPDATE SET value = excluded.value,
                 interpretation = excluded.interpretation,
                 computed_at = excluded.computed_at
"""


def compute_fairvalue_features() -> None:
    conn = duckdb.connect(DB_PATH)
    today = date.today()
    now = datetime.now(timezone.utc).isoformat()
    try:
        _run(conn, today, now)
    finally:
        conn.close()


def _run(conn, today: date, now: str) -> None:
    current_deficit = _get_current_deficit(conn)
    current_price = _get_current_price(conn)
    if current_deficit is None or current_price is None:
        logger.info("[features_fairvalue] missing current deficit or price — skipping")
        return

    season = "winter" if today.month in _WINTER_MONTHS else "summer"

    coeffs = _load_coefficients()
    if coeffs is not None:
        _run_ols(conn, today, now, current_deficit, current_price, season, coeffs)
    else:
        _run_lookup(conn, today, now, current_deficit, current_price, season)


# ---------------------------------------------------------------------------
# OLS mode
# ---------------------------------------------------------------------------

def _run_ols(conn, today, now, deficit, price, season, coeffs):
    intercept = coeffs["intercept"]
    coef = coeffs["coefficients"]
    sigma = coeffs["residual_sigma"]

    features = {
        "storage_deficit_vs_5yr_bcf": deficit,
        "season_winter": 1.0 if season == "winter" else 0.0,
    }
    for fname in ("hdd_7d_weighted", "cot_mm_net_pct_oi"):
        row = conn.execute("""
            SELECT value FROM features_daily
            WHERE feature_name = ? AND region = 'US'
            ORDER BY feature_date DESC LIMIT 1
        """, [fname]).fetchone()
        if row and row[0] is not None:
            features[fname] = row[0]

    fv_mid = intercept + sum(coef.get(k, 0.0) * v for k, v in features.items())
    fv_low = fv_mid - 1.645 * sigma
    fv_high = fv_mid + 1.645 * sigma
    gap = price - fv_mid
    interp = _interpret_gap(gap)

    _write_features(conn, today, now, fv_mid, fv_low, fv_high, gap, interp, "high")
    logger.info(
        "[features_fairvalue] OLS fv=%.2f [%.2f-%.2f] price=%.2f gap=%+.2f (%s) R2=%.2f",
        fv_mid, fv_low, fv_high, price, gap, interp, coeffs.get("r_squared", 0),
    )


# ---------------------------------------------------------------------------
# Lookup table mode
# ---------------------------------------------------------------------------

def _run_lookup(conn, today, now, deficit, price, season):
    history = _build_history(conn)
    if len(history) < _MIN_HISTORY_WEEKS:
        logger.info(
            "[features_fairvalue] only %d weeks of history (need %d) — skipping",
            len(history), _MIN_HISTORY_WEEKS,
        )
        return

    deficits_sorted = sorted(r[1] for r in history)
    edges = [_percentile_sorted(deficits_sorted, p) for p in (20, 40, 60, 80)]

    bins: dict[str, list[float]] = {}
    for _d, hist_deficit, hist_season, hist_price in history:
        key = f"{hist_season}_{_quintile(hist_deficit, edges)}"
        bins.setdefault(key, []).append(hist_price)

    bin_key = f"{season}_{_quintile(deficit, edges)}"
    bin_prices = bins.get(bin_key, [])
    if len(bin_prices) < _MIN_BIN_SAMPLES:
        logger.info(
            "[features_fairvalue] bin %s has only %d samples — skipping",
            bin_key, len(bin_prices),
        )
        return

    bp = sorted(bin_prices)
    fv_low = _percentile_sorted(bp, 5)
    fv_mid = _percentile_sorted(bp, 50)
    fv_high = _percentile_sorted(bp, 95)
    gap = price - fv_mid
    interp = _interpret_gap(gap)

    _write_features(conn, today, now, fv_mid, fv_low, fv_high, gap, interp, "medium")
    logger.info(
        "[features_fairvalue] lookup fv=%.2f [%.2f-%.2f] price=%.2f gap=%+.2f (%s) bin=%s n=%d",
        fv_mid, fv_low, fv_high, price, gap, interp, bin_key, len(bin_prices),
    )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _write_features(conn, today, now, fv_mid, fv_low, fv_high, gap, interp, confidence):
    for name, value in (
        ("fairvalue_mid",  fv_mid),
        ("fairvalue_low",  fv_low),
        ("fairvalue_high", fv_high),
        ("fairvalue_gap",  gap),
    ):
        conn.execute(_UPSERT_SQL, [today, name, value, interp, confidence, now])


def _build_history(conn) -> list[tuple]:
    """
    Returns (obs_date, deficit_bcf, season, price) for each EIA storage week
    where both a 3+ year rolling avg and a price are available.
    """
    storage_rows = conn.execute("""
        SELECT observation_time::DATE AS d, value
        FROM facts_time_series
        WHERE source_name = 'eia_storage' AND series_name = 'storage_total'
        ORDER BY d
    """).fetchall()
    if not storage_rows:
        return []

    storage = {r[0]: r[1] for r in storage_rows}
    all_dates = sorted(storage.keys())

    price_rows = conn.execute("""
        SELECT observation_time::DATE AS d, value, source_name
        FROM facts_time_series
        WHERE (source_name = 'fred'     AND series_name = 'ng_spot_price')
           OR (source_name = 'yfinance' AND series_name = 'ng_front_close')
        ORDER BY d
    """).fetchall()

    prices: dict = {}
    for d, v, src in price_rows:
        if d not in prices or src == "fred":
            prices[d] = v

    result = []
    for obs_date in all_dates:
        val = storage[obs_date]
        iso_week = obs_date.isocalendar()[1]

        comparables = [
            storage[d] for d in all_dates
            if 1 <= obs_date.year - d.year <= 5
            and d.isocalendar()[1] == iso_week
        ]
        if len(comparables) < 3:
            continue

        deficit = val - sum(comparables) / len(comparables)

        price = None
        for delta in range(6):
            for sign in (0, 1, -1):
                candidate = obs_date + timedelta(days=delta * sign)
                if candidate in prices:
                    price = prices[candidate]
                    break
            if price is not None:
                break
        if price is None:
            continue

        hist_season = "winter" if obs_date.month in _WINTER_MONTHS else "summer"
        result.append((obs_date, deficit, hist_season, price))

    return result


def _get_current_deficit(conn) -> float | None:
    row = conn.execute("""
        SELECT value FROM features_daily
        WHERE feature_name = 'storage_deficit_vs_5yr_bcf' AND region = 'US'
        ORDER BY feature_date DESC LIMIT 1
    """).fetchone()
    return row[0] if row else None


def _get_current_price(conn) -> float | None:
    row = conn.execute("""
        SELECT value FROM facts_time_series
        WHERE (source_name = 'fred'     AND series_name = 'ng_spot_price')
           OR (source_name = 'yfinance' AND series_name = 'ng_front_close')
        ORDER BY observation_time DESC LIMIT 1
    """).fetchone()
    return row[0] if row else None


def _load_coefficients() -> dict | None:
    if not _COEFF_PATH.exists():
        return None
    try:
        with open(_COEFF_PATH) as f:
            data = json.load(f)
        fitted_at = datetime.fromisoformat(data["fitted_at"])
        age_days = (datetime.now(timezone.utc) - fitted_at).days
        if age_days > _MAX_COEFF_AGE_DAYS:
            logger.info(
                "[features_fairvalue] coefficients are %d days old (max %d) — using lookup table",
                age_days, _MAX_COEFF_AGE_DAYS,
            )
            return None
        return data
    except Exception as exc:
        logger.warning("[features_fairvalue] could not load coefficients: %s", exc)
        return None


def _percentile_sorted(sorted_vals: list, p: float) -> float:
    n = len(sorted_vals)
    if n == 1:
        return float(sorted_vals[0])
    idx = (p / 100.0) * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    return sorted_vals[lo] * (1 - (idx - lo)) + sorted_vals[hi] * (idx - lo)


def _quintile(value: float, edges: list) -> int:
    for i, edge in enumerate(edges):
        if value < edge:
            return i
    return 4


def _interpret_gap(gap: float) -> str:
    """
    gap = current_price - fairvalue_mid
    Negative means market is below fair value (bullish signal).
    Positive means market is above fair value (bearish signal).
    """
    if gap < -0.50:
        return "bullish_mispricing"
    if gap < -0.20:
        return "mildly_bullish"
    if gap <= 0.20:
        return "fairly_priced"
    if gap <= 0.50:
        return "mildly_bearish"
    return "bearish_mispricing"
