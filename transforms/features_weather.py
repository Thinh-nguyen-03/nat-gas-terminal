import json
import logging
import os
from datetime import date, datetime, timedelta, timezone

import duckdb

from collectors.weather import WEATHER_POINTS
from config.settings import ARCHIVE_DIR, DB_PATH
from transforms.demand_coefficients import estimate_demand, seasonal_normal_demand

logger = logging.getLogger("collectors")

# Pop-weighted HDD thresholds for interpretation (seasonal; Phase 2 will make dynamic)
_HDD_BULLISH       = 80.0
_HDD_MILD_BULLISH  = 50.0
_HDD_NEUTRAL       = 20.0
_HDD_MILD_BEARISH  = 10.0

_REVISION_BULLISH      = 5.0
_REVISION_MILD_BULLISH = 1.0
_REVISION_MILD_BEARISH = -1.0
_REVISION_BEARISH      = -5.0

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


def compute_weather_features() -> None:
    conn = duckdb.connect(DB_PATH)
    today = date.today()
    now = datetime.now(timezone.utc).isoformat()

    try:
        hdd_7d = _compute_7d_hdd(conn, today, now)
        _compute_revision_delta(conn, today, now)
        if hdd_7d is not None:
            _compute_demand_estimate(conn, today, now, hdd_7d)
    finally:
        conn.close()


def _compute_7d_hdd(conn, today: date, now: str) -> float | None:
    row = conn.execute("""
        SELECT SUM(value)
        FROM facts_time_series
        WHERE source_name = 'nws'
          AND series_name = 'forecast_hdd_wtd'
          AND observation_time BETWEEN NOW() AND NOW() + INTERVAL '7 days'
    """).fetchone()

    total_hdd = row[0] if row and row[0] is not None else None
    if total_hdd is None:
        return None

    conn.execute(_UPSERT_SQL, [
        today, "weather_hdd_7d_weighted", total_hdd,
        _interpret_hdd(total_hdd), "high", now,
    ])
    return total_hdd


def _compute_revision_delta(conn, today: date, now: str) -> None:
    yesterday = today - timedelta(days=1)

    today_hdd     = _load_archive_hdd(today)
    yesterday_hdd = _load_archive_hdd(yesterday)

    if not today_hdd or not yesterday_hdd:
        return

    revision = sum(
        (today_hdd.get(city, 0) - yesterday_hdd.get(city, 0)) * coords["pop_weight"]
        for city, coords in WEATHER_POINTS.items()
        if city in today_hdd and city in yesterday_hdd
    )

    conn.execute(_UPSERT_SQL, [
        today, "weather_hdd_revision_delta", revision,
        _interpret_revision(revision), "high", now,
    ])


def _load_archive_hdd(for_date: date) -> dict[str, float]:
    """Return city -> unweighted 7-day HDD sum from the daily forecast archive."""
    archive_dir = os.path.join(ARCHIVE_DIR, str(for_date))
    if not os.path.exists(archive_dir):
        return {}

    result: dict[str, float] = {}
    for fname in os.listdir(archive_dir):
        if not fname.endswith("_forecast.json"):
            continue
        city = fname.replace("_forecast.json", "")
        try:
            with open(os.path.join(archive_dir, fname)) as f:
                data = json.load(f)
            periods = data.get("properties", {}).get("periods", [])
            city_hdd = sum(
                max(0.0, 65.0 - p["temperature"])
                for p in periods
                if p.get("isDaytime")
            )
            result[city] = city_hdd
        except Exception as e:
            logger.warning("[features_weather] could not read archive %s: %s", fname, e)

    return result


def _interpret_hdd(hdd: float) -> str:
    if hdd > _HDD_BULLISH:
        return "bullish"
    if hdd > _HDD_MILD_BULLISH:
        return "mildly_bullish"
    if hdd > _HDD_NEUTRAL:
        return "neutral"
    if hdd > _HDD_MILD_BEARISH:
        return "mildly_bearish"
    return "bearish"


def _interpret_revision(revision: float) -> str:
    if revision > _REVISION_BULLISH:
        return "bullish"
    if revision > _REVISION_MILD_BULLISH:
        return "mildly_bullish"
    if revision < _REVISION_BEARISH:
        return "bearish"
    if revision < _REVISION_MILD_BEARISH:
        return "mildly_bearish"
    return "neutral"


def _compute_demand_estimate(conn, today: date, now: str, hdd_7d: float) -> None:
    """
    Translate the 7-day weighted HDD into an estimated residential/commercial
    gas demand (Bcf/d) using seasonal regression coefficients.

    Writes two features to features_daily:
      - weather_implied_resi_comm_bcfd  — the demand estimate
      - weather_demand_vs_normal_bcfd   — delta above/below seasonal normal
    """
    demand = estimate_demand(hdd_7d, today.month)
    normal = seasonal_normal_demand(today.month)
    delta  = demand - normal

    conn.execute(_UPSERT_SQL, [
        today, "weather_implied_resi_comm_bcfd", demand,
        _interpret_demand_delta(delta), "medium", now,
    ])
    conn.execute(_UPSERT_SQL, [
        today, "weather_demand_vs_normal_bcfd", delta,
        _interpret_demand_delta(delta), "medium", now,
    ])


def _interpret_demand_delta(delta: float) -> str:
    """Interpret demand deviation from seasonal normal."""
    if delta > 5.0:
        return "bullish"
    if delta > 2.0:
        return "mildly_bullish"
    if delta < -5.0:
        return "bearish"
    if delta < -2.0:
        return "mildly_bearish"
    return "neutral"
