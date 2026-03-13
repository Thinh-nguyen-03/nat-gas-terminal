import logging
from datetime import date, datetime, timezone

import duckdb

from collectors.weather import WEATHER_POINTS
from config.settings import DB_PATH, connect_db

logger = logging.getLogger("collectors")

# Interpretation thresholds for population-weighted prob_below (0–100 scale)
_BULLISH       = 50.0   # weighted avg above 50% = meaningful cold signal
_MILD_BULLISH  = 40.0
_MILD_BEARISH  = 28.0   # below EC (33.33%) = net warm lean
_BEARISH       = 20.0

_UPSERT_SQL = """
    INSERT INTO features_daily
        (feature_date, feature_name, region, value,
         interpretation, confidence, computed_at)
    VALUES (?, ?, 'US', ?, ?, 'medium', ?)
    ON CONFLICT (feature_date, feature_name, region)
    DO UPDATE SET value = excluded.value,
                 interpretation = excluded.interpretation,
                 computed_at = excluded.computed_at
"""


def compute_cpc_features() -> None:
    conn = connect_db()
    today = date.today()
    now = datetime.now(timezone.utc).isoformat()

    try:
        for window in ("6_10", "8_14"):
            _compute_window(conn, today, now, window)
    finally:
        conn.close()


def _compute_window(conn, today: date, now: str, window: str) -> None:
    """Compute population-weighted prob_below for one CPC outlook window."""
    # Fetch the most recent observation for each city for this window.
    rows = conn.execute("""
        SELECT region, value
        FROM (
            SELECT region, value,
                   ROW_NUMBER() OVER (PARTITION BY region ORDER BY observation_time DESC) AS rn
            FROM facts_time_series
            WHERE source_name = 'cpc'
              AND series_name  = ?
        ) ranked
        WHERE rn = 1
    """, [f"cpc_{window}_prob_below"]).fetchall()

    if not rows:
        return

    city_probs = {region: value for region, value in rows}

    # Population-weighted average prob_below across cities with data
    weighted_sum = 0.0
    weight_total = 0.0
    for city, coords in WEATHER_POINTS.items():
        if city in city_probs:
            weighted_sum  += city_probs[city] * coords["pop_weight"]
            weight_total  += coords["pop_weight"]

    if weight_total == 0:
        return

    weighted_prob = weighted_sum / weight_total

    conn.execute(_UPSERT_SQL, [
        today,
        f"cpc_{window}_weighted_prob_below",
        weighted_prob,
        _interpret(weighted_prob),
        now,
    ])


def _interpret(prob_below: float) -> str:
    if prob_below >= _BULLISH:
        return "bullish"
    if prob_below >= _MILD_BULLISH:
        return "mildly_bullish"
    if prob_below <= _BEARISH:
        return "bearish"
    if prob_below <= _MILD_BEARISH:
        return "mildly_bearish"
    return "neutral"
