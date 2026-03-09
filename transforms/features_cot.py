import logging
from datetime import date, datetime, timezone

import duckdb

from config.settings import DB_PATH

logger = logging.getLogger("collectors")

# COT interpretation is contrarian:
#   Crowded long  (>+20% OI) -> unwind risk on bearish catalyst  -> bearish signal
#   Crowded short (<-20% OI) -> squeeze risk on bullish catalyst -> bullish signal
_CROWDED_LONG_THRESHOLD   =  20.0
_MILD_LONG_THRESHOLD      =  10.0
_MILD_SHORT_THRESHOLD     = -10.0
_CROWDED_SHORT_THRESHOLD  = -20.0

_UPSERT_SQL = """
    INSERT INTO features_daily
        (feature_date, feature_name, region, value,
         interpretation, confidence, computed_at)
    VALUES (?, ?, 'US', ?, ?, 'high', ?)
    ON CONFLICT (feature_date, feature_name, region)
    DO UPDATE SET value = excluded.value,
                 interpretation = excluded.interpretation,
                 computed_at = excluded.computed_at
"""


def compute_cot_features() -> None:
    conn = duckdb.connect(DB_PATH)
    today = date.today()
    now = datetime.now(timezone.utc).isoformat()

    try:
        _compute_and_write(conn, today, now)
    finally:
        conn.close()


def _compute_and_write(conn, today: date, now: str) -> None:
    rows = conn.execute("""
        SELECT observation_time::DATE AS obs_date, series_name, value
        FROM facts_time_series
        WHERE source_name = 'cftc'
          AND series_name IN ('cot_mm_long', 'cot_mm_short', 'cot_open_interest')
        ORDER BY obs_date DESC
        LIMIT 6
    """).fetchall()

    if not rows:
        return

    latest_date = rows[0][0]
    latest = {r[1]: r[2] for r in rows if r[0] == latest_date}
    prior_dates = sorted({r[0] for r in rows if r[0] != latest_date}, reverse=True)
    prior_date  = prior_dates[0] if prior_dates else None
    prior = {r[1]: r[2] for r in rows if r[0] == prior_date} if prior_date else {}

    mm_long  = latest.get("cot_mm_long")
    mm_short = latest.get("cot_mm_short")
    oi       = latest.get("cot_open_interest")

    if mm_long is None or mm_short is None:
        return

    mm_net     = mm_long - mm_short
    mm_net_pct = (mm_net / oi * 100) if oi else None

    prior_long  = prior.get("cot_mm_long")
    prior_short = prior.get("cot_mm_short")
    mm_net_wow  = (
        mm_net - (prior_long - prior_short)
        if (prior_long is not None and prior_short is not None) else None
    )

    features = [
        ("cot_mm_net_contracts", mm_net,      "neutral"),
        ("cot_mm_net_pct_oi",   mm_net_pct,  _interpret_cot(mm_net_pct)),
        ("cot_mm_net_wow",      mm_net_wow,  "neutral"),
        ("cot_open_interest",   oi,           "neutral"),
    ]
    for name, value, interp in features:
        if value is None:
            continue
        conn.execute(_UPSERT_SQL, [today, name, value, interp, now])


def _interpret_cot(pct: float | None) -> str:
    """Contrarian: crowded shorts are bullish (squeeze risk), crowded longs are bearish."""
    if pct is None:
        return "unknown"
    if pct < _CROWDED_SHORT_THRESHOLD:
        return "bullish"
    if pct < _MILD_SHORT_THRESHOLD:
        return "mildly_bullish"
    if pct < _MILD_LONG_THRESHOLD:
        return "neutral"
    if pct < _CROWDED_LONG_THRESHOLD:
        return "mildly_bearish"
    return "bearish"
