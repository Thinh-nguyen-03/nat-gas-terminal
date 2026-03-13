import logging
from datetime import date, datetime, timezone

import duckdb

from config.settings import DB_PATH, connect_db

logger = logging.getLogger("collectors")

# Season boundaries and comfortable end-of-season inventory ranges (Bcf)
_WITHDRAWAL_SEASON_MONTHS = {11, 12, 1, 2, 3}
_WITHDRAWAL_EOS_LOW  = 1700.0
_WITHDRAWAL_EOS_HIGH = 2000.0
_INJECTION_EOS_LOW   = 3500.0
_INJECTION_EOS_HIGH  = 3800.0

# Weeks of history used to estimate the rolling withdrawal/injection pace
_PACE_LOOKBACK_WEEKS = 4

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


def compute_storage_features() -> None:
    conn = connect_db()
    today = date.today()
    now = datetime.now(timezone.utc).isoformat()

    rows = conn.execute("""
        SELECT observation_time::DATE AS obs_date, value
        FROM facts_time_series
        WHERE source_name = 'eia_storage'
          AND series_name = 'storage_total'
        ORDER BY obs_date DESC
        LIMIT 104
    """).fetchall()

    if not rows:
        logger.info("[features_storage] no storage data available — skipping")
        conn.close()
        return

    try:
        _compute_and_write(conn, rows, today, now)
        _compute_storage_surprise(conn, today, now)
    finally:
        conn.close()


def _compute_and_write(conn, rows: list, today: date, now: str) -> None:
    latest_date, latest_val = rows[0]
    prior_val = rows[1][1] if len(rows) > 1 else None

    year_ago_val = _find_year_ago(rows, latest_date)
    five_yr, five_yr_confidence = _lookup_five_year_avg(conn, latest_date)

    deficit_vs_5yr = (latest_val - five_yr) if five_yr is not None else None
    deficit_vs_py  = (latest_val - year_ago_val) if year_ago_val is not None else None
    wow_change     = (latest_val - prior_val) if prior_val is not None else None

    projected_eos, weeks_remaining, avg_weekly_pace, eos_interp = _project_eos(
        latest_val, latest_date, rows, today
    )

    features = [
        ("storage_total_bcf",             latest_val,    "neutral",                      "high"),
        ("storage_deficit_vs_5yr_bcf",    deficit_vs_5yr, _interpret_deficit(deficit_vs_5yr), five_yr_confidence),
        ("storage_deficit_vs_py_bcf",     deficit_vs_py,  _interpret_deficit(deficit_vs_py),  "high"),
        ("storage_wow_change_bcf",        wow_change,    "neutral",                      "high"),
        ("storage_eos_projection_bcf",    projected_eos, eos_interp,                     "medium"),
        ("storage_eos_deficit_vs_norm_bcf",
            (_eos_comfortable_mid(today) - projected_eos) if projected_eos is not None else None,
            eos_interp, "medium"),
        ("storage_weeks_remaining",       float(weeks_remaining), "neutral",             "high"),
        ("storage_avg_weekly_pace_bcf",   avg_weekly_pace, "neutral",                   "high"),
    ]

    for name, value, interp, confidence in features:
        conn.execute(_UPSERT_SQL, [today, name, value, interp, confidence, now])


def _find_year_ago(rows: list, latest_date: date):
    latest_week = latest_date.isocalendar()[1]
    for obs_date, val in rows:
        if (obs_date.year == latest_date.year - 1
                and obs_date.isocalendar()[1] == latest_week):
            return val
    return None


def _lookup_five_year_avg(conn, latest_date: date) -> tuple:
    """Look up the EIA official 5-year average for the week matching latest_date.

    Queries eia_storage_stats (populated by EIAStorageStatsCollector from ngsstats.xls).
    Matches on ISO week number so the correct band row is returned regardless of year.
    """
    row = conn.execute("""
        SELECT value
        FROM facts_time_series
        WHERE source_name = 'eia_storage_stats'
          AND series_name = 'storage_5yr_avg_total'
          AND EXTRACT(week FROM observation_time::DATE) = ?
        ORDER BY observation_time DESC
        LIMIT 1
    """, [latest_date.isocalendar()[1]]).fetchone()
    if row is None:
        return None, "low"
    return row[0], "high"


def _project_eos(
    latest_val: float,
    latest_date: date,
    rows: list,
    today: date,
) -> tuple:
    month = today.month
    if month in _WITHDRAWAL_SEASON_MONTHS:
        season_end = (
            date(today.year + 1, 3, 31) if month >= 11
            else date(today.year, 3, 31)
        )
    else:
        season_end = date(today.year, 10, 31)

    weeks_remaining = max(0, (season_end - latest_date).days // 7)

    weekly_changes = [
        rows[i][1] - rows[i + 1][1]
        for i in range(min(_PACE_LOOKBACK_WEEKS, len(rows) - 1))
    ]
    avg_weekly_pace = (
        sum(weekly_changes) / len(weekly_changes) if weekly_changes else None
    )
    projected_eos = (
        latest_val + avg_weekly_pace * weeks_remaining
        if avg_weekly_pace is not None else None
    )
    eos_interp = _interpret_eos(projected_eos, today)
    return projected_eos, weeks_remaining, avg_weekly_pace, eos_interp


def _eos_comfortable_mid(today: date) -> float:
    if today.month in _WITHDRAWAL_SEASON_MONTHS:
        return (_WITHDRAWAL_EOS_LOW + _WITHDRAWAL_EOS_HIGH) / 2
    return (_INJECTION_EOS_LOW + _INJECTION_EOS_HIGH) / 2


def _interpret_deficit(deficit) -> str:
    if deficit is None:
        return "unknown"
    if deficit < -200:
        return "very_bullish"
    if deficit < -100:
        return "bullish"
    if deficit < 0:
        return "mildly_bullish"
    if deficit < 100:
        return "mildly_bearish"
    return "bearish"


def _compute_storage_surprise(conn, today: date, now: str) -> None:
    """
    Compute the EIA storage surprise (actual WoW change minus consensus).

    Requires a consensus entry in consensus_inputs with
    input_type='eia_storage_consensus' for the current report week.
    Silently skips if no consensus is available — this is expected until
    collectors/storage_consensus.py (Feature 8 full impl.) is running.

    Sign convention:
      positive surprise → drew/injected MORE than consensus → bullish
      (e.g. actual=-90, consensus=-80 → surprise=-10, which is a bigger
       draw than expected, so bearish for storage but bullish for price)

    Writes to features_daily:
      storage_consensus_bcf      — the consensus estimate
      storage_eia_surprise_bcf   — actual minus consensus
    """
    # Most recent actual WoW change (written by _compute_and_write above)
    actual_row = conn.execute("""
        SELECT value FROM features_daily
        WHERE feature_name = 'storage_wow_change_bcf' AND region = 'US'
        ORDER BY feature_date DESC
        LIMIT 1
    """).fetchone()
    if not actual_row or actual_row[0] is None:
        return

    # Most recent consensus estimate
    consensus_row = conn.execute("""
        SELECT value FROM consensus_inputs
        WHERE input_type = 'eia_storage_consensus'
        ORDER BY input_date DESC
        LIMIT 1
    """).fetchone()
    if not consensus_row or consensus_row[0] is None:
        return

    actual    = actual_row[0]
    consensus = consensus_row[0]
    surprise  = actual - consensus

    conn.execute(_UPSERT_SQL, [
        today, "storage_consensus_bcf", consensus,
        "neutral", "medium", now,
    ])
    conn.execute(_UPSERT_SQL, [
        today, "storage_eia_surprise_bcf", surprise,
        _interpret_surprise(surprise), "high", now,
    ])


def _interpret_surprise(surprise: float) -> str:
    """
    Negative surprise = drew/injected more than consensus (bullish for price).
    Positive surprise = drew/injected less than consensus (bearish for price).
    """
    if surprise < -10:
        return "very_bullish"
    if surprise < -3:
        return "bullish"
    if surprise > 10:
        return "very_bearish"
    if surprise > 3:
        return "bearish"
    return "neutral"


def _interpret_eos(projected_eos, today: date) -> str:
    if projected_eos is None:
        return "unknown"
    month = today.month
    if month in _WITHDRAWAL_SEASON_MONTHS:
        low, high = _WITHDRAWAL_EOS_LOW, _WITHDRAWAL_EOS_HIGH
    else:
        low, high = _INJECTION_EOS_LOW, _INJECTION_EOS_HIGH
    mid = (low + high) / 2
    if projected_eos < low:
        return "very_bullish"
    if projected_eos < mid:
        return "bullish"
    if projected_eos > high:
        return "bearish"
    return "neutral"
