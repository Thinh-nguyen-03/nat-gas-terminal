"""
Power demand feature transform (Feature 3).

Reads recent ISO LMP data from facts_time_series (source='iso_lmp') and
computes per-ISO stress z-scores plus a composite demand signal index.

Writes to:
  features_daily:
    lmp_stress_score          region=ISO_NAME  value=z-score (trailing 30d)
    power_demand_stress_index region='US'      value=0-100 composite index
  features_intraday:
    power_demand_stress_index region='US'      value=0-100 composite index

The 0-100 composite index is the average z-score across all ISOs with live
data, linearly scaled so z=-2 → 0, z=0 → 50, z=+2 → 100 and clipped.

Runs every hour at :12 (after the LMP collector at :10).
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timezone

import duckdb

from config.settings import DB_PATH

logger = logging.getLogger("collectors")

# Known ISOs — match the region values written by ISOLMPCollector.
_ISOS = ["NYISO", "MISO", "CAISO"]

# How many days of history to use for trailing mean/std.
_LOOKBACK_DAYS = 30

# Minimum observations required in the lookback window before computing z-score.
_MIN_OBS = 5

_UPSERT_DAILY = """
    INSERT INTO features_daily
        (feature_date, feature_name, region, value,
         interpretation, confidence, computed_at)
    VALUES (?, ?, ?, ?, ?, 'medium', ?)
    ON CONFLICT (feature_date, feature_name, region)
    DO UPDATE SET value = excluded.value,
                 interpretation = excluded.interpretation,
                 computed_at = excluded.computed_at
"""

_UPSERT_INTRADAY = """
    INSERT INTO features_intraday (ts, feature_name, region, value)
    VALUES (?, 'power_demand_stress_index', 'US', ?)
    ON CONFLICT (ts, feature_name, region)
    DO UPDATE SET value = excluded.value
"""


def compute_power_demand_features() -> None:
    conn = duckdb.connect(DB_PATH)
    today = date.today()
    now = datetime.now(timezone.utc)
    now_str = now.isoformat()

    try:
        _run(conn, today, now_str, now)
    finally:
        conn.close()


def _run(conn, today: date, now_str: str, now: datetime) -> None:
    z_scores: list[float] = []

    for iso in _ISOS:
        z = _compute_iso_zscore(conn, iso, today, now_str)
        if z is not None:
            z_scores.append(z)

    if not z_scores:
        logger.info("[power_demand] no ISO data available — skipping")
        return

    # Composite 0-100 index: avg(z), linearly mapped z=-2→0, z=+2→100, clipped.
    avg_z = sum(z_scores) / len(z_scores)
    index = max(0.0, min(100.0, (avg_z + 2.0) / 4.0 * 100.0))

    interp = _interpret_index(index)
    conn.execute(_UPSERT_DAILY, [today, "power_demand_stress_index", "US", index, interp, now_str])
    conn.execute(_UPSERT_INTRADAY, [now_str, index])

    logger.info(
        "[power_demand] stress_index=%.1f (%s) from %d ISOs (avg_z=%.2f)",
        index, interp, len(z_scores), avg_z,
    )


def _compute_iso_zscore(conn, iso: str, today: date, now_str: str) -> float | None:
    """Compute trailing z-score for an ISO's hub LMP and write to features_daily."""

    # All hub LMP values for this ISO over the lookback window.
    rows = conn.execute("""
        SELECT value
        FROM facts_time_series
        WHERE source_name = 'iso_lmp'
          AND series_name  = 'lmp_hub'
          AND region       = ?
          AND observation_time >= NOW() - INTERVAL 30 DAYS
          AND value IS NOT NULL
        ORDER BY observation_time DESC
    """, [iso]).fetchall()

    if len(rows) < _MIN_OBS:
        return None

    values = [r[0] for r in rows]
    current = values[0]  # most recent

    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(variance) if variance > 0 else 0.0

    z = (current - mean) / std if std > 0 else 0.0

    interp = _classify_z(z)
    conn.execute(_UPSERT_DAILY, [today, "lmp_stress_score", iso, z, interp, now_str])

    logger.info("[power_demand] %s: lmp=%.2f mean=%.2f std=%.2f z=%.2f (%s)",
                iso, current, mean, std, z, interp)
    return z


def _classify_z(z: float) -> str:
    if z > 2.0:
        return "high"
    if z > 0.5:
        return "elevated"
    if z < -1.0:
        return "suppressed"
    return "normal"


def _interpret_index(index: float) -> str:
    if index >= 75:
        return "high_demand"
    if index >= 55:
        return "elevated_demand"
    if index <= 25:
        return "suppressed_demand"
    return "normal_demand"
