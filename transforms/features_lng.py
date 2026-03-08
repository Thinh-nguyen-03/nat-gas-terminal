"""
LNG export feature transform (Feature 2).

Reads the latest vessel counts per terminal from facts_time_series (source='ais')
and computes:
  lng_implied_exports_bcfd       — sum of capacity for terminals with loading ships
  lng_terminal_utilization_pct   — implied_exports / total_capacity × 100

Writes both to features_daily (region='US').

The implied export rate is a coarse proxy: if at least one ship is loading at
a terminal, that terminal's full nameplate capacity is counted. This matches
how traders interpret AIS berth occupancy data.

Runs every 30 minutes at :15 and :45 (after the AIS collector).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import duckdb

from config.settings import DB_PATH

logger = logging.getLogger("collectors")

# Nameplate capacity by terminal name (Bcf/d) — must match lng.go knownTerminals.
_TERMINAL_CAPACITIES: dict[str, float] = {
    "Sabine Pass":    5.00,
    "Corpus Christi": 2.40,
    "Freeport LNG":   2.40,
    "Cameron LNG":    2.10,
    "Calcasieu Pass": 1.40,
    "Cove Point":     0.75,
    "Elba Island":    0.35,
}

_TOTAL_CAPACITY = sum(_TERMINAL_CAPACITIES.values())

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


def compute_lng_features() -> None:
    conn = duckdb.connect(DB_PATH)
    today = date.today()
    now_str = datetime.now(timezone.utc).isoformat()

    try:
        _run(conn, today, now_str)
    finally:
        conn.close()


def _run(conn, today: date, now_str: str) -> None:
    # Read the latest vessel counts per terminal (within last 2 hours).
    rows = conn.execute("""
        SELECT region, series_name, value
        FROM (
            SELECT region, series_name, value,
                   ROW_NUMBER() OVER (PARTITION BY region, series_name
                                      ORDER BY observation_time DESC) AS rn
            FROM facts_time_series
            WHERE source_name = 'ais'
              AND series_name IN ('lng_ships_loading', 'lng_ships_anchored')
              AND observation_time >= NOW() - INTERVAL 2 HOURS
        ) ranked
        WHERE rn = 1
    """).fetchall()

    if not rows:
        logger.info("[features_lng] no AIS data in last 2 hours — skipping")
        return

    # Build per-terminal ship count dict.
    loading_counts: dict[str, int] = {}
    for terminal, series, val in rows:
        if series == "lng_ships_loading" and val is not None:
            loading_counts[terminal] = int(val)

    # Implied exports: sum capacity of terminals with ≥1 loading ship.
    implied = sum(
        _TERMINAL_CAPACITIES[t]
        for t, cnt in loading_counts.items()
        if cnt > 0 and t in _TERMINAL_CAPACITIES
    )
    utilization = (implied / _TOTAL_CAPACITY * 100) if _TOTAL_CAPACITY > 0 else 0.0

    interp = _interpret_utilization(utilization)

    conn.execute(_UPSERT_SQL, [today, "lng_implied_exports_bcfd", implied, interp, now_str])
    conn.execute(_UPSERT_SQL, [today, "lng_terminal_utilization_pct", utilization, interp, now_str])

    logger.info(
        "[features_lng] implied_exports=%.2f Bcfd utilization=%.1f%% (%s)",
        implied, utilization, interp,
    )


def _interpret_utilization(pct: float) -> str:
    if pct >= 85:
        return "high_exports"
    if pct >= 60:
        return "moderate_exports"
    if pct >= 30:
        return "low_exports"
    return "minimal_exports"
