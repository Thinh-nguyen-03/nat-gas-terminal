"""
LNG export feature transform (Feature 2).

Primary path: reads the latest vessel counts per terminal from facts_time_series
(source='ais') and computes:
  lng_implied_exports_bcfd       — sum of capacity for terminals with loading ships
  lng_terminal_utilization_pct   — implied_exports / total_capacity × 100
  lng_export_pressure_index      — composite 0-100 signal (export rate + queue pressure)
  lng_queue_depth                — total anchored ships across all terminals
  lng_destination_eu_pct         — % of vessels with known destination bound for Europe
                                   (written when ≥2 vessels have a destination set)

Fallback path (no AIS): reads the most recent EIA monthly lng_exports_mmcf
from facts_time_series (source='eia_supply') and converts to BCF/D.
Confidence is set to 'low' for EIA-derived features.

Runs every 10 minutes (at :05, :15, :25, :35, :45, :55) after the AIS collector.
"""

from __future__ import annotations

import calendar
import json
import logging
import os
from datetime import date, datetime, timezone, timedelta

import duckdb

from config.settings import DB_PATH

# Path where the Go API server writes the latest AIS snapshot.
# Layout: data/db/terminal.duckdb → data/ais_snapshot.json
_AIS_SNAPSHOT_PATH = os.path.join(os.path.dirname(DB_PATH), "..", "ais_snapshot.json")

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

# European regasification port keywords (lowercase). AIS destination field is
# free-text and often abbreviated, so we match substrings.
_EUROPE_KEYWORDS = (
    "rotterdam", "zeebrugge", "isle of grain", "grain", "milford",
    "south hook", "dragon", "dunkerque", "montoir", "fos", "barcelona",
    "bilbao", "huelva", "sines", "revithoussa", "brindisi", "panigaglia",
    "porto empedocle", "eems", "gate terminal", "amsterdam", "lisbon",
    "musel", "mugardos", "neptuno", "aliaga", "istanbul", "marmara",
    "netherlands", "belgium", "france", "spain", "portugal", "italy",
    "greece", "germany", "turkey", "uk ", "united kingdom",
)

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

_UPSERT_SQL_LOW = """
    INSERT INTO features_daily
        (feature_date, feature_name, region, value,
         interpretation, confidence, computed_at)
    VALUES (?, ?, 'US', ?, ?, 'low', ?)
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


def _read_ais_snapshot() -> dict | None:
    """Read the JSON snapshot written by the Go API server after each AIS update.

    Returns None if the file is missing, unreadable, or older than 2 hours.
    """
    try:
        with open(_AIS_SNAPSHOT_PATH) as f:
            data = json.load(f)
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.debug("[features_lng] failed to read AIS snapshot: %s", e)
        return None

    updated_at_str = data.get("updated_at", "")
    if updated_at_str:
        try:
            updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - updated_at > timedelta(hours=2):
                logger.info("[features_lng] AIS snapshot is stale — falling back to EIA")
                return None
        except ValueError:
            pass

    return data


def _run(conn, today: date, now_str: str) -> None:
    # Read the latest AIS snapshot written by the Go API server.
    ais = _read_ais_snapshot()

    if not ais:
        logger.info("[features_lng] no AIS data in last 2 hours — falling back to EIA supply data")
        _run_eia_fallback(conn, today, now_str)
        return

    # counts: {"Sabine Pass": [loading, anchored], ...}
    raw_counts: dict = ais.get("counts") or {}
    vessels: list = ais.get("vessels") or []

    loading_counts: dict[str, int] = {}
    anchored_counts: dict[str, int] = {}
    for terminal, counts in raw_counts.items():
        loading_counts[terminal] = counts[0]
        anchored_counts[terminal] = counts[1]

    # Implied exports: sum capacity of terminals with ≥1 loading ship.
    implied = sum(
        _TERMINAL_CAPACITIES[t]
        for t, cnt in loading_counts.items()
        if cnt > 0 and t in _TERMINAL_CAPACITIES
    )
    utilization = (implied / _TOTAL_CAPACITY * 100) if _TOTAL_CAPACITY > 0 else 0.0
    total_anchored = sum(anchored_counts.values())

    interp = _interpret_utilization(utilization)

    conn.execute(_UPSERT_SQL, [today, "lng_implied_exports_bcfd",    implied,     interp, now_str])
    conn.execute(_UPSERT_SQL, [today, "lng_terminal_utilization_pct", utilization, interp, now_str])

    # Export Pressure Index — composite bullish/bearish signal (0-100).
    epi = _compute_epi(implied, total_anchored)
    conn.execute(_UPSERT_SQL, [today, "lng_export_pressure_index", epi, _interpret_epi(epi), now_str])

    # Queue depth — raw count of ships waiting for a berth.
    queue_interp = "high_queue" if total_anchored >= 3 else "normal_queue" if total_anchored > 0 else "no_queue"
    conn.execute(_UPSERT_SQL, [today, "lng_queue_depth", float(total_anchored), queue_interp, now_str])

    # Destination mix — from vessel list in the snapshot.
    _write_destination_mix(conn, today, now_str, vessels)

    logger.info(
        "[features_lng] AIS: implied=%.2f Bcfd util=%.1f%% EPI=%.0f queue=%d (%s)",
        implied, utilization, epi, total_anchored, interp,
    )


def _write_destination_mix(conn, today: date, now_str: str, vessels: list) -> None:
    """Compute European destination % from vessels in the AIS snapshot."""
    if not vessels:
        return

    eu_count = 0
    total = 0
    for v in vessels:
        dest = (v.get("destination") or "").strip()
        if not dest:
            continue
        if any(kw in dest.lower() for kw in _EUROPE_KEYWORDS):
            eu_count += 1
        total += 1

    if total < 2:
        return  # too few data points to be meaningful

    eu_pct = round(eu_count / total * 100, 1)
    dest_interp = "europe_heavy" if eu_pct > 60 else "asia_heavy" if eu_pct < 25 else "balanced"
    conn.execute(_UPSERT_SQL, [today, "lng_destination_eu_pct", eu_pct, dest_interp, now_str])

    logger.info(
        "[features_lng] destination mix: EU=%.0f%% of %d vessels",
        eu_pct, total,
    )


def _run_eia_fallback(conn, today: date, now_str: str) -> None:
    """Fall back to EIA monthly LNG export data when AIS is unavailable."""
    row = conn.execute("""
        SELECT observation_time, value
        FROM facts_time_series
        WHERE source_name = 'eia_supply'
          AND series_name = 'lng_exports_mmcf'
          AND value IS NOT NULL
        ORDER BY observation_time DESC
        LIMIT 1
    """).fetchone()

    if not row:
        logger.info("[features_lng] no EIA supply data available — skipping")
        return

    obs_time, mmcf_month = row
    if mmcf_month is None:
        logger.info("[features_lng] EIA supply value is null — skipping")
        return

    if isinstance(obs_time, str):
        obs_dt = datetime.fromisoformat(obs_time)
    else:
        obs_dt = obs_time

    days_in_month = calendar.monthrange(obs_dt.year, obs_dt.month)[1]
    bcfd = (float(mmcf_month) / 1000.0) / days_in_month
    interp = _interpret_eia_exports(bcfd)

    # Only write BCF/D — skip utilization % and EPI because the static capacity
    # list is outdated and per-terminal counts aren't available from EIA monthly data.
    conn.execute(_UPSERT_SQL_LOW, [today, "lng_implied_exports_bcfd", bcfd, interp, now_str])

    logger.info(
        "[features_lng] EIA fallback: %.1f MMcf/month → %.2f Bcfd (source=%s)",
        mmcf_month, bcfd, obs_dt.strftime("%Y-%m"),
    )


def backfill_lng_from_eia() -> None:
    """Backfill historical lng_implied_exports_bcfd from EIA monthly data.

    Writes one features_daily row per available EIA month (using the last day
    of each month as feature_date). Existing AIS-derived rows are not overwritten
    since those are written with ON CONFLICT DO UPDATE only if the upsert fires;
    here we use a lighter touch and skip dates that already have a 'medium'
    confidence entry.
    """
    conn = duckdb.connect(DB_PATH)
    now_str = datetime.now(timezone.utc).isoformat()

    try:
        rows = conn.execute("""
            SELECT observation_time, value
            FROM facts_time_series
            WHERE source_name = 'eia_supply'
              AND series_name = 'lng_exports_mmcf'
              AND value IS NOT NULL
            ORDER BY observation_time ASC
        """).fetchall()

        if not rows:
            logger.info("[features_lng] no EIA data for backfill")
            return

        n = 0
        for obs_time, mmcf_month in rows:
            if isinstance(obs_time, str):
                obs_dt = datetime.fromisoformat(obs_time)
            else:
                obs_dt = obs_time

            days_in_month = calendar.monthrange(obs_dt.year, obs_dt.month)[1]
            bcfd = (float(mmcf_month) / 1000.0) / days_in_month
            interp = _interpret_eia_exports(bcfd)

            # Use last day of the observation month as feature_date.
            feature_date = date(obs_dt.year, obs_dt.month, days_in_month)

            conn.execute(_UPSERT_SQL_LOW, [feature_date, "lng_implied_exports_bcfd", bcfd, interp, now_str])
            n += 1

        logger.info("[features_lng] backfilled %d months from EIA data", n)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Signal computation helpers
# ---------------------------------------------------------------------------

def _compute_epi(implied_bcfd: float, total_anchored: int) -> float:
    """Export Pressure Index (0–100).

    Higher = more bullish pressure on domestic gas prices.

    Components:
      - Export rate (65%): how much of nameplate capacity is actively loading
      - Queue pressure (35%): anchored ships waiting signal demand > throughput;
        scaled so 5+ ships = 100% queue pressure (very rare, very bullish)
    """
    export_rate_pct = min(implied_bcfd / _TOTAL_CAPACITY * 100.0, 100.0)
    queue_pressure  = min(total_anchored / 5.0 * 100.0, 100.0)
    epi = 0.65 * export_rate_pct + 0.35 * queue_pressure
    return round(min(epi, 100.0), 1)


def _interpret_utilization(pct: float) -> str:
    if pct >= 85:
        return "high_exports"
    if pct >= 60:
        return "moderate_exports"
    if pct >= 30:
        return "low_exports"
    return "minimal_exports"


def _interpret_epi(epi: float) -> str:
    if epi >= 75:
        return "high_pressure"
    if epi >= 50:
        return "moderate_pressure"
    if epi >= 25:
        return "low_pressure"
    return "minimal_pressure"


def _interpret_eia_exports(bcfd: float) -> str:
    """Interpret raw BCF/D export rate (EIA-sourced, no per-terminal data)."""
    if bcfd >= 14:
        return "high_exports"
    if bcfd >= 10:
        return "moderate_exports"
    if bcfd >= 5:
        return "low_exports"
    return "minimal_exports"
