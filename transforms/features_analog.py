"""
Historical Analog Finder (Feature 6).

Each scheduled run:
  1. Builds a feature vector from the most-recent values in features_daily.
  2. Saves today's vector to feature_snapshots (idempotent upsert).
  3. Loads all stored snapshots and computes cosine similarity to today's vector.
  4. Writes the top-5 analog dates + scores to summary_outputs as 'analog_finder'.

The transform requires MIN_SNAPSHOTS historical snapshots before reporting
results, so it silently no-ops until enough history has accumulated (~1 year).
"""

from __future__ import annotations

import json
import logging
import math
from datetime import date, datetime, timezone

import duckdb

from config.settings import DB_PATH, connect_db

logger = logging.getLogger("collectors")

# Features included in the analog comparison vector.
# Must be present in features_daily (written by the storage, COT, weather, and
# price transforms). Missing features are excluded from the similarity calc.
_FEATURE_KEYS: list[str] = [
    "storage_deficit_vs_5yr_bcf",
    "storage_wow_change_bcf",
    "storage_eos_projection_bcf",
    "storage_eos_deficit_vs_norm_bcf",
    "cot_mm_net_pct_oi",
    "hdd_7d_weighted",
    "weather_implied_resi_comm_bcfd",
]

# Minimum historical snapshots required before publishing analog results.
# At 1 snapshot/day, this is roughly 1 year.
MIN_SNAPSHOTS = 52

# Exclude the most recent N days from the historical search to avoid
# self-matching the current market regime.
EXCLUDE_RECENT_DAYS = 30


def compute_analog_features() -> None:
    conn = connect_db()
    today = date.today()
    now = datetime.now(timezone.utc).isoformat()

    try:
        _run(conn, today, now)
    finally:
        conn.close()


def _run(conn, today: date, now: str) -> None:
    vector = _build_today_vector(conn)
    if len(vector) < 3:
        logger.info("[analog] only %d features available — skipping", len(vector))
        return

    # Always persist today's snapshot so history accumulates.
    conn.execute("""
        INSERT INTO feature_snapshots (snapshot_date, feature_vector, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT (snapshot_date) DO UPDATE SET
            feature_vector = excluded.feature_vector,
            created_at     = excluded.created_at
    """, [today, json.dumps(vector), now])
    logger.info("[analog] snapshot saved for %s (%d features)", today, len(vector))

    # Load historical snapshots outside the recent exclusion window.
    cutoff = date.fromordinal(today.toordinal() - EXCLUDE_RECENT_DAYS)
    rows = conn.execute("""
        SELECT snapshot_date, feature_vector
        FROM feature_snapshots
        WHERE snapshot_date < ?
        ORDER BY snapshot_date
    """, [cutoff]).fetchall()

    if len(rows) < MIN_SNAPSHOTS:
        logger.info(
            "[analog] %d historical snapshots available (need %d) — skipping analog search",
            len(rows), MIN_SNAPSHOTS,
        )
        return

    # Only compare on keys present in today's vector AND each historical snapshot.
    today_keys = [k for k in _FEATURE_KEYS if k in vector]

    analogs: list[dict] = []
    for snap_date, snap_json in rows:
        hist = json.loads(snap_json)
        shared = [k for k in today_keys if k in hist]
        if len(shared) < 3:
            continue
        sim = _cosine_similarity(vector, hist, shared)
        d = snap_date.isoformat() if hasattr(snap_date, "isoformat") else str(snap_date)
        analogs.append({
            "date":       d,
            "similarity": round(sim, 4),
            "features":   {k: round(hist[k], 2) for k in shared},
        })

    if not analogs:
        return

    analogs.sort(key=lambda x: x["similarity"], reverse=True)
    top5 = analogs[:5]

    result = {
        "computed_at":    now,
        "reference_date": today.isoformat(),
        "feature_vector": {k: round(vector[k], 2) for k in today_keys},
        "analogs":        top5,
    }

    conn.execute("""
        INSERT INTO summary_outputs
            (summary_date, summary_type, content, inputs_hash, generated_at)
        VALUES (?, 'analog_finder', ?, NULL, ?)
        ON CONFLICT (summary_date, summary_type) DO UPDATE SET
            content      = excluded.content,
            generated_at = excluded.generated_at
    """, [today, json.dumps(result), now])

    logger.info(
        "[analog] top analog: %s (similarity=%.3f)",
        top5[0]["date"], top5[0]["similarity"],
    )


def _build_today_vector(conn) -> dict[str, float]:
    """Return the most-recent value for each feature key from features_daily."""
    placeholders = ",".join("?" * len(_FEATURE_KEYS))
    rows = conn.execute(f"""
        SELECT feature_name, value
        FROM (
            SELECT feature_name, value,
                   ROW_NUMBER() OVER (PARTITION BY feature_name
                                      ORDER BY feature_date DESC) AS rn
            FROM features_daily
            WHERE feature_name IN ({placeholders})
              AND region = 'US'
        ) ranked
        WHERE rn = 1
    """, _FEATURE_KEYS).fetchall()

    return {name: val for name, val in rows if val is not None}


def _cosine_similarity(a: dict, b: dict, keys: list[str]) -> float:
    dot   = sum(a.get(k, 0.0) * b.get(k, 0.0) for k in keys)
    mag_a = math.sqrt(sum(a.get(k, 0.0) ** 2 for k in keys))
    mag_b = math.sqrt(sum(b.get(k, 0.0) ** 2 for k in keys))
    if mag_a == 0.0 or mag_b == 0.0:
        return 0.0
    return dot / (mag_a * mag_b)
