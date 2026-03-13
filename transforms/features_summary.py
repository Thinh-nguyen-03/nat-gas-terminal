import json
import logging
from datetime import date, datetime, timedelta, timezone

import duckdb

from config.settings import DB_PATH, connect_db

logger = logging.getLogger("collectors")

# Features tracked in the "What Changed" table, ordered by trading relevance
TRACKED_FEATURES: list[str] = [
    "storage_eos_projection_bcf",
    "storage_deficit_vs_5yr_bcf",
    "weather_hdd_revision_delta",
    "weather_hdd_7d_weighted",
    "ng_price_current",
    "ng_price_daily_chg_pct",
    "ng_nov_jan_spread",
    "cot_mm_net_pct_oi",
    "dry_gas_production_bcfd",
    "lng_exports_bcf",
]

# Minimum absolute change required to flag a feature as significant
SIGNIFICANCE_THRESHOLDS: dict[str, float] = {
    "storage_eos_projection_bcf":   50.0,   # Bcf shift in trajectory
    "storage_deficit_vs_5yr_bcf":   20.0,   # Bcf shift in deficit
    "weather_hdd_revision_delta":    2.0,   # pop-weighted HDD revision
    "weather_hdd_7d_weighted":       5.0,
    "ng_price_current":              0.05,  # $/MMBtu move
    "ng_price_daily_chg_pct":        1.5,   # 1.5% daily price change
    "ng_nov_jan_spread":             0.05,
    "cot_mm_net_pct_oi":             3.0,   # pct-of-OI swing
    "dry_gas_production_bcfd":       0.5,   # Bcf/d production change
    "lng_exports_bcf":               1.0,   # Bcf weekly change
}

# Fundamental score weight by component (must sum to 100)
_SCORE_WEIGHTS = {
    "storage_deficit":    20,
    "eos_trajectory":     20,
    "weather_hdd":        15,
    "weather_revision":   15,
    "production":         10,
    "cot_positioning":    10,
    "lng_exports":        10,
}

_SUMMARY_UPSERT_SQL = """
    INSERT INTO summary_outputs
        (summary_date, summary_type, content, generated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT (summary_date, summary_type)
    DO UPDATE SET content = excluded.content,
                 generated_at = excluded.generated_at
"""


def compute_what_changed() -> list[dict]:
    """
    Compare today's features against yesterday's. Return a list of changes
    sorted so the most significant appear first.
    """
    conn = connect_db()
    today = date.today()
    yesterday = today - timedelta(days=1)
    changes: list[dict] = []

    try:
        for feature in TRACKED_FEATURES:
            today_row = conn.execute("""
                SELECT value, interpretation FROM features_daily
                WHERE feature_date = ? AND feature_name = ? AND region = 'US'
            """, [today, feature]).fetchone()

            if not today_row:
                continue

            yesterday_row = conn.execute("""
                SELECT value, interpretation FROM features_daily
                WHERE feature_date = ? AND feature_name = ? AND region = 'US'
            """, [yesterday, feature]).fetchone()

            today_val    = today_row[0]
            today_interp = today_row[1]
            prior_val    = yesterday_row[0] if yesterday_row else None
            prior_interp = yesterday_row[1] if yesterday_row else None

            delta = (
                today_val - prior_val
                if (today_val is not None and prior_val is not None) else None
            )
            delta_pct = (
                delta / abs(prior_val) * 100
                if (delta is not None and prior_val and prior_val != 0) else None
            )
            threshold   = SIGNIFICANCE_THRESHOLDS.get(feature, 0.0)
            significant = abs(delta) > threshold if delta is not None else False

            changes.append({
                "feature":        feature,
                "current_value":  today_val,
                "prior_value":    prior_val,
                "delta":          delta,
                "delta_pct":      delta_pct,
                "interpretation": today_interp,
                "prior_interp":   prior_interp,
                "significant":    significant,
                "interp_changed": today_interp != prior_interp,
            })
    finally:
        conn.close()

    # Significant changes first, then interpretation flips, then the rest
    return sorted(changes, key=lambda x: (not x["significant"], not x["interp_changed"]))


def compute_fundamental_score() -> dict:
    """
    Composite score from -100 (max bearish) to +100 (max bullish).
    Each component is clamped to its weight before summing.
    Returns the score, a label, and the top driver bullets.
    """
    conn = connect_db()
    today = date.today()
    score = 0.0
    drivers: list[str] = []

    try:
        score, drivers = _build_score(conn, today)
    finally:
        conn.close()

    return {
        "score":   round(score, 1),
        "label":   _score_label(score),
        "drivers": drivers[:4],  # top 4 shown in morning brief
    }


def save_summary() -> None:
    """Compute score and what-changed, persist both to summary_outputs."""
    score_data = compute_fundamental_score()
    changes    = compute_what_changed()
    today = date.today()
    now   = datetime.now(timezone.utc).isoformat()

    conn = connect_db()
    try:
        for summary_type, payload in [
            ("fundamental_score", score_data),
            ("what_changed",      changes),
        ]:
            conn.execute(_SUMMARY_UPSERT_SQL, [
                today, summary_type, json.dumps(payload), now
            ])
    finally:
        conn.close()


def _get_feature(name: str, today: date, conn) -> float | None:
    row = conn.execute("""
        SELECT value FROM features_daily
        WHERE feature_date = ? AND feature_name = ? AND region = 'US'
    """, [today, name]).fetchone()
    return row[0] if row else None


def _build_score(conn, today: date) -> tuple[float, list[str]]:
    score = 0.0
    drivers: list[str] = []
    month = today.month

    deficit = _get_feature("storage_deficit_vs_5yr_bcf", today, conn)
    if deficit is not None:
        s = max(-20.0, min(20.0, -deficit / 12.0))
        score += s
        if abs(s) > 8:
            direction = "below" if deficit < 0 else "above"
            label = "bullish" if deficit < 0 else "bearish"
            drivers.append(
                f"Storage {abs(deficit):.0f} Bcf {direction} 5yr avg ({label})"
            )

    eos = _get_feature("storage_eos_projection_bcf", today, conn)
    if eos is not None:
        comfortable_mid = 1850.0 if month in {11, 12, 1, 2, 3} else 3650.0
        s = max(-20.0, min(20.0, (comfortable_mid - eos) / 18.0))
        score += s
        if abs(s) > 8:
            direction = "below" if eos < comfortable_mid else "above"
            drivers.append(
                f"EOS trajectory {eos:.0f} Bcf — {direction} comfortable range"
            )

    hdd = _get_feature("weather_hdd_7d_weighted", today, conn)
    if hdd is not None:
        score += max(-15.0, min(15.0, (hdd - 40.0) / 5.0))

    revision = _get_feature("weather_hdd_revision_delta", today, conn)
    if revision is not None:
        s = max(-15.0, min(15.0, revision * 2.0))
        score += s
        if abs(revision) > 2:
            direction = "colder" if revision > 0 else "warmer"
            drivers.append(
                f"Forecast revised {abs(revision):.1f} pop-wtd HDD {direction} overnight"
            )

    prod = _get_feature("dry_gas_production_bcfd", today, conn)
    if prod is not None:
        score += max(-10.0, min(10.0, (103.0 - prod) * 4.0))

    mm_pct = _get_feature("cot_mm_net_pct_oi", today, conn)
    if mm_pct is not None:
        s = max(-10.0, min(10.0, -mm_pct / 3.0))
        score += s
        if abs(mm_pct) > 15:
            crowding = "long" if mm_pct > 0 else "short"
            consequence = "unwind risk" if mm_pct > 0 else "squeeze risk on bullish catalyst"
            drivers.append(
                f"Specs crowded {crowding} ({mm_pct:.1f}% OI) — {consequence}"
            )

    lng = _get_feature("lng_exports_bcf", today, conn)
    if lng is not None:
        score += max(-10.0, min(10.0, (lng - 14.0) * 3.0))

    return score, drivers


def _score_label(score: float) -> str:
    if score > 40:
        return "Strongly Bullish"
    if score > 20:
        return "Bullish"
    if score > 5:
        return "Mildly Bullish"
    if score > -5:
        return "Neutral / Mixed"
    if score > -20:
        return "Mildly Bearish"
    if score > -40:
        return "Bearish"
    return "Strongly Bearish"
