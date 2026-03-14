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
from datetime import date, datetime, timedelta, timezone

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


def _load_ng_prices(conn) -> dict[str, float]:
    """Return {date_str: close_price} for NG front-month from facts_time_series."""
    rows = conn.execute("""
        SELECT observation_time::TIMESTAMP::DATE::VARCHAR, value
        FROM facts_time_series
        WHERE source_name = 'yfinance' AND series_name = 'ng_front_close'
        ORDER BY observation_time
    """).fetchall()
    return {row[0]: float(row[1]) for row in rows}


def _forward_return(prices: dict[str, float], base_date_str: str, days: int) -> float | None:
    """Pct return from base_date to base_date+days; tolerates ±3 trading-day gaps."""
    try:
        base_date = date.fromisoformat(base_date_str[:10])
    except ValueError:
        return None

    def nearest(d: date) -> float | None:
        for delta in range(4):
            for signed in (delta, -delta):
                s = (d + timedelta(days=signed)).isoformat()
                if s in prices:
                    return prices[s]
        return None

    base_price = nearest(base_date)
    if not base_price or base_price <= 0:
        return None
    fwd_price = nearest(base_date + timedelta(days=days))
    if fwd_price is None:
        return None
    return round((fwd_price - base_price) / base_price * 100, 2)


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

    ng_prices = _load_ng_prices(conn)
    for a in top5:
        a["price_outcome"] = {
            "return_4w_pct":  _forward_return(ng_prices, a["date"], 28),
            "return_8w_pct":  _forward_return(ng_prices, a["date"], 56),
            "return_12w_pct": _forward_return(ng_prices, a["date"], 84),
        }

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


# ── Historical snapshot backfill ──────────────────────────────────────────────
# EOS bounds mirror features_storage.py so analog vectors are comparable to
# live snapshots.
_SNAP_WITHDRAWAL_MONTHS: frozenset[int] = frozenset({11, 12, 1, 2, 3})
_SNAP_EOS_WITHDRAWAL_LOW,  _SNAP_EOS_WITHDRAWAL_HIGH = 1700.0, 2000.0
_SNAP_EOS_INJECTION_LOW,   _SNAP_EOS_INJECTION_HIGH  = 3500.0, 3800.0


def backfill_feature_snapshots() -> None:
    """Reconstruct historical feature_snapshots from facts_time_series raw data.

    Seeds feature_snapshots with years of computed analog vectors so the
    AnalogsPanel works immediately after bootstrap rather than waiting 52+
    days for live transforms to accumulate enough history.

    Uses EIA weekly storage report dates as anchors (weekly granularity for
    history, then daily once live transforms take over).  For each date:
      - Storage: deficit vs 5yr avg, WoW change, EOS projection, EOS deficit
      - COT: MM net % OI (most recent on or before each storage date)
      - Weather: 7-day trailing actual HDD from NOAA historical data +
                 demand estimate via demand_coefficients.

    Note: hdd_7d_weighted here uses *trailing* actual HDD (NOAA historical),
    not a forward NWS forecast as in the live system.  This is an acceptable
    proxy for historical analog comparisons.

    Idempotent — ON CONFLICT DO UPDATE.  Skips dates with < 3 features.
    """
    conn = connect_db()
    now = datetime.now(timezone.utc).isoformat()
    try:
        _run_snapshot_backfill(conn, now)
    finally:
        conn.close()


def _snap_to_date(val) -> date:
    """Coerce DuckDB output (date, datetime, or str) to datetime.date."""
    if type(val) is date:
        return val
    if isinstance(val, datetime):
        return val.date()
    return date.fromisoformat(str(val)[:10])


def _snap_eos_projection(current_storage: float, snap_date: date, avg_pace: float | None) -> float | None:
    if avg_pace is None:
        return None
    month = snap_date.month
    if month in _SNAP_WITHDRAWAL_MONTHS:
        season_end = (
            date(snap_date.year + 1, 3, 31) if month >= 11
            else date(snap_date.year, 3, 31)
        )
    else:
        season_end = date(snap_date.year, 10, 31)
    weeks_remaining = max(0, (season_end - snap_date).days // 7)
    return current_storage + avg_pace * weeks_remaining


def _snap_eos_comfortable_mid(snap_date: date) -> float:
    if snap_date.month in _SNAP_WITHDRAWAL_MONTHS:
        return (_SNAP_EOS_WITHDRAWAL_LOW + _SNAP_EOS_WITHDRAWAL_HIGH) / 2.0
    return (_SNAP_EOS_INJECTION_LOW + _SNAP_EOS_INJECTION_HIGH) / 2.0


def _run_snapshot_backfill(conn, now: str) -> None:
    import bisect
    from transforms.demand_coefficients import estimate_demand

    # ── EIA weekly storage ────────────────────────────────────────────────────
    storage_raw = conn.execute("""
        SELECT observation_time::DATE AS obs_date, value
        FROM facts_time_series
        WHERE source_name = 'eia_storage'
          AND series_name  = 'storage_total'
        ORDER BY obs_date ASC
    """).fetchall()

    if len(storage_raw) < 5:
        logger.info("[analog backfill] not enough storage history — skipping")
        return

    storage_dates = [_snap_to_date(r[0]) for r in storage_raw]
    storage_vals  = [r[1] for r in storage_raw]

    # ── 5yr avg by ISO week (most-recent row wins per week) ───────────────────
    stats_raw = conn.execute("""
        SELECT observation_time::DATE AS obs_date, value
        FROM facts_time_series
        WHERE source_name = 'eia_storage_stats'
          AND series_name  = 'storage_5yr_avg_total'
        ORDER BY obs_date DESC
    """).fetchall()
    five_yr_by_week: dict[int, float] = {}
    for obs_date, val in stats_raw:
        wk = _snap_to_date(obs_date).isocalendar()[1]
        five_yr_by_week.setdefault(wk, val)

    # ── CFTC COT ──────────────────────────────────────────────────────────────
    cot_raw = conn.execute("""
        SELECT observation_time::DATE AS obs_date, series_name, value
        FROM facts_time_series
        WHERE source_name = 'cftc'
          AND series_name IN ('cot_mm_long', 'cot_mm_short', 'cot_open_interest')
        ORDER BY obs_date ASC
    """).fetchall()
    cot_by_date: dict = {}
    for obs_date, sname, val in cot_raw:
        cot_by_date.setdefault(_snap_to_date(obs_date), {})[sname] = val
    cot_dates = sorted(cot_by_date)

    # ── NOAA daily HDD historical ─────────────────────────────────────────────
    hdd_raw = conn.execute("""
        SELECT observation_time::DATE AS obs_date, value
        FROM facts_time_series
        WHERE source_name = 'noaa_hdd_historical'
          AND series_name  = 'hdd_weighted_national'
        ORDER BY obs_date ASC
    """).fetchall()
    hdd_by_date: dict = {_snap_to_date(r[0]): r[1] for r in hdd_raw}
    hdd_dates = sorted(hdd_by_date)

    # ── Build one snapshot per storage report date ────────────────────────────
    n_written = 0
    for i in range(4, len(storage_dates)):
        snap_date    = storage_dates[i]
        curr_storage = storage_vals[i]

        # Storage features
        wow_change = curr_storage - storage_vals[i - 1]
        weekly_changes = [
            storage_vals[j] - storage_vals[j - 1]
            for j in range(i, max(i - 4, 0), -1)
        ]
        avg_pace = sum(weekly_changes) / len(weekly_changes) if weekly_changes else None

        eos_proj    = _snap_eos_projection(curr_storage, snap_date, avg_pace)
        eos_deficit = (
            _snap_eos_comfortable_mid(snap_date) - eos_proj
            if eos_proj is not None else None
        )

        wk             = snap_date.isocalendar()[1]
        five_yr        = five_yr_by_week.get(wk)
        deficit_vs_5yr = (curr_storage - five_yr) if five_yr is not None else None

        # COT: carry forward to snap_date
        mm_net_pct: float | None = None
        if cot_dates:
            ci = bisect.bisect_right(cot_dates, snap_date) - 1
            if ci >= 0:
                c  = cot_by_date[cot_dates[ci]]
                ml = c.get("cot_mm_long")
                ms = c.get("cot_mm_short")
                oi = c.get("cot_open_interest")
                if ml is not None and ms is not None and oi and oi > 0:
                    mm_net_pct = (ml - ms) / oi * 100.0

        # HDD: 7-day trailing actual from NOAA historical
        hdd_7d: float | None = None
        demand: float | None = None
        if hdd_dates:
            hi = bisect.bisect_right(hdd_dates, snap_date) - 1
            if hi >= 6:
                seven = [hdd_by_date.get(hdd_dates[hi - k]) for k in range(7)]
                if all(v is not None for v in seven):
                    hdd_7d = sum(seven)
                    demand = estimate_demand(hdd_7d, snap_date.month)

        # Assemble vector — only include features that have a value
        vector: dict[str, float] = {}
        if deficit_vs_5yr is not None:
            vector["storage_deficit_vs_5yr_bcf"]      = deficit_vs_5yr
        if wow_change is not None:
            vector["storage_wow_change_bcf"]           = wow_change
        if eos_proj is not None:
            vector["storage_eos_projection_bcf"]       = eos_proj
        if eos_deficit is not None:
            vector["storage_eos_deficit_vs_norm_bcf"]  = eos_deficit
        if mm_net_pct is not None:
            vector["cot_mm_net_pct_oi"]                = mm_net_pct
        if hdd_7d is not None:
            vector["hdd_7d_weighted"]                  = hdd_7d
        if demand is not None:
            vector["weather_implied_resi_comm_bcfd"]   = demand

        if len(vector) < 3:
            continue

        conn.execute("""
            INSERT INTO feature_snapshots (snapshot_date, feature_vector, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT (snapshot_date) DO UPDATE SET
                feature_vector = excluded.feature_vector,
                created_at     = excluded.created_at
        """, [snap_date, json.dumps(vector), now])
        n_written += 1

    logger.info("[analog backfill] wrote %d historical snapshots to feature_snapshots", n_written)
