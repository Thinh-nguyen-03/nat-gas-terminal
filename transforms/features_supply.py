"""
Supply/demand balance feature transform.

Converts monthly EIA supply fundamentals (source='eia_supply' in facts_time_series)
and hourly EIA-930 power burn (source='eia_930') into daily Bcf/d features
written to features_daily.

Features written:
  dry_gas_production_bcfd      — marketed dry production (EIA monthly, ~2-month lag)
  canada_imports_bcfd          — total pipeline imports (EIA monthly, ~99% Canada)
  power_burn_bcfd              — gas-fired power sector burn:
                                   primary  → EIA-930 trailing-24h per ISO, scaled to US
                                   fallback → EIA monthly power_sector_burn_mmcf
  mexico_pipeline_exports_bcfd — pipeline exports to Mexico (EIA monthly)

All features are written for today's feature_date using the most recent data available,
effectively forward-filling monthly values until the next release.

Runs every hour at :15 (after EIA-930 power_burn at :05) and at 8:15 AM ET
(30 min after the daily EIA supply collection at 8:00 AM).
"""
from __future__ import annotations

import calendar
import logging
from datetime import date, datetime, timedelta, timezone

from config.settings import connect_db

logger = logging.getLogger("collectors")

# MW → Bcf/d: 1 MW × 24h × 7500 BTU/kWh × 1000 kWh/MWh / (1025 BTU/cf × 1e9 cf/Bcf)
_MW_TO_BCFD: float = 24 * 7_500 * 1_000 / (1_025 * 1e9)   # ≈ 1.756e-4

# EIA-930 covers 8 ISOs (ERCO, MISO, PJM, SWPP, SOCO, NYIS, ISNE, CISO).
# These represent ~75% of US gas-fired generation; scale up to full US.
_ISO_COVERAGE: float = 0.75

# Minimum ISOs needed to prefer EIA-930 over EIA monthly.
_MIN_ISO_COUNT: int = 5

# EIA-930 data older than this falls back to the monthly EIA series.
_MAX_EIA930_AGE_H: int = 48

_UPSERT = """
    INSERT INTO features_daily
        (feature_date, feature_name, region, value,
         interpretation, confidence, computed_at)
    VALUES (?, ?, 'US', ?, 'normal', ?, ?)
    ON CONFLICT (feature_date, feature_name, region)
    DO UPDATE SET value       = excluded.value,
                 confidence  = excluded.confidence,
                 computed_at = excluded.computed_at
"""


def compute_supply_features() -> None:
    conn = connect_db()
    today = date.today()
    now_str = datetime.now(timezone.utc).isoformat()
    try:
        _run(conn, today, now_str)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _run(conn, today: date, now_str: str) -> None:
    _compute_monthly_supply(conn, today, now_str)
    _compute_power_burn(conn, today, now_str)


def _compute_monthly_supply(conn, today: date, now_str: str) -> None:
    """Convert the latest monthly EIA supply values to daily Bcf/d."""
    series_to_feature: dict[str, str] = {
        "dry_gas_production_mmcf":     "dry_gas_production_bcfd",
        "total_imports_mmcf":          "canada_imports_bcfd",
        "mexico_pipeline_exp_mmcf":    "mexico_pipeline_exports_bcfd",
    }

    rows = conn.execute("""
        SELECT series_name, value, observation_time
        FROM (
            SELECT series_name, value, observation_time,
                   ROW_NUMBER() OVER (
                       PARTITION BY series_name ORDER BY observation_time DESC
                   ) AS rn
            FROM facts_time_series
            WHERE source_name = 'eia_supply'
              AND series_name IN (
                    'dry_gas_production_mmcf',
                    'total_imports_mmcf',
                    'mexico_pipeline_exp_mmcf'
                  )
        ) t
        WHERE rn = 1
    """).fetchall()

    for series_name, raw_mmcf, obs_time in rows:
        if raw_mmcf is None:
            continue
        feat_name = series_to_feature[series_name]

        # obs_time is stored as "YYYY-MM-01T00:00:00Z" (first of the reporting month).
        # Extract year/month in UTC — local timezone can shift it to the prior month.
        if hasattr(obs_time, "year"):
            obs_utc = obs_time.astimezone(timezone.utc) if (hasattr(obs_time, "tzinfo") and obs_time.tzinfo) else obs_time
            yr, mo = obs_utc.year, obs_utc.month
        else:
            dt = datetime.fromisoformat(str(obs_time).replace("Z", "+00:00"))
            yr, mo = dt.year, dt.month
        days = calendar.monthrange(yr, mo)[1]

        value_bcfd = raw_mmcf / days / 1_000.0
        conn.execute(_UPSERT, [today, feat_name, value_bcfd, "medium", now_str])
        logger.info("[supply] %s = %.1f Bcf/d (EIA %d-%02d, %d days)",
                    feat_name, value_bcfd, yr, mo, days)


def _compute_power_burn(conn, today: date, now_str: str) -> None:
    """Compute power burn Bcf/d.

    Primary: sum the latest per-ISO reading from EIA-930, scale for full US coverage.
    Fallback: EIA monthly power_sector_burn_mmcf (stale but complete).
    """
    # Attempt EIA-930 path.
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_MAX_EIA930_AGE_H)

    iso_rows = conn.execute("""
        SELECT region, value, observation_time
        FROM (
            SELECT region, value, observation_time,
                   ROW_NUMBER() OVER (
                       PARTITION BY region ORDER BY observation_time DESC
                   ) AS rn
            FROM facts_time_series
            WHERE source_name = 'eia_930'
              AND series_name  = 'gas_fired_gen_mw'
        ) t
        WHERE rn = 1
    """).fetchall()

    valid_mw: list[float] = []
    for region, mw, obs_time in iso_rows:
        if mw is None:
            continue
        # Normalise obs_time to UTC-aware datetime for comparison.
        if hasattr(obs_time, "tzinfo") and obs_time.tzinfo is not None:
            obs_utc = obs_time.astimezone(timezone.utc)
        else:
            try:
                obs_utc = datetime.fromisoformat(
                    str(obs_time).replace("Z", "+00:00")
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        if obs_utc >= cutoff:
            valid_mw.append(mw)

    if len(valid_mw) >= _MIN_ISO_COUNT:
        total_mw = sum(valid_mw)
        power_burn_bcfd = (total_mw * _MW_TO_BCFD) / _ISO_COVERAGE
        confidence = "medium"
        source_tag = f"eia_930 ({len(valid_mw)} ISOs)"
    else:
        # Fall back to EIA monthly power sector burn.
        row = conn.execute("""
            SELECT value, observation_time
            FROM facts_time_series
            WHERE source_name = 'eia_supply'
              AND series_name  = 'power_sector_burn_mmcf'
            ORDER BY observation_time DESC
            LIMIT 1
        """).fetchone()

        if row is None or row[0] is None:
            logger.info("[supply] power_burn_bcfd: no data available (EIA-930 stale, no monthly fallback)")
            return

        raw_mmcf, obs_time = row
        if hasattr(obs_time, "year"):
            obs_utc = obs_time.astimezone(timezone.utc) if (hasattr(obs_time, "tzinfo") and obs_time.tzinfo) else obs_time
            yr, mo = obs_utc.year, obs_utc.month
        else:
            dt = datetime.fromisoformat(str(obs_time).replace("Z", "+00:00"))
            yr, mo = dt.year, dt.month
        days = calendar.monthrange(yr, mo)[1]
        power_burn_bcfd = raw_mmcf / days / 1_000.0
        confidence = "low"
        source_tag = f"eia_monthly ({yr}-{mo:02d})"

    conn.execute(_UPSERT, [today, "power_burn_bcfd", power_burn_bcfd, confidence, now_str])
    logger.info("[supply] power_burn_bcfd = %.1f Bcf/d (%s)", power_burn_bcfd, source_tag)
