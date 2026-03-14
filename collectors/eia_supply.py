import logging
from datetime import datetime, timezone

import duckdb
import requests

from collectors.base import CollectorBase
from config.settings import DB_PATH, EIA_API_KEY

logger = logging.getLogger("collectors")

# EIA supply fundamentals — dry production, LNG exports, power burn, Mexico pipeline.
# Data is weekly with roughly a 2-week lag; daily polling catches each new release.
# Validate series IDs at https://api.eia.gov/v2/natural-gas/ — EIA reorganizes
# series paths without notice and these may need updating.
EIA_SUPPLY_SERIES: dict[str, tuple[str, str, str]] = {
    #  Series ID             : (series_name,                      unit,    frequency)
    # All series are monthly — EIA does not publish weekly supply fundamentals via v2 seriesid.
    # Verify IDs at https://www.eia.gov/opendata/browser/natural-gas if a series returns 404.
    "NG.N9070US2.M": ("dry_gas_production_mmcf",      "MMcf", "monthly"),
    "NG.N9133US2.M": ("lng_exports_mmcf",              "MMcf", "monthly"),
    "NG.N3045US2.M": ("power_sector_burn_mmcf",        "MMcf", "monthly"),
    "NG.N9132MX2.M": ("mexico_pipeline_exp_mmcf",      "MMcf", "monthly"),
    # Total US imports is ~99% Canada pipeline (US receives negligible non-Canadian pipeline gas)
    "NG.N9100US2.M": ("total_imports_mmcf",            "MMcf", "monthly"),
    "NG.N9130US2.M": ("total_pipeline_exports_mmcf",   "MMcf", "monthly"),
}

LOOKBACK_WEEKS = 52

_INSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency)
    VALUES ('eia_supply', ?, 'US', ?, ?, ?, ?, ?)
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
"""


class EIASupplyCollector(CollectorBase):
    source_name = "eia_supply"

    def collect(self) -> dict:
        records_written = 0
        conn = duckdb.connect(DB_PATH)
        now_str = datetime.now(timezone.utc).isoformat()

        try:
            for series_id, (series_name, unit, freq) in EIA_SUPPLY_SERIES.items():
                try:
                    records_written += self._collect_series(
                        conn, series_id, series_name, unit, freq, now_str
                    )
                except Exception as e:
                    logger.warning("[eia_supply] %s failed: %s", series_id, e)
        finally:
            conn.close()

        return {"status": "ok", "records_written": records_written}

    def _collect_series(
        self,
        conn,
        series_id: str,
        series_name: str,
        unit: str,
        freq: str,
        now_str: str,
    ) -> int:
        resp = requests.get(
            f"https://api.eia.gov/v2/seriesid/{series_id}",
            params={"api_key": EIA_API_KEY, "data[0]": "value", "length": LOOKBACK_WEEKS},
            timeout=90,
        )
        resp.raise_for_status()
        payload = resp.json()
        self.save_raw(payload, subdir="eia_supply")

        count = 0
        for row in payload.get("response", {}).get("data", []):
            period_str = row.get("period", "")
            value = row.get("value")
            if not period_str or value is None:
                continue

            # EIA period: "2026-02-28" (weekly) or "2026-02" (monthly)
            if len(period_str) == 7:
                obs_time = f"{period_str}-01T00:00:00Z"
            else:
                obs_time = f"{period_str}T00:00:00Z"

            conn.execute(_INSERT_SQL, [
                series_name, obs_time, now_str, float(value), unit, freq
            ])
            count += 1

        return count
