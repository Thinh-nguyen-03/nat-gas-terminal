import logging
from datetime import datetime, timezone

import duckdb
import requests

from collectors.base import CollectorBase
from config.settings import DB_PATH, EIA_API_KEY

logger = logging.getLogger("collectors")

# EIA v2 seriesid — Natural Gas Working Storage by Region (Lower 48, Bcf, weekly)
# Validate IDs at: https://api.eia.gov/v2/natural-gas/
EIA_STORAGE_SERIES: dict[str, str] = {
    "total":         "NG.NW2_EPG0_SWO_R48_BCF.W",
    "east":          "NG.NW2_EPG0_SWO_R31_BCF.W",
    "midwest":       "NG.NW2_EPG0_SWO_R32_BCF.W",
    "south_central": "NG.NW2_EPG0_SWO_R33_BCF.W",
    "mountain":      "NG.NW2_EPG0_SWO_R34_BCF.W",
    "pacific":       "NG.NW2_EPG0_SWO_R35_BCF.W",
}

# 2 years of history supports the 5-year average once enough data accumulates
LOOKBACK_WEEKS = 104

_INSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency)
    VALUES ('eia_storage', ?, ?, ?, ?, ?, 'Bcf', 'weekly')
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
"""


class EIAStorageCollector(CollectorBase):
    source_name = "eia_storage"

    def collect(self) -> dict:
        records_written = 0
        conn = duckdb.connect(DB_PATH)
        now_str = datetime.now(timezone.utc).isoformat()

        try:
            for region, series_id in EIA_STORAGE_SERIES.items():
                try:
                    records_written += self._collect_series(
                        conn, region, series_id, now_str
                    )
                except Exception as e:
                    logger.warning("[eia_storage] %s failed: %s", region, e)
        finally:
            conn.close()

        return {"status": "ok", "records_written": records_written}

    def _collect_series(
        self, conn, region: str, series_id: str, now_str: str
    ) -> int:
        resp = requests.get(
            f"https://api.eia.gov/v2/seriesid/{series_id}",
            params={"api_key": EIA_API_KEY, "data[0]": "value", "length": LOOKBACK_WEEKS},
            timeout=90,
        )
        resp.raise_for_status()
        payload = resp.json()
        self.save_raw(payload, subdir="eia_storage")

        count = 0
        for row in payload.get("response", {}).get("data", []):
            # EIA period is the week-ending Saturday, e.g. "2026-02-28"
            period_str = row.get("period", "")
            value = row.get("value")
            if not period_str or value is None:
                continue

            obs_time = f"{period_str}T00:00:00Z"
            conn.execute(_INSERT_SQL, [
                f"storage_{region}",
                region,
                obs_time,
                now_str,
                float(value),
            ])
            count += 1

        return count
