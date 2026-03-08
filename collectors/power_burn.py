import logging
from datetime import datetime, timedelta, timezone

import duckdb
import requests

from collectors.base import CollectorBase
from config.settings import DB_PATH, EIA_API_KEY

logger = logging.getLogger("collectors")

# EIA-930 covers all U.S. balancing authorities. ERCOT (ERCO) is included here,
# so a separate ERCOT collector is not needed for gas burn analysis.
REGIONS: dict[str, str] = {
    "ERCO": "ERCOT/Texas",
    "MISO": "Midwest ISO",
    "PJM":  "PJM/Northeast",
    "SWPP": "SPP/Central",
    "SOCO": "Southeast",
    "NYIS": "New York ISO",
    "ISNE": "ISO New England",
    "CISO": "California ISO",
}

# Pull 72 hours to guarantee no gaps given the ~1 hour reporting lag
LOOKBACK_HOURS = 72

_INSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency)
    VALUES ('eia_930', 'gas_fired_gen_mw', ?, ?, ?, ?, 'MW', 'hourly')
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
"""


class PowerBurnCollector(CollectorBase):
    source_name = "eia_930_power_burn"

    def collect(self) -> dict:
        records_written = 0
        conn = duckdb.connect(DB_PATH)
        now = datetime.now(timezone.utc)
        start = (now - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H")
        now_str = now.isoformat()

        try:
            for region_code in REGIONS:
                try:
                    records_written += self._collect_region(
                        conn, region_code, start, now_str
                    )
                except Exception as e:
                    # One region failure must not stop the remaining regions
                    logger.warning("[power_burn] %s failed: %s", region_code, e)
        finally:
            conn.close()

        return {"status": "ok", "records_written": records_written}

    def _collect_region(
        self, conn, region_code: str, start: str, now_str: str
    ) -> int:
        resp = requests.get(
            "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/",
            params={
                "api_key": EIA_API_KEY,
                "data[0]": "value",
                "facets[respondent][]": region_code,
                "facets[fueltype][]": "NG",
                "start": start,
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": LOOKBACK_HOURS,
            },
            timeout=90,
        )
        resp.raise_for_status()
        payload = resp.json()
        self.save_raw(payload, subdir="power_burn")

        count = 0
        for row in payload.get("response", {}).get("data", []):
            period = row.get("period", "")  # EIA-930 format: "2026-03-06T14"
            value = row.get("value")
            if not period or value is None:
                continue
            obs_time = f"{period}:00:00Z"
            conn.execute(_INSERT_SQL, [region_code, obs_time, now_str, float(value)])
            count += 1

        return count
