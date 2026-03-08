import json
import logging
import os
from datetime import datetime, timezone

import duckdb
import requests

from collectors.base import CollectorBase
from config.settings import ARCHIVE_DIR, DB_PATH

logger = logging.getLogger("collectors")

NWS_BASE = "https://api.weather.gov"
HEADERS = {"User-Agent": "NatGasTerminal/1.0 (personal research tool)"}

# 8 cities covering the major gas-heated demand regions.
# Weights reflect gas-heated household density, not raw population.
# Weights sum to 1.00.
WEATHER_POINTS: dict[str, dict] = {
    "new_york":     {"lat": 40.71, "lon": -74.01, "pop_weight": 0.25},
    "chicago":      {"lat": 41.85, "lon": -87.65, "pop_weight": 0.15},
    "boston":       {"lat": 42.36, "lon": -71.06, "pop_weight": 0.10},
    "philadelphia": {"lat": 39.95, "lon": -75.16, "pop_weight": 0.12},
    "houston":      {"lat": 29.76, "lon": -95.37, "pop_weight": 0.10},
    "atlanta":      {"lat": 33.75, "lon": -84.39, "pop_weight": 0.08},
    "minneapolis":  {"lat": 44.98, "lon": -93.27, "pop_weight": 0.10},
    "detroit":      {"lat": 42.33, "lon": -83.05, "pop_weight": 0.10},
}

HDD_CDD_BASE_F = 65.0

_INSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency, metadata_json)
    VALUES ('nws', ?, ?, ?, ?, ?, ?, 'period', ?)
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value,
                 ingest_time = excluded.ingest_time
"""


class WeatherCollector(CollectorBase):
    source_name = "nws_weather"

    def collect(self) -> dict:
        records_written = 0
        conn = duckdb.connect(DB_PATH)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now_str = datetime.now(timezone.utc).isoformat()

        try:
            for city, coords in WEATHER_POINTS.items():
                try:
                    records_written += self._collect_city(
                        conn, city, coords, today, now_str
                    )
                except Exception as e:
                    # One city failure must not stop the remaining cities
                    logger.warning("[nws_weather] %s failed: %s", city, e)
        finally:
            conn.close()

        return {"status": "ok", "records_written": records_written}

    def _collect_city(
        self,
        conn,
        city: str,
        coords: dict,
        today: str,
        now_str: str,
    ) -> int:
        # Step 1: resolve NWS grid metadata for this lat/lon
        r = requests.get(
            f"{NWS_BASE}/points/{coords['lat']},{coords['lon']}",
            headers=HEADERS,
            timeout=30,
        )
        r.raise_for_status()
        points_data = r.json()
        forecast_url = points_data.get("properties", {}).get("forecast")
        if not forecast_url:
            raise ValueError(f"NWS points response missing forecast URL for {city}")

        # Step 2: fetch 7-day daily forecast
        r2 = requests.get(forecast_url, headers=HEADERS, timeout=30)
        r2.raise_for_status()
        forecast_data = r2.json()

        # Archive snapshot for day-over-day revision delta (must start from day 1)
        archive_path = os.path.join(ARCHIVE_DIR, today)
        os.makedirs(archive_path, exist_ok=True)
        with open(os.path.join(archive_path, f"{city}_forecast.json"), "w") as f:
            json.dump(forecast_data, f)

        self.save_raw(forecast_data, subdir="weather")

        count = 0
        weight = coords["pop_weight"]

        periods = forecast_data.get("properties", {}).get("periods", [])
        if not periods:
            logger.warning("[nws_weather] %s: no forecast periods returned", city)
            return 0

        for period in periods:
            if not period.get("isDaytime"):
                continue

            temp_f = float(period["temperature"])
            temp_c = (temp_f - 32) * 5 / 9
            hdd = max(0.0, HDD_CDD_BASE_F - temp_f)
            cdd = max(0.0, temp_f - HDD_CDD_BASE_F)

            metadata = json.dumps({
                "valid_to": period["endTime"],
                "name": period["name"],
            })

            for metric, val, unit in [
                ("temp_f",  temp_f,        "F"),
                ("temp_c",  temp_c,        "C"),
                ("hdd_65",  hdd,           "degree-days"),
                ("cdd_65",  cdd,           "degree-days"),
                ("hdd_wtd", hdd * weight,  "degree-days"),
                ("cdd_wtd", cdd * weight,  "degree-days"),
            ]:
                conn.execute(_INSERT_SQL, [
                    f"forecast_{metric}",
                    city,
                    period["startTime"],
                    now_str,
                    float(val),
                    unit,
                    metadata,
                ])
                count += 1

        return count
