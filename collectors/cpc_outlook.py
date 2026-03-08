import io
import logging
import zipfile
from datetime import datetime, timezone

import duckdb
import requests
import shapefile

from collectors.base import CollectorBase
from collectors.weather import WEATHER_POINTS
from config.settings import DB_PATH

logger = logging.getLogger("collectors")

HEADERS = {"User-Agent": "NatGasTerminal/1.0 (personal research tool)"}

# Stable _latest.zip URLs — NOAA updates these in place each day.
# Dated archives are also available at the same FTP path (e.g. 610temp_20260307.zip).
CPC_URLS: dict[str, str] = {
    "6_10": "https://ftp.cpc.ncep.noaa.gov/GIS/us_tempprcpfcst/610temp_latest.zip",
    "8_14": "https://ftp.cpc.ncep.noaa.gov/GIS/us_tempprcpfcst/814temp_latest.zip",
}

# Probability assigned to non-dominant categories when a city falls inside a polygon
# (CPC only publishes the dominant category probability; others are treated as EC).
EC_PROB = 33.33

_INSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency)
    VALUES ('cpc', ?, ?, ?, ?, ?, '%', 'daily')
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
"""


class CPCOutlookCollector(CollectorBase):
    source_name = "cpc_outlook"

    def collect(self) -> dict:
        conn = duckdb.connect(DB_PATH)
        now_str = datetime.now(timezone.utc).isoformat()
        total = 0

        try:
            for window, url in CPC_URLS.items():
                try:
                    total += self._collect_window(conn, window, url, now_str)
                except Exception as e:
                    logger.warning("[cpc_outlook] %s failed: %s", window, e)
        finally:
            conn.close()

        return {"status": "ok", "records_written": total}

    def _collect_window(self, conn, window: str, url: str, now_str: str) -> int:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        self.save_raw({"url": url, "size_bytes": len(resp.content)}, subdir="cpc_outlook")

        shapes, records, fcst_date = self._parse_zip(resp.content, window)

        obs_time = f"{fcst_date}T00:00:00Z"
        count = 0

        for city, coords in WEATHER_POINTS.items():
            lon, lat = coords["lon"], coords["lat"]
            match = self._find_polygon(lon, lat, shapes, records)

            # Derive prob_below, prob_above for this city.
            # CPC only publishes the dominant category probability.
            # Non-dominant categories are treated as EC (equal chances = 33.33%).
            if match is None:
                # City falls outside all classified polygons → equal chances
                prob_below = EC_PROB
                prob_above = EC_PROB
            elif match["cat"] == "Below":
                prob_below = match["prob"]
                prob_above = EC_PROB
            elif match["cat"] == "Above":
                prob_above = match["prob"]
                prob_below = EC_PROB
            else:  # Normal or unexpected category
                prob_below = EC_PROB
                prob_above = EC_PROB

            for series_name, value in [
                (f"cpc_{window}_prob_below", prob_below),
                (f"cpc_{window}_prob_above", prob_above),
            ]:
                conn.execute(_INSERT_SQL, [series_name, city, obs_time, now_str, value])
                count += 1

        return count

    def _parse_zip(self, content: bytes, window: str):
        """Extract shapes and records from a CPC temperature ZIP."""
        prefix = "610temp_latest" if window == "6_10" else "814temp_latest"
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            shp_bytes = io.BytesIO(z.read(f"{prefix}.shp"))
            dbf_bytes = io.BytesIO(z.read(f"{prefix}.dbf"))

        sf = shapefile.Reader(shp=shp_bytes, dbf=dbf_bytes)
        shapes = sf.shapes()
        records = sf.records()

        fcst_date = None
        if records:
            raw = records[0]["Fcst_Date"]
            # DBF date fields come as datetime.date or string YYYYMMDD
            if hasattr(raw, "strftime"):
                fcst_date = raw.strftime("%Y-%m-%d")
            else:
                s = str(raw)
                fcst_date = f"{s[:4]}-{s[4:6]}-{s[6:]}"

        if not fcst_date:
            fcst_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return shapes, records, fcst_date

    @staticmethod
    def _find_polygon(lon: float, lat: float, shapes, records) -> dict | None:
        """Return the first polygon record containing (lon, lat), or None."""
        for shp_geom, rec in zip(shapes, records):
            for i, part_start in enumerate(shp_geom.parts):
                part_end = (
                    shp_geom.parts[i + 1]
                    if i + 1 < len(shp_geom.parts)
                    else len(shp_geom.points)
                )
                pts = shp_geom.points[part_start:part_end]
                if _point_in_polygon(lon, lat, pts):
                    return {"cat": rec["Cat"], "prob": float(rec["Prob"])}
        return None


def _point_in_polygon(x: float, y: float, points: list) -> bool:
    """Ray-casting point-in-polygon test."""
    n = len(points)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = points[i]
        xj, yj = points[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside
