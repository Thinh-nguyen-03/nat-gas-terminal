"""
AIS LNG vessel tracking collector (Feature 2).

Polls AISHub (free tier) every 30 minutes to detect LNG tankers (vessel
type 84) near each US LNG export terminal.  Vessel counts are written to
facts_time_series with source_name='ais'.

AISHub free account: 1 request/minute rate limit, ≤100 vessels returned.
Register at https://www.aishub.net/  — set AIS_HUB_USERNAME in .env.

Bounding box: a single query covering the US Gulf Coast + East Coast where
all 7 active LNG export terminals are located, to stay within rate limit.

Classification heuristic:
  loading  — AIS nav status 5 (moored) OR speed < 0.5 kn, within 0.05° of berth
  anchored — speed < 2 kn, within terminal bounding box but not at berth

If AIS_HUB_USERNAME is not set, the collector records a health error and exits.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests

from collectors.base import CollectorBase
from config.settings import AIS_HUB_USERNAME, DB_PATH

logger = logging.getLogger("collectors")

_AIS_URL = "https://data.aishub.net/ws.php"

# LNG tanker vessel types (AIS ship type codes)
_LNG_VESSEL_TYPES = {84}  # 84 = LNG tanker

# Each terminal: (name, berth_lat, berth_lon, bbox half-width in degrees)
_TERMINALS: list[dict] = [
    {"name": "Sabine Pass",    "lat": 29.726, "lon": -93.872, "box": 0.10},
    {"name": "Corpus Christi", "lat": 27.636, "lon": -97.325, "box": 0.10},
    {"name": "Freeport LNG",   "lat": 28.861, "lon": -95.315, "box": 0.10},
    {"name": "Cameron LNG",    "lat": 29.817, "lon": -93.292, "box": 0.10},
    {"name": "Calcasieu Pass", "lat": 29.809, "lon": -93.347, "box": 0.15},
    {"name": "Cove Point",     "lat": 38.406, "lon": -76.540, "box": 0.10},
    {"name": "Elba Island",    "lat": 32.082, "lon": -81.102, "box": 0.10},
]

# Single bounding box covering all terminals (Gulf Coast + East Coast)
_QUERY_BBOX = {
    "latmin": 27.0,
    "latmax": 39.0,
    "lonmin": -98.0,
    "lonmax": -75.0,
}

# Speed (knots) thresholds for classification
_LOADING_SPEED_KN  = 0.5   # at berth, engine-idle/mooring
_ANCHORED_SPEED_KN = 2.0   # slow drift / at anchor

# Nav status codes (AIS field NAVSTAT)
_NAV_MOORED  = 5
_NAV_ANCHOR  = 1

# Known tanker MMSI set (loaded once from seed file)
_KNOWN_TANKERS_PATH = Path(__file__).resolve().parent.parent / "data" / "lng_vessels" / "known_tankers.json"

def _load_known_mmsis() -> set[str]:
    try:
        data = json.loads(_KNOWN_TANKERS_PATH.read_text())
        return {t["mmsi"] for t in data.get("tankers", [])}
    except Exception:
        return set()

_KNOWN_MMSIS: set[str] = _load_known_mmsis()


class LNGVesselsCollector(CollectorBase):
    source_name = "ais"

    def collect(self) -> dict:
        if not AIS_HUB_USERNAME:
            raise RuntimeError(
                "AIS_HUB_USERNAME not set — register at aishub.net and set it in .env"
            )

        vessels = self._fetch_vessels()
        counts  = self._classify_vessels(vessels)
        written = self._write_counts(counts)

        return {"status": "ok", "vessels_seen": len(vessels), "rows_written": written}

    def _fetch_vessels(self) -> list[dict]:
        params = {
            "username": AIS_HUB_USERNAME,
            "format":   "1",
            "output":   "json",
            "compress": "0",
            **_QUERY_BBOX,
        }
        resp = requests.get(_AIS_URL, params=params, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        # AISHub returns a list: [{"ERROR": ...}] on failure, or list of vessel dicts
        if isinstance(data, list) and data and "ERROR" in data[0]:
            raise RuntimeError(f"AISHub error: {data[0]['ERROR']}")

        vessels = data if isinstance(data, list) else data.get("vessels", [])
        self.save_raw(vessels, subdir="lng_vessels")
        return vessels

    def _classify_vessels(self, vessels: list[dict]) -> dict[str, dict[str, int]]:
        """Return {terminal_name: {"loading": n, "anchored": n}}."""
        counts: dict[str, dict[str, int]] = {
            t["name"]: {"loading": 0, "anchored": 0}
            for t in _TERMINALS
        }

        for vessel in vessels:
            if not _is_lng_tanker(vessel):
                continue

            lat  = _safe_float(vessel.get("LATITUDE")  or vessel.get("LAT"))
            lon  = _safe_float(vessel.get("LONGITUDE") or vessel.get("LON"))
            sog  = _safe_float(vessel.get("SOG") or vessel.get("SPEED"), default=99.0)
            # AISHub returns SOG × 10 (tenths of knots)
            sog_kn = sog / 10.0 if sog > 10 else sog
            nav  = int(vessel.get("NAVSTAT") or vessel.get("STATUS") or -1)

            if lat is None or lon is None:
                continue

            for terminal in _TERMINALS:
                dist = _haversine_deg(lat, lon, terminal["lat"], terminal["lon"])
                if dist > terminal["box"]:
                    continue

                is_berth = dist <= 0.05  # within ~5 km of berth coordinates
                is_moored = nav in (_NAV_MOORED,) or (sog_kn < _LOADING_SPEED_KN and is_berth)
                is_anchored = nav == _NAV_ANCHOR or (sog_kn < _ANCHORED_SPEED_KN and not is_moored)

                if is_moored:
                    counts[terminal["name"]]["loading"] += 1
                elif is_anchored:
                    counts[terminal["name"]]["anchored"] += 1

        return counts

    def _write_counts(self, counts: dict[str, dict[str, int]]) -> int:
        conn = duckdb.connect(DB_PATH)
        now = datetime.now(timezone.utc)
        now_str = now.isoformat()
        written = 0

        sql = """
            INSERT INTO facts_time_series
                (source_name, series_name, region, observation_time,
                 ingest_time, value, unit, frequency)
            VALUES ('ais', ?, ?, ?, ?, ?, 'vessels', 'intraday')
            ON CONFLICT (source_name, series_name, region, observation_time)
            DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
        """
        try:
            for terminal_name, ship_counts in counts.items():
                for series, count in ship_counts.items():
                    series_name = f"lng_ships_{series}"  # lng_ships_loading / lng_ships_anchored
                    conn.execute(sql, [series_name, terminal_name, now_str, now_str, float(count)])
                    written += 1
                    if count > 0:
                        logger.info("[ais] %s %s: %d ships", terminal_name, series, count)
        finally:
            conn.close()

        return written


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_lng_tanker(vessel: dict) -> bool:
    """Return True if vessel type indicates LNG tanker or MMSI is in seed list."""
    vtype = int(vessel.get("TYPE") or vessel.get("SHIPTYPE") or 0)
    mmsi  = str(vessel.get("MMSI") or "")
    return vtype in _LNG_VESSEL_TYPES or mmsi in _KNOWN_MMSIS


def _safe_float(val, default=None) -> float | None:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _haversine_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate great-circle distance in degrees (cheap, good enough for < 50 km)."""
    dlat = abs(lat1 - lat2)
    dlon = abs(lon1 - lon2) * math.cos(math.radians((lat1 + lat2) / 2))
    return math.sqrt(dlat ** 2 + dlon ** 2)
