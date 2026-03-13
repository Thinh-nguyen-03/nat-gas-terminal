"""
AIS LNG vessel tracking collector (Feature 2).

Uses AISstream.io WebSocket API (free tier) to track LNG tankers near each
US LNG export terminal.  Connects once per run, listens for COLLECT_SECONDS
seconds, accumulates PositionReport + ShipStaticData messages, then
classifies vessels and writes terminal berth counts to facts_time_series.

Sign up (no AIS receiver required): https://aisstream.io
Set AISSTREAM_API_KEY in .env.

Classification heuristic:
  loading  — nav status 5 (moored) OR speed < 0.5 kn within 0.05° of berth
  anchored — nav status 1 (at anchor) OR speed < 2 kn within terminal bbox
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
import websockets

from collectors.base import CollectorBase
from config.settings import AISSTREAM_API_KEY, DB_PATH

logger = logging.getLogger("collectors")

_WS_URL         = "wss://stream.aisstream.io/v0/stream"
_COLLECT_SECS   = 90   # listen duration per run

# AIS ship type 84 = Liquefied gas tanker (LNG / LPG)
_LNG_VESSEL_TYPES = {84}

_TERMINALS: list[dict] = [
    {"name": "Sabine Pass",    "lat": 29.726, "lon": -93.872, "box": 0.10},
    {"name": "Corpus Christi", "lat": 27.636, "lon": -97.325, "box": 0.10},
    {"name": "Freeport LNG",   "lat": 28.861, "lon": -95.315, "box": 0.10},
    {"name": "Cameron LNG",    "lat": 29.817, "lon": -93.292, "box": 0.10},
    {"name": "Calcasieu Pass", "lat": 29.809, "lon": -93.347, "box": 0.15},
    {"name": "Cove Point",     "lat": 38.406, "lon": -76.540, "box": 0.10},
    {"name": "Elba Island",    "lat": 32.082, "lon": -81.102, "box": 0.10},
]

# Two subscription boxes (Gulf Coast + East Coast) to minimise overhead
_BBOXES = [
    # Gulf Coast: covers Sabine Pass, Corpus Christi, Freeport, Cameron, Calcasieu
    [[27.0, -98.0], [30.5, -92.0]],
    # East Coast: covers Cove Point and Elba Island
    [[31.5, -82.0], [39.0, -75.0]],
]

_LOADING_SPEED_KN  = 0.5
_ANCHORED_SPEED_KN = 2.0
_NAV_MOORED = 5
_NAV_ANCHOR = 1

_KNOWN_TANKERS_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "lng_vessels" / "known_tankers.json"
)


def _load_known_mmsis() -> set[int]:
    try:
        data = json.loads(_KNOWN_TANKERS_PATH.read_text())
        return {int(t["mmsi"]) for t in data.get("tankers", [])}
    except Exception:
        return set()


_KNOWN_MMSIS: set[int] = _load_known_mmsis()


class LNGVesselsCollector(CollectorBase):
    source_name = "ais"

    def collect(self) -> dict:
        if not AISSTREAM_API_KEY:
            raise RuntimeError(
                "AISSTREAM_API_KEY not set — sign up at aisstream.io and add it to .env"
            )

        vessels = asyncio.run(_collect_async(AISSTREAM_API_KEY))
        logger.info("[ais] collected %d unique vessels from AISstream.io", len(vessels))

        counts  = _classify_vessels(vessels)
        written = _write_counts(counts)

        return {"status": "ok", "vessels_seen": len(vessels), "rows_written": written}


async def _collect_async(api_key: str) -> dict[int, dict]:
    """Open a WebSocket to AISstream.io and collect for _COLLECT_SECS seconds."""
    vessels: dict[int, dict] = {}

    sub = {
        "APIKey": api_key,
        "BoundingBoxes": _BBOXES,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    loop = asyncio.get_event_loop()
    deadline = loop.time() + _COLLECT_SECS

    try:
        async with websockets.connect(_WS_URL, ping_interval=20, open_timeout=15) as ws:
            await ws.send(json.dumps(sub))
            logger.info("[ais] AISstream.io connected — listening %ds", _COLLECT_SECS)

            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 5.0))
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed as e:
                    logger.warning("[ais] connection closed: %s", e)
                    break

                try:
                    _process_message(json.loads(raw), vessels)
                except (json.JSONDecodeError, KeyError, ValueError):
                    continue

    except (OSError, websockets.exceptions.WebSocketException) as e:
        logger.error("[ais] WebSocket error: %s", e)

    return vessels


def _process_message(msg: dict, vessels: dict[int, dict]) -> None:
    msg_type = msg.get("MessageType", "")
    meta     = msg.get("MetaData", {})

    raw_mmsi = meta.get("MMSI") or meta.get("Mmsi")
    if not raw_mmsi:
        return
    mmsi = int(raw_mmsi)

    if mmsi not in vessels:
        vessels[mmsi] = {
            "mmsi":      mmsi,
            "ship_type": None,
            "lat":       None,
            "lon":       None,
            "sog":       None,
            "nav":       None,
            "name":      "",
        }

    v = vessels[mmsi]
    ship_name = (meta.get("ShipName") or "").strip()
    if ship_name:
        v["name"] = ship_name

    if msg_type == "PositionReport":
        pr = msg.get("Message", {}).get("PositionReport", {})
        lat = pr.get("Latitude") if pr.get("Latitude") is not None else meta.get("latitude")
        lon = pr.get("Longitude") if pr.get("Longitude") is not None else meta.get("longitude")
        if lat is not None:
            v["lat"] = float(lat)
        if lon is not None:
            v["lon"] = float(lon)
        sog = pr.get("Sog")
        if sog is not None:
            v["sog"] = float(sog)
        nav = pr.get("NavigationalStatus")
        if nav is not None:
            v["nav"] = int(nav)

    elif msg_type == "ShipStaticData":
        sd = msg.get("Message", {}).get("ShipStaticData", {})
        ship_type = sd.get("Type")
        if ship_type is not None:
            v["ship_type"] = int(ship_type)


def _classify_vessels(vessels: dict[int, dict]) -> dict[str, dict[str, int]]:
    """Return {terminal_name: {"loading": n, "anchored": n}}."""
    counts: dict[str, dict[str, int]] = {
        t["name"]: {"loading": 0, "anchored": 0}
        for t in _TERMINALS
    }

    for v in vessels.values():
        if not _is_lng_tanker(v):
            continue

        lat = v["lat"]
        lon = v["lon"]
        if lat is None or lon is None:
            continue

        sog_kn = v["sog"] if v["sog"] is not None else 99.0
        nav    = v["nav"] if v["nav"] is not None else -1

        for terminal in _TERMINALS:
            dist = _haversine_deg(lat, lon, terminal["lat"], terminal["lon"])
            if dist > terminal["box"]:
                continue

            is_berth   = dist <= 0.05
            is_moored  = nav == _NAV_MOORED or (sog_kn < _LOADING_SPEED_KN and is_berth)
            is_anchored = nav == _NAV_ANCHOR or (sog_kn < _ANCHORED_SPEED_KN and not is_moored)

            if is_moored:
                counts[terminal["name"]]["loading"]  += 1
                logger.info("[ais] %s — LOADING: %s (mmsi=%d sog=%.1f nav=%d)",
                            terminal["name"], v["name"], v["mmsi"], sog_kn, nav)
            elif is_anchored:
                counts[terminal["name"]]["anchored"] += 1
                logger.info("[ais] %s — ANCHORED: %s (mmsi=%d sog=%.1f nav=%d)",
                            terminal["name"], v["name"], v["mmsi"], sog_kn, nav)

    return counts


def _write_counts(counts: dict[str, dict[str, int]]) -> int:
    conn    = duckdb.connect(DB_PATH)
    now_str = datetime.now(timezone.utc).isoformat()
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
                conn.execute(sql, [
                    f"lng_ships_{series}",
                    terminal_name,
                    now_str, now_str,
                    float(count),
                ])
                written += 1
    finally:
        conn.close()

    return written


def _is_lng_tanker(v: dict) -> bool:
    return v["ship_type"] in _LNG_VESSEL_TYPES or v["mmsi"] in _KNOWN_MMSIS


def _haversine_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate great-circle distance in degrees (cheap, good for < 50 km)."""
    dlat = abs(lat1 - lat2)
    dlon = abs(lon1 - lon2) * math.cos(math.radians((lat1 + lat2) / 2))
    return math.sqrt(dlat ** 2 + dlon ** 2)
