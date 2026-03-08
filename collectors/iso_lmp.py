"""
ISO LMP collector (Feature 3).

Fetches the most recent hub Locational Marginal Price (USD/MWh) for each
tracked ISO and stores it in facts_time_series with source_name='iso_lmp'.

Free public APIs (no registration required):
  NYISO  — mis.nyiso.com public CSV  (Zone J / NYC)
  MISO   — misoenergy.org BI Reporter JSON  (Illinois Hub)
  CAISO  — oasis.caiso.com OASIS API  (NP15 hub, returns ZIP+CSV)

Registration required (currently stubbed — set to skip):
  PJM    — dataminer2.pjm.com  (requires free API key)
  ERCOT  — api.ercot.com  (requires API key + certificate)
  ISO-NE — webservices.iso-ne.com  (requires free account)

When a per-ISO fetch fails the error is logged and that ISO is skipped;
other ISOs are not affected.

Runs every hour at :10.
"""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import datetime, timezone

import duckdb
import requests

from collectors.base import CollectorBase
from config.settings import DB_PATH

logger = logging.getLogger("collectors")

_UPSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency)
    VALUES ('iso_lmp', 'lmp_hub', ?, ?, ?, ?, 'USD/MWh', 'intraday')
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
"""


class ISOLMPCollector(CollectorBase):
    source_name = "iso_lmp"

    def collect(self) -> dict:
        conn = duckdb.connect(DB_PATH)
        now = datetime.now(timezone.utc)
        now_str = now.isoformat()
        written = 0

        try:
            for iso, lmp, obs_time in _fetch_all(now):
                conn.execute(_UPSERT_SQL, [iso, obs_time, now_str, lmp])
                written += 1
                logger.info("[iso_lmp] %s: %.2f USD/MWh @ %s", iso, lmp, obs_time)
        finally:
            conn.close()

        return {"status": "ok", "isos_written": written}


# ---------------------------------------------------------------------------
# Fetchers — one per free-API ISO
# ---------------------------------------------------------------------------

def _fetch_all(now: datetime) -> list[tuple[str, float, str]]:
    """Return list of (iso, lmp_usd_mwh, obs_time_iso8601) tuples."""
    results: list[tuple[str, float, str]] = []

    for fetcher in (_fetch_nyiso, _fetch_miso, _fetch_caiso):
        try:
            row = fetcher(now)
            if row:
                results.append(row)
        except Exception as e:
            logger.warning("[iso_lmp] %s fetch failed: %s", fetcher.__name__, e)

    return results


def _fetch_nyiso(now: datetime) -> tuple[str, float, str] | None:
    """
    NYISO public real-time zonal LMP CSV.
    URL: https://mis.nyiso.com/public/csv/realtime/{YYYYMMDD}realtime_zone.csv
    Returns the latest entry for Zone J (NYC) — the most gas-heavy load zone.
    """
    date_str = now.strftime("%Y%m%d")
    url = f"https://mis.nyiso.com/public/csv/realtime/{date_str}realtime_zone.csv"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    # CSV columns: Time Stamp, Name, PTID, Integrated Real-Time LMP, ...
    lines = resp.text.strip().splitlines()
    if len(lines) < 2:
        return None

    header = [h.strip() for h in lines[0].split(",")]
    name_idx = header.index("Name")
    lmp_idx  = header.index("Integrated Real-Time LMP")
    ts_idx   = header.index("Time Stamp")

    # Find the last Zone J row (newest first isn't guaranteed — scan all)
    best_ts = None
    best_lmp = None
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) <= max(name_idx, lmp_idx, ts_idx):
            continue
        if parts[name_idx].strip() != "N.Y.C.":
            continue
        try:
            lmp = float(parts[lmp_idx].strip())
        except ValueError:
            continue
        ts_raw = parts[ts_idx].strip()  # e.g. "03/08/2026 14:55:00"
        try:
            dt = datetime.strptime(ts_raw, "%m/%d/%Y %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if best_ts is None or dt > best_ts:
            best_ts = dt
            best_lmp = lmp

    if best_ts is None or best_lmp is None:
        return None

    return ("NYISO", best_lmp, best_ts.isoformat())


def _fetch_miso(now: datetime) -> tuple[str, float, str] | None:
    """
    MISO BI Reporter real-time LMP (Illinois Hub, no registration required).
    Endpoint returns JSON with hub-level LMP values.
    """
    url = (
        "https://api.misoenergy.org/MISORTWDBIReporter/services/"
        "MISORTWDDataBroker/getLMPConsolidatedTable"
    )
    params = {"colType": "LMPZ", "requestType": "getLatestData"}
    resp = requests.get(url, params=params, timeout=30,
                        headers={"Accept": "application/json"})
    resp.raise_for_status()
    data = resp.json()

    # Response: {"LMPData": {"Data": [{"name": "ILLINOIS HUB", "lmp": "25.34", ...}, ...]}}
    rows = (
        data.get("LMPData", {}).get("Data", [])
        or data.get("lmpData", {}).get("data", [])
    )
    for row in rows:
        name = (row.get("name") or row.get("Name") or "").upper()
        if "ILLINOIS" not in name:
            continue
        try:
            lmp = float(str(row.get("lmp") or row.get("LMP") or "").replace(",", ""))
        except (ValueError, TypeError):
            continue
        obs_time = now.strftime("%Y-%m-%dT%H:00:00Z")
        return ("MISO", lmp, obs_time)

    return None


def _fetch_caiso(now: datetime) -> tuple[str, float, str] | None:
    """
    CAISO OASIS API — NP15 hub (Northern California), real-time market.
    Returns a ZIP file containing a CSV; no API key required.
    """
    # Request the last 1-hour window ending now.
    # OASIS timestamps use America/Los_Angeles (PST/PDT) but the API
    # accepts UTC if we format as YYYYMMDDTHHmmZ.
    end_dt   = now.replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt.replace(hour=max(end_dt.hour - 1, 0))

    fmt = "%Y%m%dT%H%MZ"
    url = "http://oasis.caiso.com/oasisapi/SingleZip"
    params = {
        "queryname":     "PRC_INTVL_LMP",
        "startdatetime": start_dt.strftime(fmt),
        "enddatetime":   end_dt.strftime(fmt),
        "version":       "1",
        "market_run_id": "RTM",
        "node":          "TH_NP15_GEN-APND",
        "resultformat":  "6",  # CSV
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()

    if resp.headers.get("Content-Type", "").startswith("application/zip") or resp.content[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
            if not csv_name:
                return None
            csv_text = zf.read(csv_name).decode("utf-8")
    else:
        csv_text = resp.text

    lines = csv_text.strip().splitlines()
    if len(lines) < 2:
        return None

    # Find the LMP column (MW column name varies — look for "MW" header)
    header = [h.strip() for h in lines[0].split(",")]
    lmp_idx = next((i for i, h in enumerate(header) if h.upper() in ("MW", "LMP_PRC", "LMP")), None)
    ts_idx  = next((i for i, h in enumerate(header) if "INTERVALSTARTTIME" in h.upper() or h.upper() == "STARTDATETIME"), None)

    if lmp_idx is None:
        logger.debug("[iso_lmp] CAISO CSV headers: %s", header)
        return None

    # Grab the last valid row
    best_ts = None
    best_lmp = None
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) <= lmp_idx:
            continue
        try:
            lmp = float(parts[lmp_idx].strip())
        except ValueError:
            continue
        if ts_idx is not None and len(parts) > ts_idx:
            ts_raw = parts[ts_idx].strip().strip('"')
            try:
                # CAISO uses ISO 8601 in the CSV: "2026-03-08T14:05:00-08:00"
                dt = datetime.fromisoformat(ts_raw).astimezone(timezone.utc)
                if best_ts is None or dt > best_ts:
                    best_ts = dt
                    best_lmp = lmp
            except ValueError:
                best_lmp = lmp
                best_ts = now
        else:
            best_lmp = lmp
            best_ts = now

    if best_lmp is None:
        return None

    return ("CAISO", best_lmp, best_ts.isoformat())
