"""
ISO LMP collector (Feature 3).

Fetches the most recent hub Locational Marginal Price (USD/MWh) for each
tracked ISO and stores it in facts_time_series with source_name='iso_lmp'.

Free public APIs (no registration required):
  NYISO  — mis.nyiso.com public CSV  (Zone J / NYC)
  MISO   — misoenergy.org BI Reporter JSON  (Illinois Hub)
  CAISO  — oasis.caiso.com OASIS API  (NP15 hub, returns ZIP+CSV)

Registration required:
  PJM    — dataminer2.pjm.com  (free API key; set PJM_API_KEY in .env)
  ERCOT  — apiexplorer.ercot.com  (free account; set ERCOT_SUBSCRIPTION_KEY,
           ERCOT_USERNAME, ERCOT_PASSWORD in .env)
  ISO-NE — webservices.iso-ne.com  (free account; set ISO_NE_USERNAME,
           ISO_NE_PASSWORD in .env)

When a per-ISO fetch fails the error is logged and that ISO is skipped;
other ISOs are not affected.

Runs every hour at :10.
"""

from __future__ import annotations

import io
import logging
import time
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

    for fetcher in (_fetch_nyiso, _fetch_miso, _fetch_caiso,
                    _fetch_pjm, _fetch_ercot, _fetch_isone):
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


# ---------------------------------------------------------------------------
# PJM — DataMiner2 API (requires PJM_API_KEY)
# ---------------------------------------------------------------------------

def _fetch_pjm(now: datetime) -> tuple[str, float, str] | None:
    from config.settings import PJM_API_KEY
    if not PJM_API_KEY:
        return None

    resp = requests.get(
        "https://dataminer2.pjm.com/feed/rt_hrl_lmps/json",
        params={
            "rowCount":  1,
            "startRow":  1,
            "pnode_name": "WESTERN HUB",
            "sort":      "datetime_beginning_utc",
            "order":     "desc",
        },
        headers={"Ocp-Apim-Subscription-Key": PJM_API_KEY},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()

    rows   = payload.get("data", [])
    fields = payload.get("fields", [])
    if not rows:
        return None

    row = rows[0]
    # DataMiner2 returns either a list of dicts or a fields+data list-of-lists
    if fields and isinstance(row, (list, tuple)):
        field_names = [f["name"] for f in fields]
        row = dict(zip(field_names, row))

    lmp     = float(row["total_lmp_rt"])
    ts_raw  = str(row.get("datetime_beginning_utc", ""))
    try:
        dt = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        obs_time = dt.isoformat()
    except ValueError:
        obs_time = now.strftime("%Y-%m-%dT%H:00:00Z")

    return ("PJM", lmp, obs_time)


# ---------------------------------------------------------------------------
# ERCOT — public API (requires ERCOT_SUBSCRIPTION_KEY + username/password)
# ---------------------------------------------------------------------------

# Azure B2C ERCOT public app — client_id is a published constant, not a secret
_ERCOT_CLIENT_ID   = "fec253ea-0d06-4272-a5e6-b478baeecd70"
_ERCOT_TOKEN_CACHE: tuple[str, float] | None = None


def _get_ercot_token(username: str, password: str) -> str:
    global _ERCOT_TOKEN_CACHE
    now_ts = time.time()
    if _ERCOT_TOKEN_CACHE and now_ts < _ERCOT_TOKEN_CACHE[1] - 60:
        return _ERCOT_TOKEN_CACHE[0]

    resp = requests.post(
        "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/"
        "B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token",
        data={
            "username":      username,
            "password":      password,
            "grant_type":    "password",
            "scope":         f"openid {_ERCOT_CLIENT_ID} offline_access",
            "client_id":     _ERCOT_CLIENT_ID,
            "response_type": "id_token",
        },
        timeout=30,
    )
    resp.raise_for_status()
    token_data = resp.json()
    token = token_data.get("access_token") or token_data.get("id_token")
    if not token:
        raise ValueError("ERCOT auth response missing token field")
    _ERCOT_TOKEN_CACHE = (token, now_ts + token_data.get("expires_in", 3600))
    return token


def _fetch_ercot(now: datetime) -> tuple[str, float, str] | None:
    from config.settings import ERCOT_SUBSCRIPTION_KEY, ERCOT_USERNAME, ERCOT_PASSWORD
    if not (ERCOT_SUBSCRIPTION_KEY and ERCOT_USERNAME and ERCOT_PASSWORD):
        return None

    token    = _get_ercot_token(ERCOT_USERNAME, ERCOT_PASSWORD)
    today    = now.strftime("%Y-%m-%d")

    resp = requests.get(
        "https://api.ercot.com/api/public-reports/np6-905-cd/spp_node_zone_hub",
        params={
            "deliveryDateFrom": today,
            "deliveryDateTo":   today,
            "settlementPoint":  "HB_NORTH",
        },
        headers={
            "Authorization":             f"Bearer {token}",
            "Ocp-Apim-Subscription-Key": ERCOT_SUBSCRIPTION_KEY,
        },
        timeout=30,
    )
    resp.raise_for_status()
    rows = resp.json().get("data", [])
    if not rows:
        return None

    # Take the last row (highest delivery hour available so far today)
    row = rows[-1]

    # Field names vary; try the most common candidates
    price = None
    for key in ("Settlement Point Price", "settlementPointPrice", "spp"):
        if key in row:
            price = float(row[key])
            break
    if price is None:
        return None

    delivery_date = row.get("Delivery Date") or row.get("deliveryDate") or today
    delivery_hour = row.get("Delivery Hour") or row.get("deliveryHour") or now.hour
    try:
        dt = datetime.strptime(
            f"{delivery_date} {int(delivery_hour):02d}:00", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=timezone.utc)
        obs_time = dt.isoformat()
    except (ValueError, TypeError):
        obs_time = now.strftime("%Y-%m-%dT%H:00:00Z")

    return ("ERCOT", price, obs_time)


# ---------------------------------------------------------------------------
# ISO-NE — webservices API (requires ISO_NE_USERNAME + ISO_NE_PASSWORD)
# ---------------------------------------------------------------------------

def _fetch_isone(now: datetime) -> tuple[str, float, str] | None:
    from config.settings import ISO_NE_USERNAME, ISO_NE_PASSWORD
    if not (ISO_NE_USERNAME and ISO_NE_PASSWORD):
        return None

    # Location 4000 = .H.INTERNALHUB (ISO-NE internal hub)
    resp = requests.get(
        "https://webservices.iso-ne.com/api/v1.1/hourlylmp/rt/prelim/current/location/4000.json",
        auth=(ISO_NE_USERNAME, ISO_NE_PASSWORD),
        headers={"Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    lmps = (
        data.get("HourlyRtPrelimLmps", {}).get("HourlyRtPrelimLmp")
        or data.get("hourlylmps", {}).get("hourlylmp")
        or []
    )
    if not lmps:
        return None

    entry = lmps[-1] if isinstance(lmps, list) else lmps

    lmp_total = (
        entry.get("LmpTotal")
        or entry.get("lmpTotal")
        or entry.get("TotalLmp")
    )
    if lmp_total is None:
        return None
    lmp = float(lmp_total)

    begin_raw = entry.get("BeginDate") or entry.get("beginDate") or ""
    try:
        dt = datetime.fromisoformat(
            begin_raw.replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        obs_time = dt.isoformat()
    except (ValueError, AttributeError):
        obs_time = now.strftime("%Y-%m-%dT%H:00:00Z")

    return ("ISO-NE", lmp, obs_time)
