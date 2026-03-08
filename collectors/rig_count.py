import io
import logging
import re
from datetime import datetime, timezone

import duckdb
import pandas as pd
import requests

from collectors.base import CollectorBase
from config.settings import DB_PATH

logger = logging.getLogger("collectors")

BHI_PAGE_URL = "https://rigcount.bakerhughes.com/na-rig-count"
BHI_BASE_URL = "https://rigcount.bakerhughes.com"

# BHI rate-limits non-browser UAs on the HTML page; use a generic browser UA for scraping.
# File downloads accept our app UA.
_PAGE_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
_FILE_HEADERS = {"User-Agent": "NatGasTerminal/1.0 (personal research tool)"}

# Weeks of history to retain from each file (~2 years; file contains ~114 weeks)
LOOKBACK_WEEKS = 104

_INSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency)
    VALUES ('baker_hughes', 'ng_rig_count', 'US', ?, ?, ?, 'rigs', 'weekly')
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
"""


class RigCountCollector(CollectorBase):
    source_name = "baker_hughes"

    def collect(self) -> dict:
        file_url = self._get_file_url()
        weekly = self._download_and_parse(file_url)

        conn = duckdb.connect(DB_PATH)
        now_str = datetime.now(timezone.utc).isoformat()
        count = 0
        try:
            for pub_date, rig_count in weekly.items():
                obs_time = pub_date.strftime("%Y-%m-%dT00:00:00Z")
                conn.execute(_INSERT_SQL, [obs_time, now_str, float(rig_count)])
                count += 1
        finally:
            conn.close()

        return {"status": "ok", "records_written": count}

    def _get_file_url(self) -> str:
        """Scrape the BHI page to find the current week's Excel download URL.

        BHI does not publish a stable direct URL — the file hash changes each week.
        The most recent weekly report is always the first static-files UUID link on
        the page. If BHI restructures the page, check the URL format here first.
        """
        resp = requests.get(BHI_PAGE_URL, headers=_PAGE_HEADERS, timeout=60)
        resp.raise_for_status()
        # Match /static-files/<uuid> — UUIDs are 36 chars (8-4-4-4-12 + hyphens)
        matches = re.findall(r"/static-files/([a-f0-9-]{36})", resp.text)
        if not matches:
            raise ValueError("No BHI static-files link found on rig count page — URL format may have changed")
        return f"{BHI_BASE_URL}/static-files/{matches[0]}"

    def _download_and_parse(self, url: str) -> pd.Series:
        resp = requests.get(url, headers=_FILE_HEADERS, timeout=300, stream=True)
        content = resp.content  # fully buffer before parsing
        resp.raise_for_status()
        self.save_raw({"url": url, "size_bytes": len(content)}, subdir="rig_count")

        xl = pd.ExcelFile(io.BytesIO(content))
        # NAM Weekly sheet: header on row 10 (0-indexed), data rows follow.
        # Columns of interest: Country, DrillFor, US_PublishDate, Rig Count Value.
        df = xl.parse("NAM Weekly", header=10)

        us_gas = df[
            (df["Country"] == "UNITED STATES") &
            (df["DrillFor"] == "Gas")
        ]
        weekly = (
            us_gas.groupby("US_PublishDate")["Rig Count Value"]
            .sum()
            .sort_index()
            .tail(LOOKBACK_WEEKS)
        )
        return weekly
