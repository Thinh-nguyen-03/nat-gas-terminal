import io
import logging
import zipfile
from datetime import datetime, timezone

import duckdb
import pandas as pd
import requests

from collectors.base import CollectorBase
from config.settings import DB_PATH

logger = logging.getLogger("collectors")

# NYMEX Natural Gas commodity code in the CFTC disaggregated futures report
NG_COMMODITY_CODE = "023651"

# CFTC publishes annual ZIP files. URL format has been stable but verify annually.
CFTC_URL = "https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"

REPORT_DATE_COLUMN = "Report_Date_as_YYYY-MM-DD"

# Mapping from our series name suffix to the CFTC CSV column name
MM_POSITION_COLUMNS: dict[str, str] = {
    "mm_long":       "M_Money_Positions_Long_All",
    "mm_short":      "M_Money_Positions_Short_All",
    "mm_spreading":  "M_Money_Positions_Spread_All",
    "prod_long":     "Prod_Merc_Positions_Long_All",
    "prod_short":    "Prod_Merc_Positions_Short_All",
    "swap_long":     "Swap_Positions_Long_All",
    "swap_short":    "Swap_Positions_Short_All",
    "open_interest": "Open_Interest_All",
}

_INSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency)
    VALUES ('cftc', ?, 'US', ?, ?, ?, 'contracts', 'weekly')
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
"""


class CFTCCollector(CollectorBase):
    source_name = "cftc_cot"

    def collect(self) -> dict:
        year = datetime.now(timezone.utc).year
        url = CFTC_URL.format(year=year)

        resp = requests.get(
            url,
            timeout=120,
            headers={"User-Agent": "NatGasTerminal/1.0"},
        )
        resp.raise_for_status()

        df = self._parse_zip(resp.content)
        ng = df[df["CFTC_Contract_Market_Code"] == NG_COMMODITY_CODE].copy()

        if ng.empty:
            logger.warning("[cftc] no NG rows found in disaggregated report")
            return {"status": "ok", "records_written": 0}

        self.save_raw(ng.tail(10).to_dict("records"), subdir="cftc")

        conn = duckdb.connect(DB_PATH)
        records_written = 0
        now_str = datetime.now(timezone.utc).isoformat()

        try:
            for _, row in ng.iterrows():
                report_date = str(row.get(REPORT_DATE_COLUMN, "")).strip()
                if len(report_date) != 10:
                    continue

                obs_time = f"{report_date}T00:00:00Z"
                for series_suffix, col_name in MM_POSITION_COLUMNS.items():
                    value = row.get(col_name)
                    if value is None or (isinstance(value, float) and value != value):
                        continue
                    conn.execute(_INSERT_SQL, [
                        f"cot_{series_suffix}",
                        obs_time,
                        now_str,
                        float(value),
                    ])
                    records_written += 1
        finally:
            conn.close()

        return {"status": "ok", "records_written": records_written}

    @staticmethod
    def _parse_zip(content: bytes) -> pd.DataFrame:
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            fname = z.namelist()[0]
            with z.open(fname) as f:
                return pd.read_csv(f)
