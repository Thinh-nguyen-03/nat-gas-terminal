import io
import logging
from datetime import datetime, timezone

import duckdb
import requests
import xlrd

from collectors.base import CollectorBase
from config.settings import DB_PATH

logger = logging.getLogger("collectors")

# EIA publishes one XLS per report year.  Each sheet contains the 5-year
# avg/max/min for every Thursday report date in that year.
# URL is stable and always reflects the latest published file.
EIA_STATS_URL = "https://ir.eia.gov/ngs/ngsstats.xls"

# Column indices within each sheet (0-based).
# Row 2 is the column header; row 3+ are weekly data rows.
_COL_DATE = 0

# 5-year Average: cols 1-8  (East, Midwest, Mountain, Pacific, SC, SC-Salt, SC-NonSalt, Total)
# 5-year Maximum: cols 9-16 (same order)
# 5-year Minimum: cols 17-24 (same order)
_REGIONS: list[tuple[str, int, int, int]] = [
    # (region_name, avg_col, max_col, min_col)
    ("total",         8,  16, 24),
    ("east",          1,   9, 17),
    ("midwest",       2,  10, 18),
    ("mountain",      3,  11, 19),
    ("pacific",       4,  12, 20),
    ("south_central", 5,  13, 21),
]

_INSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency)
    VALUES ('eia_storage_stats', ?, ?, ?, ?, ?, 'Bcf', 'weekly')
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
"""


class EIAStorageStatsCollector(CollectorBase):
    source_name = "eia_storage_stats"

    def collect(self) -> dict:
        resp = requests.get(EIA_STATS_URL, timeout=60)
        resp.raise_for_status()
        self.save_raw({"url": EIA_STATS_URL, "bytes": len(resp.content)},
                      subdir="eia_storage_stats")

        wb = xlrd.open_workbook(file_contents=resp.content)
        # The first sheet always covers the current report year
        sheet = wb.sheet_by_index(0)

        conn = duckdb.connect(DB_PATH)
        now_str = datetime.now(timezone.utc).isoformat()
        records_written = 0

        try:
            records_written = self._parse_sheet(conn, sheet, now_str)
        finally:
            conn.close()

        return {"status": "ok", "records_written": records_written}

    def _parse_sheet(self, conn, sheet, now_str: str) -> int:
        count = 0
        # Data starts at row 3 (rows 0-2 are title/header)
        for row_idx in range(3, sheet.nrows):
            row = sheet.row_values(row_idx)
            date_serial = row[_COL_DATE]
            if not date_serial:
                continue

            # xlrd returns Excel serial dates as floats; convert to date string
            obs_date = xlrd.xldate_as_datetime(date_serial, sheet.book.datemode)
            obs_time = obs_date.strftime("%Y-%m-%dT00:00:00Z")

            for region, avg_col, max_col, min_col in _REGIONS:
                avg_val = row[avg_col] if row[avg_col] != "" else None
                max_val = row[max_col] if row[max_col] != "" else None
                min_val = row[min_col] if row[min_col] != "" else None

                for series_name, value in [
                    (f"storage_5yr_avg_{region}", avg_val),
                    (f"storage_5yr_max_{region}", max_val),
                    (f"storage_5yr_min_{region}", min_val),
                ]:
                    if value is None:
                        continue
                    conn.execute(_INSERT_SQL, [
                        series_name, region, obs_time, now_str, float(value),
                    ])
                    count += 1

        return count
