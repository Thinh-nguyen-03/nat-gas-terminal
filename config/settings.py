import logging
import os
import time

import duckdb
from dotenv import load_dotenv

load_dotenv()

EIA_API_KEY      = os.environ["EIA_API_KEY"]   # raises if missing — intentional
FRED_API_KEY     = os.environ["FRED_API_KEY"]
AISSTREAM_API_KEY      = os.environ.get("AISSTREAM_API_KEY", "")

GEMINI_API_KEY         = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL           = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite")

NOAA_CDO_TOKEN         = os.environ.get("NOAA_CDO_TOKEN", "")
PJM_API_KEY            = os.environ.get("PJM_API_KEY", "")
ERCOT_SUBSCRIPTION_KEY = os.environ.get("ERCOT_SUBSCRIPTION_KEY", "")
ERCOT_USERNAME         = os.environ.get("ERCOT_USERNAME", "")
ERCOT_PASSWORD         = os.environ.get("ERCOT_PASSWORD", "")
ISO_NE_USERNAME        = os.environ.get("ISO_NE_USERNAME", "")
ISO_NE_PASSWORD        = os.environ.get("ISO_NE_PASSWORD", "")

# DATA_BASE_DIR lets Docker override via env_file
_BASE_DIR    = os.environ.get("DATA_BASE_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
DATA_DIR     = os.path.abspath(_BASE_DIR)
RAW_DIR      = os.path.join(DATA_DIR, "raw")
DB_PATH      = os.path.join(DATA_DIR, "db", "terminal.duckdb")
ARCHIVE_DIR  = os.path.join(DATA_DIR, "forecasts_archive")
LOG_PATH     = os.path.join(os.path.dirname(__file__), "..", "logs", "collectors.log")

# After a successful write, POST here so the Go API broadcasts SSE to the frontend
NOTIFY_API_URL   = os.environ.get("NOTIFY_API_URL", "http://localhost:8080/internal/notify")
INTERNAL_API_KEY = os.environ.get("INTERNAL_API_KEY", "")

_settings_log = logging.getLogger("collectors")


def connect_db(path: str = "") -> "duckdb.DuckDBPyConnection":
    """Open DuckDB read-write with retry for transient file-lock conflicts.

    The Go API holds DuckDB in READ_ONLY mode and releases the lock between
    requests (SetMaxIdleConns=0). If Python tries to open a write connection
    exactly during a Go query, DuckDB raises IOException "already open".
    Retrying with exponential backoff hits the next idle window.
    """
    target = path or DB_PATH
    for attempt in range(10):
        try:
            return duckdb.connect(target)
        except duckdb.IOException as exc:
            if "already open" not in str(exc) or attempt == 9:
                raise
            wait = 0.3 * (2 ** attempt)
            _settings_log.debug(
                "DuckDB locked, retry in %.1fs (attempt %d/10)", wait, attempt + 1
            )
            time.sleep(wait)
    raise RuntimeError("unreachable")
