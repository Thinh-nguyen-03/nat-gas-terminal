import os
from dotenv import load_dotenv

load_dotenv()

EIA_API_KEY      = os.environ["EIA_API_KEY"]   # raises if missing — intentional
FRED_API_KEY     = os.environ["FRED_API_KEY"]
AIS_HUB_USERNAME = os.environ.get("AIS_HUB_USERNAME", "")  # optional — AISHub free account

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
