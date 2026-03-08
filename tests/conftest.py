"""
Shared pytest configuration and fixtures.

Sets required environment variables before any module-level import can fail
(config.settings reads EIA_API_KEY and FRED_API_KEY at import time), then
provides a reusable initialized-database fixture.
"""

import os

import duckdb
import pytest


def pytest_configure(config):
    """Set placeholder env vars before test collection imports config.settings."""
    os.environ.setdefault("EIA_API_KEY",  "test_key_placeholder")
    os.environ.setdefault("FRED_API_KEY", "test_key_placeholder")
    # DATA_BASE_DIR is overridden per-test via monkeypatch; this default
    # prevents settings.py from resolving to a real path that must exist.
    os.environ.setdefault("DATA_BASE_DIR", "/tmp/nat-gas-test")


@pytest.fixture
def test_db(tmp_path, monkeypatch):
    """
    Yield an initialized DuckDB path isolated to the current test.

    Patches DB_PATH and RAW_DIR at every module level that references them
    so collectors and transforms use the temp database rather than the real one.
    """
    db = str(tmp_path / "test.duckdb")
    raw = str(tmp_path / "raw")
    archive = str(tmp_path / "archive")
    os.makedirs(raw, exist_ok=True)
    os.makedirs(archive, exist_ok=True)

    # Patch every module that has already imported DB_PATH / RAW_DIR
    for mod, attr, val in [
        ("db.schema",              "DB_PATH",    db),
        ("collectors.base",        "DB_PATH",    db),
        ("collectors.base",        "RAW_DIR",    raw),
        ("collectors.eia_storage",       "DB_PATH", db),
        ("collectors.eia_storage_stats", "DB_PATH", db),
        ("collectors.price",       "DB_PATH",    db),
        ("collectors.weather",     "DB_PATH",    db),
        ("collectors.weather",     "ARCHIVE_DIR", archive),
        ("collectors.power_burn",  "DB_PATH",    db),
        ("collectors.eia_supply",  "DB_PATH",    db),
        ("collectors.cftc",        "DB_PATH",    db),
        ("transforms.features_storage", "DB_PATH", db),
        ("transforms.features_price",   "DB_PATH", db),
        ("transforms.features_weather", "DB_PATH", db),
        ("transforms.features_weather", "ARCHIVE_DIR", archive),
        ("transforms.features_cot",     "DB_PATH", db),
        ("transforms.features_summary", "DB_PATH", db),
    ]:
        try:
            monkeypatch.setattr(f"{mod}.{attr}", val)
        except AttributeError:
            # Module not yet imported — that is fine; the env default covers it
            pass

    # Initialize schema in the temp DB
    monkeypatch.setattr("db.schema.DB_PATH", db)
    from db.schema import initialize_schema
    initialize_schema()

    yield {"db": db, "raw": raw, "archive": archive}
