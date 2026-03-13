"""
Transform unit tests.

Tests for interpretation functions (pure, no DB) and feature computation
functions (seeded DuckDB, no network calls).
"""

import json
import os
from datetime import date, datetime, timezone

import duckdb
import pytest


def _seed_storage(db_path: str, rows: list[tuple]) -> None:
    """Insert (obs_date_str, value) tuples into facts_time_series as eia_storage/total."""
    conn = duckdb.connect(db_path)
    for obs_date, value in rows:
        conn.execute("""
            INSERT INTO facts_time_series
                (source_name, series_name, region, observation_time,
                 ingest_time, value, unit, frequency)
            VALUES ('eia_storage', 'storage_total', 'total', ?, ?, ?, 'Bcf', 'weekly')
            ON CONFLICT DO NOTHING
        """, [f"{obs_date}T00:00:00Z", datetime.now(timezone.utc).isoformat(), value])
    conn.close()


def _seed_cot(db_path: str, obs_date: str, mm_long: float, mm_short: float, oi: float) -> None:
    conn = duckdb.connect(db_path)
    now = datetime.now(timezone.utc).isoformat()
    obs_time = f"{obs_date}T00:00:00Z"
    for series, val in [
        ("cot_mm_long",       mm_long),
        ("cot_mm_short",      mm_short),
        ("cot_open_interest", oi),
    ]:
        conn.execute("""
            INSERT INTO facts_time_series
                (source_name, series_name, region, observation_time,
                 ingest_time, value, unit, frequency)
            VALUES ('cftc', ?, 'US', ?, ?, ?, 'contracts', 'weekly')
            ON CONFLICT DO NOTHING
        """, [series, obs_time, now, val])
    conn.close()


def _get_feature(db_path: str, feature_name: str, for_date: date = None) -> dict | None:
    if for_date is None:
        for_date = date.today()
    conn = duckdb.connect(db_path, read_only=True)
    row = conn.execute("""
        SELECT value, interpretation, confidence
        FROM features_daily
        WHERE feature_date = ? AND feature_name = ? AND region = 'US'
    """, [for_date, feature_name]).fetchone()
    conn.close()
    if row is None:
        return None
    return {"value": row[0], "interpretation": row[1], "confidence": row[2]}


class TestStorageInterpretation:

    def test_interpret_deficit_thresholds(self):
        from transforms.features_storage import _interpret_deficit
        assert _interpret_deficit(-250) == "very_bullish"
        assert _interpret_deficit(-150) == "bullish"
        assert _interpret_deficit(-50)  == "mildly_bullish"
        assert _interpret_deficit(50)   == "mildly_bearish"
        assert _interpret_deficit(150)  == "bearish"
        assert _interpret_deficit(None) == "unknown"

    def test_interpret_eos_withdrawal_season(self):
        from transforms.features_storage import _interpret_eos
        winter = date(2026, 1, 15)
        assert _interpret_eos(1500, winter) == "very_bullish"   # below 1700 low
        assert _interpret_eos(1750, winter) == "bullish"        # below mid (1850)
        assert _interpret_eos(2100, winter) == "bearish"        # above 2000 high
        assert _interpret_eos(1900, winter) == "neutral"        # within band
        assert _interpret_eos(None, winter) == "unknown"

    def test_interpret_eos_injection_season(self):
        from transforms.features_storage import _interpret_eos
        summer = date(2026, 7, 15)
        assert _interpret_eos(3200, summer) == "very_bullish"   # below 3500 low
        assert _interpret_eos(4000, summer) == "bearish"        # above 3800 high


class TestComputeStorageFeatures:

    def test_basic_features_written(self, test_db):
        db = test_db["db"]
        _seed_storage(db, [
            ("2026-03-01", 1780.0),
            ("2026-02-22", 1830.0),
            ("2026-02-15", 1900.0),
            ("2026-02-08", 1960.0),
        ])

        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "transforms.features_storage.DB_PATH", db
        ):
            from transforms.features_storage import compute_storage_features
            compute_storage_features()

        total = _get_feature(db, "storage_total_bcf")
        assert total is not None
        assert total["value"] == pytest.approx(1780.0)
        assert total["interpretation"] == "neutral"

    def test_wow_change_is_correct(self, test_db):
        db = test_db["db"]
        _seed_storage(db, [
            ("2026-03-01", 1780.0),
            ("2026-02-22", 1830.0),
        ])

        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "transforms.features_storage.DB_PATH", db
        ):
            from transforms.features_storage import compute_storage_features
            compute_storage_features()

        wow = _get_feature(db, "storage_wow_change_bcf")
        assert wow is not None
        assert wow["value"] == pytest.approx(-50.0)  # 1780 - 1830

    def test_upsert_on_recompute(self, test_db):
        """Re-running feature computation must update, not duplicate."""
        db = test_db["db"]
        _seed_storage(db, [("2026-03-01", 1780.0), ("2026-02-22", 1830.0)])

        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "transforms.features_storage.DB_PATH", db
        ):
            from transforms.features_storage import compute_storage_features
            compute_storage_features()
            compute_storage_features()

        conn = duckdb.connect(db, read_only=True)
        count = conn.execute(
            "SELECT COUNT(*) FROM features_daily WHERE feature_name = 'storage_total_bcf'"
        ).fetchone()[0]
        conn.close()
        assert count == 1, "Upsert must not create duplicate feature rows"

    def test_no_data_returns_gracefully(self, test_db):
        """An empty DB must not raise — compute must return without writing."""
        db = test_db["db"]
        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "transforms.features_storage.DB_PATH", db
        ):
            from transforms.features_storage import compute_storage_features
            compute_storage_features()  # must not raise

        conn = duckdb.connect(db, read_only=True)
        count = conn.execute("SELECT COUNT(*) FROM features_daily").fetchone()[0]
        conn.close()
        assert count == 0


class TestCOTInterpretation:

    def test_contrarian_crowded_short_is_bullish(self):
        from transforms.features_cot import _interpret_cot
        assert _interpret_cot(-25) == "bullish"
        assert _interpret_cot(-15) == "mildly_bullish"
        assert _interpret_cot(0)   == "neutral"
        assert _interpret_cot(15)  == "mildly_bearish"
        assert _interpret_cot(25)  == "bearish"
        assert _interpret_cot(None) == "unknown"


class TestComputeCOTFeatures:

    def test_mm_net_computed_correctly(self, test_db):
        db = test_db["db"]
        _seed_cot(db, "2026-03-04", mm_long=150000.0, mm_short=80000.0, oi=500000.0)

        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "transforms.features_cot.DB_PATH", db
        ):
            from transforms.features_cot import compute_cot_features
            compute_cot_features()

        net = _get_feature(db, "cot_mm_net_contracts")
        assert net is not None
        assert net["value"] == pytest.approx(70000.0)

    def test_mm_net_pct_oi_computed_correctly(self, test_db):
        db = test_db["db"]
        # 150k long, 50k short, 400k OI -> net = 100k -> 25% of OI -> bearish
        _seed_cot(db, "2026-03-04", mm_long=150000.0, mm_short=50000.0, oi=400000.0)

        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "transforms.features_cot.DB_PATH", db
        ):
            from transforms.features_cot import compute_cot_features
            compute_cot_features()

        pct = _get_feature(db, "cot_mm_net_pct_oi")
        assert pct is not None
        assert pct["value"] == pytest.approx(25.0)
        assert pct["interpretation"] == "bearish"


class TestPriceInterpretation:

    def test_price_interp_boundaries(self):
        from transforms.features_price import _price_interp
        assert _price_interp(5.0)  == "bullish"
        assert _price_interp(1.0)  == "mildly_bullish"
        assert _price_interp(-1.0) == "mildly_bearish"
        assert _price_interp(-5.0) == "bearish"
        assert _price_interp(None) == "unknown"

    def test_pct_change_calculation(self):
        from transforms.features_price import _pct
        assert _pct(110.0, 100.0) == pytest.approx(10.0)
        assert _pct(90.0,  100.0) == pytest.approx(-10.0)
        assert _pct(100.0, 0.0)   is None
        assert _pct(None,  100.0) is None


class TestWeatherInterpretation:

    def test_hdd_interpretation_thresholds(self):
        from transforms.features_weather import _interpret_hdd
        assert _interpret_hdd(90)  == "bullish"
        assert _interpret_hdd(60)  == "mildly_bullish"
        assert _interpret_hdd(30)  == "neutral"
        assert _interpret_hdd(12)  == "mildly_bearish"
        assert _interpret_hdd(5)   == "bearish"

    def test_revision_interpretation_thresholds(self):
        from transforms.features_weather import _interpret_revision
        assert _interpret_revision(6.0)  == "bullish"
        assert _interpret_revision(2.0)  == "mildly_bullish"
        assert _interpret_revision(0.5)  == "neutral"
        assert _interpret_revision(-2.0) == "mildly_bearish"
        assert _interpret_revision(-6.0) == "bearish"


class TestFundamentalScore:

    def test_score_label_boundaries(self):
        from transforms.features_summary import _score_label
        assert _score_label(50)  == "Strongly Bullish"
        assert _score_label(30)  == "Bullish"
        assert _score_label(10)  == "Mildly Bullish"
        assert _score_label(0)   == "Neutral / Mixed"
        assert _score_label(-10) == "Mildly Bearish"
        assert _score_label(-30) == "Bearish"
        assert _score_label(-50) == "Strongly Bearish"

    def test_empty_features_returns_zero_score(self, test_db):
        """Score computed with no features in DB must return 0.0 without raising."""
        db = test_db["db"]
        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "transforms.features_summary.DB_PATH", db
        ):
            from transforms.features_summary import compute_fundamental_score
            result = compute_fundamental_score()

        assert result["score"] == 0.0
        assert result["label"] == "Neutral / Mixed"
        assert result["drivers"] == []
