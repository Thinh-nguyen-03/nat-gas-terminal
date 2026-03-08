"""
Collector unit tests.

All HTTP calls are mocked — no real API keys or network access required.
Each test uses an isolated DuckDB instance via the test_db fixture.
"""

import json
import os
from unittest.mock import MagicMock, call, patch

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_fixture(name: str) -> dict:
    fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", name)
    with open(fixture_path) as f:
        return json.load(f)


def _mock_response(payload: dict) -> MagicMock:
    m = MagicMock()
    m.json.return_value = payload
    m.raise_for_status = MagicMock()
    return m


def _count_rows(db_path: str, source_name: str) -> int:
    conn = duckdb.connect(db_path, read_only=True)
    result = conn.execute(
        "SELECT COUNT(*) FROM facts_time_series WHERE source_name = ?",
        [source_name],
    ).fetchone()[0]
    conn.close()
    return result


# ---------------------------------------------------------------------------
# EIA Storage
# ---------------------------------------------------------------------------

class TestEIAStorageCollector:

    def test_records_written_match_data_rows(self, test_db):
        fixture = _load_fixture("eia_storage_fixture.json")
        with patch("collectors.eia_storage.requests.get",
                   return_value=_mock_response(fixture)):
            from collectors.eia_storage import EIAStorageCollector
            result = EIAStorageCollector().collect()

        assert result["status"] == "ok"
        # 5 rows in fixture * 6 regions
        assert result["records_written"] == 5 * 6

    def test_deduplication_on_second_run(self, test_db):
        """Running the collector twice must not double-insert any rows."""
        fixture = _load_fixture("eia_storage_fixture.json")
        with patch("collectors.eia_storage.requests.get",
                   return_value=_mock_response(fixture)):
            from collectors.eia_storage import EIAStorageCollector
            collector = EIAStorageCollector()
            r1 = collector.collect()
            r2 = collector.collect()

        db_count = _count_rows(test_db["db"], "eia_storage")
        assert db_count == r1["records_written"], (
            "Second run must not insert new rows — ON CONFLICT dedup must work"
        )
        assert r2["records_written"] == r1["records_written"]

    def test_observation_time_is_date_only_period(self):
        """EIA period strings must be YYYY-MM-DD, not a datetime."""
        fixture = _load_fixture("eia_storage_fixture.json")
        for row in fixture["response"]["data"]:
            period = row["period"]
            assert len(period) == 10, "EIA period must be a YYYY-MM-DD date string"
            assert "T" not in period, "EIA period must not contain a time component"

    def test_all_regions_fail_returns_zero_records(self, test_db):
        """If every region's HTTP call fails, collect() must return status='ok'
        with records_written=0. Errors are logged as warnings, not re-raised,
        so one bad EIA series ID cannot abort the whole storage collection."""
        bad_response = MagicMock()
        bad_response.raise_for_status.side_effect = Exception("HTTP 503")
        with patch("collectors.eia_storage.requests.get", return_value=bad_response):
            from collectors.eia_storage import EIAStorageCollector
            result = EIAStorageCollector().collect()

        assert result["status"] == "ok"
        assert result["records_written"] == 0

    def test_null_value_rows_are_skipped(self, test_db):
        """Rows where value is None must be skipped without raising."""
        fixture = {
            "response": {
                "data": [
                    {"period": "2026-03-01", "value": None},
                    {"period": "2026-02-22", "value": 1830},
                ]
            }
        }
        with patch("collectors.eia_storage.requests.get",
                   return_value=_mock_response(fixture)):
            from collectors.eia_storage import EIAStorageCollector
            result = EIAStorageCollector().collect()

        # 1 valid row * 6 regions (null row skipped)
        assert result["records_written"] == 6


# ---------------------------------------------------------------------------
# Power Burn (EIA-930)
# ---------------------------------------------------------------------------

class TestPowerBurnCollector:

    def test_records_written_match_data_rows(self, test_db):
        fixture = _load_fixture("eia_930_fixture.json")
        with patch("collectors.power_burn.requests.get",
                   return_value=_mock_response(fixture)):
            from collectors.power_burn import PowerBurnCollector
            result = PowerBurnCollector().collect()

        assert result["status"] == "ok"
        # 3 rows per region * 8 regions
        assert result["records_written"] == 3 * 8

    def test_one_region_failure_does_not_stop_others(self, test_db):
        """If one region's HTTP call fails, the remaining regions must still run."""
        fixture = _load_fixture("eia_930_fixture.json")
        good_response = _mock_response(fixture)
        bad_response  = MagicMock()
        bad_response.raise_for_status.side_effect = Exception("timeout")

        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return bad_response
            return good_response

        with patch("collectors.power_burn.requests.get", side_effect=side_effect):
            from collectors.power_burn import PowerBurnCollector
            result = PowerBurnCollector().collect()

        assert result["status"] == "ok"
        # 7 regions succeeded (1 failed) * 3 rows each
        assert result["records_written"] == 7 * 3

    def test_observation_time_format(self, test_db):
        """EIA-930 period 'YYYY-MM-DDTHH' must be stored as 'YYYY-MM-DDTHH:00:00Z'."""
        fixture = _load_fixture("eia_930_fixture.json")
        with patch("collectors.power_burn.requests.get",
                   return_value=_mock_response(fixture)):
            from collectors.power_burn import PowerBurnCollector
            PowerBurnCollector().collect()

        conn = duckdb.connect(test_db["db"], read_only=True)
        rows = conn.execute(
            "SELECT observation_time::VARCHAR FROM facts_time_series "
            "WHERE source_name = 'eia_930' LIMIT 1"
        ).fetchall()
        conn.close()

        assert rows, "Expected at least one EIA-930 row"
        ts = rows[0][0]
        assert ts.endswith(":00:00+00") or "00:00" in ts, (
            f"Unexpected timestamp format: {ts}"
        )


# ---------------------------------------------------------------------------
# EIA Supply
# ---------------------------------------------------------------------------

class TestEIASupplyCollector:

    def test_monthly_period_padded_to_first_of_month(self, test_db):
        """Monthly period 'YYYY-MM' must be stored as 'YYYY-MM-01T00:00:00Z'."""
        fixture = {
            "response": {
                "data": [{"period": "2026-02", "value": 12.5}]
            }
        }
        # Patch only the mexico series call (monthly) to return the fixture
        with patch("collectors.eia_supply.requests.get",
                   return_value=_mock_response(fixture)):
            from collectors.eia_supply import EIASupplyCollector
            EIASupplyCollector().collect()

        conn = duckdb.connect(test_db["db"], read_only=True)
        rows = conn.execute(
            # AT TIME ZONE 'UTC' converts TIMESTAMPTZ to a plain TIMESTAMP in UTC,
            # stripping the local-timezone offset that ::VARCHAR would otherwise apply.
            "SELECT (observation_time AT TIME ZONE 'UTC')::VARCHAR FROM facts_time_series "
            "WHERE source_name = 'eia_supply'"
        ).fetchall()
        conn.close()

        timestamps = [r[0] for r in rows]
        for ts in timestamps:
            assert ts.startswith("2026-02-01"), (
                f"Monthly period must map to the 1st of the month in UTC: {ts}"
            )

    def test_series_failure_does_not_stop_others(self, test_db):
        """A failing series must log a warning but not abort the whole collection."""
        good = _mock_response({"response": {"data": [{"period": "2026-03-01", "value": 100.0}]}})
        bad  = MagicMock()
        bad.raise_for_status.side_effect = Exception("404")

        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return bad if call_count == 1 else good

        with patch("collectors.eia_supply.requests.get", side_effect=side_effect):
            from collectors.eia_supply import EIASupplyCollector
            result = EIASupplyCollector().collect()

        assert result["status"] == "ok"
        assert result["records_written"] > 0


# ---------------------------------------------------------------------------
# Weather
# ---------------------------------------------------------------------------

class TestWeatherCollector:

    def _make_requests_side_effect(self, points_fixture, forecast_fixture):
        """Return a side_effect function that alternates between grid and forecast calls."""
        responses = []
        for _ in range(8):  # 8 cities
            responses.append(_mock_response(points_fixture))
            responses.append(_mock_response(forecast_fixture))
        idx = {"i": 0}

        def side_effect(*args, **kwargs):
            r = responses[idx["i"]]
            idx["i"] += 1
            return r

        return side_effect

    def test_daytime_periods_only_are_ingested(self, test_db):
        """Only daytime periods must be stored; nighttime rows must be skipped."""
        points_fixture   = _load_fixture("nws_points_fixture.json")
        forecast_fixture = _load_fixture("nws_forecast_fixture.json")

        with patch("collectors.weather.requests.get",
                   side_effect=self._make_requests_side_effect(points_fixture, forecast_fixture)):
            from collectors.weather import WeatherCollector
            result = WeatherCollector().collect()

        assert result["status"] == "ok"
        # nws_forecast_fixture has 2 daytime periods across 8 cities * 6 metrics each
        assert result["records_written"] == 2 * 8 * 6

    def test_one_city_failure_does_not_stop_others(self, test_db):
        """A city that raises during grid resolution must not stop remaining cities."""
        points_fixture   = _load_fixture("nws_points_fixture.json")
        forecast_fixture = _load_fixture("nws_forecast_fixture.json")

        # City 1 raises on its points call (call 1). Cities 2-8 each make two
        # calls: points then forecast. After the exception, remaining calls come
        # in pairs starting at call 2, so (call - 2) % 2 == 0 means a points call.
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("NWS grid resolution failed")
            remaining = call_count - 2
            if remaining % 2 == 0:
                return _mock_response(points_fixture)
            return _mock_response(forecast_fixture)

        with patch("collectors.weather.requests.get", side_effect=side_effect):
            from collectors.weather import WeatherCollector
            result = WeatherCollector().collect()

        assert result["status"] == "ok"
        # 7 cities succeeded (1 failed) * 2 daytime periods * 6 metrics
        assert result["records_written"] == 7 * 2 * 6

    def test_forecast_archive_written(self, test_db):
        """Each successful city fetch must write a snapshot JSON to the archive dir."""
        points_fixture   = _load_fixture("nws_points_fixture.json")
        forecast_fixture = _load_fixture("nws_forecast_fixture.json")

        with patch("collectors.weather.requests.get",
                   side_effect=self._make_requests_side_effect(points_fixture, forecast_fixture)), \
             patch("collectors.weather.ARCHIVE_DIR", test_db["archive"]):
            from collectors.weather import WeatherCollector
            WeatherCollector().collect()

        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        archive_today = os.path.join(test_db["archive"], today)
        assert os.path.isdir(archive_today)
        json_files = [f for f in os.listdir(archive_today) if f.endswith("_forecast.json")]
        assert len(json_files) == 8, "One archive file expected per city"


# ---------------------------------------------------------------------------
# EIA Storage Stats (5-year avg / max / min from ngsstats.xls)
# ---------------------------------------------------------------------------

class TestEIAStorageStatsCollector:

    def _fixture_bytes(self) -> bytes:
        fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", "ngsstats_fixture.xls")
        with open(fixture_path, "rb") as f:
            return f.read()

    def test_records_written_match_expected(self, test_db):
        """3 data rows * 6 regions * 3 series (avg/max/min) = 54 records."""
        xls_bytes = self._fixture_bytes()
        mock_resp = MagicMock()
        mock_resp.content = xls_bytes
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.eia_storage_stats.requests.get", return_value=mock_resp):
            from collectors.eia_storage_stats import EIAStorageStatsCollector
            result = EIAStorageStatsCollector().collect()

        assert result["status"] == "ok"
        assert result["records_written"] == 3 * 6 * 3

    def test_deduplication_on_second_run(self, test_db):
        """Running twice must not double-insert rows."""
        xls_bytes = self._fixture_bytes()
        mock_resp = MagicMock()
        mock_resp.content = xls_bytes
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.eia_storage_stats.requests.get", return_value=mock_resp):
            from collectors.eia_storage_stats import EIAStorageStatsCollector
            collector = EIAStorageStatsCollector()
            r1 = collector.collect()
            r2 = collector.collect()

        conn = duckdb.connect(test_db["db"], read_only=True)
        db_count = conn.execute(
            "SELECT COUNT(*) FROM facts_time_series WHERE source_name = 'eia_storage_stats'"
        ).fetchone()[0]
        conn.close()

        assert db_count == r1["records_written"], "Second run must not insert duplicate rows"
        assert r2["records_written"] == r1["records_written"]

    def test_total_avg_value_correct(self, test_db):
        """The Total Lower 48 5yr average for the first row must match the fixture value (3225 Bcf)."""
        xls_bytes = self._fixture_bytes()
        mock_resp = MagicMock()
        mock_resp.content = xls_bytes
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.eia_storage_stats.requests.get", return_value=mock_resp):
            from collectors.eia_storage_stats import EIAStorageStatsCollector
            EIAStorageStatsCollector().collect()

        conn = duckdb.connect(test_db["db"], read_only=True)
        row = conn.execute("""
            SELECT value FROM facts_time_series
            WHERE source_name = 'eia_storage_stats'
              AND series_name  = 'storage_5yr_avg_total'
              AND region       = 'total'
            ORDER BY observation_time ASC
            LIMIT 1
        """).fetchone()
        conn.close()

        assert row is not None
        assert row[0] == 3225.0

    def test_http_error_propagates(self, test_db):
        """A failed download must propagate as an exception (no partial data)."""
        bad_resp = MagicMock()
        bad_resp.raise_for_status.side_effect = Exception("HTTP 503")

        with patch("collectors.eia_storage_stats.requests.get", return_value=bad_resp):
            from collectors.eia_storage_stats import EIAStorageStatsCollector
            import pytest as _pytest
            with _pytest.raises(Exception, match="HTTP 503"):
                EIAStorageStatsCollector().collect()
