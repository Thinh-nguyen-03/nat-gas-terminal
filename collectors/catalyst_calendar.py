"""
Catalyst calendar generator.

Populates the catalyst_calendar table with recurring market-moving events for
the next 60 days.  Uses deterministic IDs (event_type + date + time) so the
job can run daily without creating duplicates — ON CONFLICT DO NOTHING handles
re-runs safely.

Run automatically at 6:00 AM ET via the scheduler, or manually:

    python -c "from collectors.catalyst_calendar import CatalystCalendarCollector; CatalystCalendarCollector().run()"
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Iterator

import duckdb

from collectors.base import CollectorBase
from config.settings import DB_PATH

# ---------------------------------------------------------------------------
# FOMC decision dates (second day of each two-day meeting).
# Update each December when the Fed publishes the following year's schedule.
# Source: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm
# ---------------------------------------------------------------------------
FOMC_DATES: list[date] = [
    # 2025
    date(2025, 1, 29), date(2025, 3, 19), date(2025, 5, 7),
    date(2025, 6, 18), date(2025, 7, 30), date(2025, 9, 17),
    date(2025, 10, 29), date(2025, 12, 10),
    # 2026
    date(2026, 1, 28), date(2026, 3, 18), date(2026, 4, 29),
    date(2026, 6, 17), date(2026, 7, 29), date(2026, 9, 16),
    date(2026, 10, 28), date(2026, 12, 9),
]

_LOOKAHEAD_DAYS = 60

_UPSERT_SQL = """
    INSERT INTO catalyst_calendar
        (id, event_date, event_time_et, event_type, description, impact, is_auto)
    VALUES (?, ?, ?, ?, ?, ?, TRUE)
    ON CONFLICT (id) DO NOTHING
"""


# ---------------------------------------------------------------------------
# Calendar helpers
# ---------------------------------------------------------------------------

def _event_id(event_type: str, d: date, time_et: str | None) -> str:
    tag = time_et.replace(":", "") if time_et else "allday"
    return f"{event_type}_{d.isoformat()}_{tag}"


def _iter_weekday(start: date, end: date, weekday: int) -> Iterator[date]:
    """Yield every occurrence of weekday (0=Mon…6=Sun) from start to end."""
    offset = (weekday - start.weekday()) % 7
    d = start + timedelta(days=offset)
    while d <= end:
        yield d
        d += timedelta(weeks=1)


def _second_tuesday(year: int, month: int) -> date:
    """Return the second Tuesday of the given month."""
    first = date(year, month, 1)
    offset = (1 - first.weekday()) % 7   # days to first Tuesday (weekday=1)
    return first + timedelta(days=offset + 7)


# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------

def _eia_storage_events(start: date, end: date) -> list[tuple]:
    """EIA weekly storage report — every Thursday 10:30 ET."""
    out = []
    for d in _iter_weekday(start, end, 3):  # 3 = Thursday
        out.append((
            _event_id("eia_storage", d, "10:30"),
            d, "10:30", "eia_storage",
            "EIA Weekly Natural Gas Storage Report", "high",
        ))
    return out


def _rig_count_events(start: date, end: date) -> list[tuple]:
    """Baker Hughes rig count — every Friday 1:00 PM ET."""
    out = []
    for d in _iter_weekday(start, end, 4):  # 4 = Friday
        out.append((
            _event_id("rig_count", d, "13:00"),
            d, "13:00", "rig_count",
            "Baker Hughes US Natural Gas Rig Count", "low",
        ))
    return out


def _cftc_cot_events(start: date, end: date) -> list[tuple]:
    """CFTC COT report — every Friday 3:30 PM ET (data through prior Tuesday)."""
    out = []
    for d in _iter_weekday(start, end, 4):
        data_through = d - timedelta(days=3)
        out.append((
            _event_id("cftc_cot", d, "15:30"),
            d, "15:30", "cftc_cot",
            f"CFTC Commitments of Traders (data through {data_through})", "medium",
        ))
    return out


def _steo_events(start: date, end: date) -> list[tuple]:
    """EIA STEO — second Tuesday of each month, 9:00 AM ET."""
    out = []
    # Walk month by month across the window
    year, month = start.year, start.month
    seen: set[tuple[int, int]] = set()
    check = start
    while check <= end + timedelta(days=31):
        key = (check.year, check.month)
        if key not in seen:
            seen.add(key)
            d = _second_tuesday(check.year, check.month)
            if start <= d <= end:
                out.append((
                    _event_id("eia_steo", d, "09:00"),
                    d, "09:00", "eia_steo",
                    "EIA Short-Term Energy Outlook (STEO)", "medium",
                ))
        check += timedelta(days=28)
    return out


def _fomc_events(start: date, end: date) -> list[tuple]:
    """FOMC rate decisions — from the hardcoded schedule."""
    out = []
    for d in FOMC_DATES:
        if start <= d <= end:
            out.append((
                _event_id("fomc", d, "14:00"),
                d, "14:00", "fomc",
                "FOMC Rate Decision", "medium",
            ))
    return out


def generate_events(window_days: int = _LOOKAHEAD_DAYS) -> list[tuple]:
    """Return all recurring events for the next window_days days."""
    today = date.today()
    end   = today + timedelta(days=window_days)
    return (
        _eia_storage_events(today, end)
        + _rig_count_events(today, end)
        + _cftc_cot_events(today, end)
        + _steo_events(today, end)
        + _fomc_events(today, end)
    )


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class CatalystCalendarCollector(CollectorBase):
    source_name = "catalyst_calendar"

    def collect(self) -> dict:
        events = generate_events()
        conn = duckdb.connect(DB_PATH)
        count = 0
        try:
            for ev in events:
                conn.execute(_UPSERT_SQL, list(ev))
                count += 1
        finally:
            conn.close()
        return {"status": "ok", "events_written": count}
