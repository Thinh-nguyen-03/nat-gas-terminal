"""
Scheduler entry point. Run this as a standalone process alongside the Go API.

    python -m scheduler.jobs

All times are US/Eastern. Cron triggers fire in that timezone unless noted.
"""

import logging
import logging.config
import threading
from datetime import datetime, timezone

import pytz
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from collectors.catalyst_calendar import CatalystCalendarCollector
from collectors.iso_lmp            import ISOLMPCollector
from collectors.news_wire          import NewsWireCollector
from collectors.cftc              import CFTCCollector
from collectors.cpc_outlook       import CPCOutlookCollector
from collectors.eia_storage       import EIAStorageCollector
from collectors.eia_storage_stats import EIAStorageStatsCollector
from collectors.eia_supply        import EIASupplyCollector
from collectors.power_burn        import PowerBurnCollector
from collectors.price             import PriceCollector
from collectors.rig_count         import RigCountCollector
from collectors.weather           import WeatherCollector
from transforms.features_analog       import compute_analog_features
from transforms.features_fairvalue    import compute_fairvalue_features
from transforms.features_lng          import compute_lng_features
from transforms.features_power_demand import compute_power_demand_features
from transforms.features_cot     import compute_cot_features
from transforms.features_cpc     import compute_cpc_features
from transforms.features_price   import compute_price_features
from transforms.features_storage import compute_storage_features
from transforms.features_summary import save_summary
from transforms.market_brief     import compute_market_brief
from transforms.features_weather import compute_weather_features
from config.settings import LOG_PATH, NOTIFY_API_URL, INTERNAL_API_KEY, connect_db

ET = pytz.timezone("US/Eastern")


def _notify(source: str) -> None:
    """Fire-and-forget POST to Go API SSE broker after a transform completes."""
    try:
        requests.post(
            NOTIFY_API_URL,
            data=source.encode(),
            headers={
                "Content-Type": "text/plain",
                "X-Internal-Key": INTERNAL_API_KEY,
            },
            timeout=2,
        )
    except Exception:
        pass  # Non-critical — SSE push is best-effort


def _notify_after(fn, source: str):
    """Wrap a transform function so it notifies the SSE broker on success."""
    def wrapper():
        fn()
        _notify(source)
    wrapper.__name__ = f"{fn.__name__}+notify"
    return wrapper


# ---------------------------------------------------------------------------
# Data freshness watchdog — startup gap check + periodic monitor
# ---------------------------------------------------------------------------

def _is_market_hours(now: datetime) -> bool:
    """True Mon–Fri 7–18 ET."""
    return now.weekday() < 5 and 7 <= now.hour < 19


def _is_thursday(now: datetime) -> bool:
    return now.weekday() == 3


def _is_friday(now: datetime) -> bool:
    return now.weekday() == 4


def _build_checks() -> list[dict]:
    """Freshness check definitions for every collector source.

    max_age_h: trigger a catch-up run if last_success is older than this.
    when:      optional callable(datetime) -> bool; check skipped if False.
               Use for sources that only publish on specific days/hours.
    """
    return [
        {"source": "price",              "fn": PriceCollector().run,            "max_age_h": 2.0,   "when": _is_market_hours},
        {"source": "nws_weather",        "fn": WeatherCollector().run,          "max_age_h": 7.0,   "when": None},
        {"source": "cpc_outlook",        "fn": CPCOutlookCollector().run,       "max_age_h": 26.0,  "when": None},
        {"source": "eia_930_power_burn", "fn": PowerBurnCollector().run,        "max_age_h": 2.0,   "when": None},
        {"source": "iso_lmp",            "fn": ISOLMPCollector().run,           "max_age_h": 2.0,   "when": None},
        {"source": "news_wire",          "fn": NewsWireCollector().run,         "max_age_h": 1.0,   "when": None},
        {"source": "eia_supply",         "fn": EIASupplyCollector().run,        "max_age_h": 26.0,  "when": None},
        {"source": "catalyst_calendar",  "fn": CatalystCalendarCollector().run, "max_age_h": 26.0,  "when": None},
        # Weekly sources: only trigger on release days to avoid fetching stale data
        {"source": "eia_storage",        "fn": EIAStorageCollector().run,       "max_age_h": 192.0, "when": _is_thursday},
        {"source": "eia_storage_stats",  "fn": EIAStorageStatsCollector().run,  "max_age_h": 192.0, "when": _is_thursday},
        {"source": "cftc_cot",           "fn": CFTCCollector().run,             "max_age_h": 192.0, "when": _is_friday},
        {"source": "rig_count",          "fn": RigCountCollector().run,         "max_age_h": 192.0, "when": _is_friday},
    ]


# Prevent duplicate concurrent catch-up runs for the same source.
_CATCHUP_ACTIVE: set[str] = set()
_CATCHUP_LOCK   = threading.Lock()


def _stale_sources(checks: list[dict], logger: logging.Logger) -> list[dict]:
    """Query collector_health; return checks whose source is stale or never collected."""
    now = datetime.now(ET)
    try:
        conn = connect_db()
        rows = conn.execute(
            "SELECT source_name, last_success FROM collector_health"
        ).fetchall()
        conn.close()
    except Exception as e:
        logger.warning("[watchdog] DB read failed: %s", e)
        return []

    last_success: dict[str, datetime | None] = {r[0]: r[1] for r in rows}
    stale: list[dict] = []

    for check in checks:
        source  = check["source"]
        when_fn = check.get("when")

        if when_fn is not None and not when_fn(now):
            continue  # outside active window for this source

        last = last_success.get(source)
        if last is None:
            age_h = float("inf")  # never collected
        else:
            if isinstance(last, str):
                last = datetime.fromisoformat(last.replace("Z", "+00:00"))
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            age_h = (
                now.astimezone(timezone.utc) - last.astimezone(timezone.utc)
            ).total_seconds() / 3600

        if age_h > check["max_age_h"]:
            label = "never collected" if age_h == float("inf") else f"{age_h:.1f}h old"
            logger.info(
                "[watchdog] %s stale (%s, threshold %.1fh) — queuing catch-up",
                source, label, check["max_age_h"],
            )
            stale.append(check)

    return stale


def _run_catchup(source: str, fn, logger: logging.Logger) -> None:
    """Run a single collector catch-up, guarded against duplicate concurrent runs."""
    with _CATCHUP_LOCK:
        if source in _CATCHUP_ACTIVE:
            logger.debug("[watchdog] %s catch-up already running — skipping", source)
            return
        _CATCHUP_ACTIVE.add(source)
    try:
        fn()
    except Exception as e:
        logger.warning("[watchdog] catch-up for %s raised: %s", source, e)
    finally:
        with _CATCHUP_LOCK:
            _CATCHUP_ACTIVE.discard(source)


def _startup_gap_check(checks: list[dict], logger: logging.Logger) -> None:
    """Run once at process start — catch up stale sources in parallel, then wait."""
    logger.info("[watchdog] startup gap check starting")
    stale = _stale_sources(checks, logger)
    if not stale:
        logger.info("[watchdog] all sources fresh — no catch-up needed")
        return

    threads = [
        threading.Thread(
            target=_run_catchup,
            args=(c["source"], c["fn"], logger),
            daemon=True,
            name=f"startup-{c['source']}",
        )
        for c in stale
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)  # wait at most 2 min per collector
    logger.info("[watchdog] startup gap check complete")


def _watchdog_job(checks: list[dict], logger: logging.Logger) -> None:
    """Periodic check — fires stale collectors in background threads."""
    stale = _stale_sources(checks, logger)
    for check in stale:
        threading.Thread(
            target=_run_catchup,
            args=(check["source"], check["fn"], logger),
            daemon=True,
            name=f"watchdog-{check['source']}",
        ).start()


_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%SZ",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
        "file": {
            "class": "logging.FileHandler",
            "filename": LOG_PATH,
            "formatter": "standard",
        },
    },
    "root": {
        "handlers": ["console", "file"],
        "level": "INFO",
    },
}


def _build_scheduler() -> tuple[BlockingScheduler, list[dict]]:
    scheduler = BlockingScheduler(timezone=ET)
    checks = _build_checks()

    # Front month + forward curve: every 30 minutes during extended market hours Mon-Fri
    # Starts at 7 AM to capture pre-market moves; NYMEX NG trades from Sun 6 PM ET.
    scheduler.add_job(
        PriceCollector().run,
        CronTrigger(day_of_week="mon-fri", hour="7-18", minute="0,30", timezone=ET),
        id="price",
        name="Price + forward curve (yfinance + FRED)",
        misfire_grace_time=300,
    )

    # NWS 7-day forecast for 8 cities: every 6 hours
    scheduler.add_job(
        WeatherCollector().run,
        CronTrigger(hour="0,6,12,18", timezone=ET),
        id="weather",
        name="NWS weather forecast",
        misfire_grace_time=600,
    )

    # CPC 6-10 / 8-14 day temperature outlook: daily 7:00 AM ET
    # Additive to NWS 7-day — extends the forecast range for market positioning.
    scheduler.add_job(
        CPCOutlookCollector().run,
        CronTrigger(hour=7, minute=0, timezone=ET),
        id="cpc_outlook",
        name="CPC 6-10 / 8-14 day temperature outlook",
        misfire_grace_time=900,
    )

    # EIA-930 power burn: every hour at :05 (staggered from the top of hour)
    scheduler.add_job(
        PowerBurnCollector().run,
        CronTrigger(minute=5),
        id="power_burn",
        name="EIA-930 gas-fired generation",
        misfire_grace_time=300,
    )

    # EIA storage: Thursday 10:45 AM ET (release is 10:30, 15-minute buffer)
    # Retry at 11:15 AM in case EIA is delayed (happens ~10% of the time).
    scheduler.add_job(
        EIAStorageCollector().run,
        CronTrigger(day_of_week="thu", hour=10, minute=45, timezone=ET),
        id="eia_storage",
        name="EIA weekly storage report",
        misfire_grace_time=900,
    )
    scheduler.add_job(
        EIAStorageCollector().run,
        CronTrigger(day_of_week="thu", hour=11, minute=15, timezone=ET),
        id="eia_storage_retry",
        name="EIA weekly storage report (retry)",
        misfire_grace_time=900,
    )

    # EIA storage stats (5yr avg/max/min): Thursday 11:00 AM ET, after storage ingested
    scheduler.add_job(
        EIAStorageStatsCollector().run,
        CronTrigger(day_of_week="thu", hour=11, minute=0, timezone=ET),
        id="eia_storage_stats",
        name="EIA storage 5-year avg/max/min bands",
        misfire_grace_time=900,
    )

    # EIA supply: daily 8:00 AM ET (weekly data, ~2-week lag — daily poll catches it)
    scheduler.add_job(
        EIASupplyCollector().run,
        CronTrigger(hour=8, minute=0, timezone=ET),
        id="eia_supply",
        name="EIA supply fundamentals (production, LNG, pipeline)",
        misfire_grace_time=900,
    )

    # CFTC COT: Friday 4:00 PM ET (published ~3:30 PM ET)
    scheduler.add_job(
        CFTCCollector().run,
        CronTrigger(day_of_week="fri", hour=16, minute=0, timezone=ET),
        id="cftc_cot",
        name="CFTC disaggregated COT report",
        misfire_grace_time=1800,
    )

    # Baker Hughes rig count: Friday 2:00 PM ET (published ~1:00 PM ET)
    scheduler.add_job(
        RigCountCollector().run,
        CronTrigger(day_of_week="fri", hour=14, minute=0, timezone=ET),
        id="rig_count",
        name="Baker Hughes US natural gas rig count",
        misfire_grace_time=1800,
    )

    # Catalyst calendar: daily 6:00 AM ET — fills next 60 days of recurring events
    scheduler.add_job(
        CatalystCalendarCollector().run,
        CronTrigger(hour=6, minute=0, timezone=ET),
        id="catalyst_calendar",
        name="Catalyst calendar (EIA, CFTC, FOMC, STEO, rig count dates)",
        misfire_grace_time=900,
    )

    # ISO LMP: every hour at :10 (NYISO, MISO, CAISO — free public APIs)
    scheduler.add_job(
        ISOLMPCollector().run,
        CronTrigger(minute=10),
        id="iso_lmp",
        name="ISO LMP hub prices (NYISO, MISO, CAISO)",
        misfire_grace_time=300,
    )

    # News Wire: every 30 minutes — reduced from 15 min to ease GNews rate-limit pressure
    scheduler.add_job(
        NewsWireCollector().run,
        CronTrigger(minute="2,32"),
        id="news_wire",
        name="News Wire (EIA / FERC RSS feeds)",
        misfire_grace_time=300,
    )

    # AIS LNG vessel tracking is handled by the Go cmd/ais binary (persistent
    # WebSocket connection). No Python job needed here.

    scheduler.add_job(
        _notify_after(compute_price_features, "feat_price"),
        CronTrigger(minute=10),
        id="feat_price",
        name="Price + curve features",
    )
    # Storage features: daily at 11:30 AM ET — data is weekly, hourly was wasteful.
    # Thursday timing: 30 min after EIA Storage Stats (11:00 AM), so new data is ingested.
    scheduler.add_job(
        _notify_after(compute_storage_features, "feat_storage"),
        CronTrigger(hour=11, minute=30, timezone=ET),
        id="feat_storage",
        name="Storage features (deficit, EOS projection)",
    )
    # Weather features: every 6 hours at :20 — matches NWS update cadence (0,6,12,18 ET)
    scheduler.add_job(
        _notify_after(compute_weather_features, "feat_weather"),
        CronTrigger(hour="0,6,12,18", minute=20, timezone=ET),
        id="feat_weather",
        name="Weather features (HDD, revision delta)",
    )
    # CPC features: daily at 7:30 AM ET — 30 min after CPC collection at 7:00 AM
    scheduler.add_job(
        _notify_after(compute_cpc_features, "feat_cpc"),
        CronTrigger(hour=7, minute=30, timezone=ET),
        id="feat_cpc",
        name="CPC outlook features (6-10 / 8-14 day weighted prob below)",
    )
    # COT features: daily at 6:00 AM ET — data is weekly, hourly was wasteful.
    # Friday 4 PM CFTC release will be reflected by Saturday 6 AM recompute.
    scheduler.add_job(
        _notify_after(compute_cot_features, "feat_cot"),
        CronTrigger(hour=6, minute=0, timezone=ET),
        id="feat_cot",
        name="COT positioning features",
    )

    # Power demand z-scores: runs after LMP collector at :10
    scheduler.add_job(
        _notify_after(compute_power_demand_features, "feat_power_demand"),
        CronTrigger(minute=12),
        id="feat_power_demand",
        name="ISO LMP stress z-scores + composite demand index",
    )

    # LNG export features: every 30 minutes — reduced from 10 min to cut DB lock contention
    # with the Go AIS binary's 5-min write windows. EIA supply data is daily anyway.
    scheduler.add_job(
        _notify_after(compute_lng_features, "feat_lng"),
        CronTrigger(minute="5,35"),
        id="feat_lng",
        name="LNG implied export rate + terminal utilization",
    )

    # Fundamental score + what-changed: runs after all features are refreshed
    scheduler.add_job(
        _notify_after(save_summary, "summary"),
        CronTrigger(minute=30),
        id="summary",
        name="Fundamental score + what-changed summary",
    )

    # Historical analog finder: runs after summary so all features are fresh
    scheduler.add_job(
        _notify_after(compute_analog_features, "feat_analog"),
        CronTrigger(minute=35),
        id="feat_analog",
        name="Historical analog finder (cosine similarity on feature snapshots)",
    )

    # Fair value model: lookup table (OLS after refit_fairvalue.py is run)
    scheduler.add_job(
        _notify_after(compute_fairvalue_features, "feat_fairvalue"),
        CronTrigger(minute=40),
        id="feat_fairvalue",
        name="Fair value price model (lookup table / OLS)",
    )

    # Market Brief: Gemini synthesis of all signals — Mon-Fri 7 AM–6 PM ET only.
    # Restricting to trading hours avoids unnecessary API calls overnight/weekends.
    # No-op when GEMINI_API_KEY is not set.
    scheduler.add_job(
        _notify_after(compute_market_brief, "market_brief"),
        CronTrigger(day_of_week="mon-fri", hour="7-18", minute=45, timezone=ET),
        id="market_brief",
        name="Market Brief (Gemini multi-signal synthesis)",
    )

    # Watchdog: every 15 minutes, check collector_health for stale sources
    # and trigger catch-up runs in background threads.
    wdog_logger = logging.getLogger("scheduler")
    scheduler.add_job(
        lambda: _watchdog_job(checks, wdog_logger),
        CronTrigger(minute="7,22,37,52"),
        id="watchdog",
        name="Data freshness watchdog",
        misfire_grace_time=300,
    )

    return scheduler, checks


def main() -> None:
    logging.config.dictConfig(_LOGGING_CONFIG)
    logger = logging.getLogger("scheduler")

    scheduler, checks = _build_scheduler()

    logger.info("Scheduler starting — %d jobs registered", len(scheduler.get_jobs()))
    for job in scheduler.get_jobs():
        logger.info("  job: %s | next: %s", job.name, getattr(job, "next_run_time", "pending"))

    # Catch up any data gaps from downtime before the regular schedule begins.
    _startup_gap_check(checks, logger)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    main()
