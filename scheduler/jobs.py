"""
Scheduler entry point. Run this as a standalone process alongside the Go API.

    python -m scheduler.jobs

All times are US/Eastern. Cron triggers fire in that timezone unless noted.
"""

import logging
import logging.config

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
from config.settings import LOG_PATH, NOTIFY_API_URL, INTERNAL_API_KEY

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


def _build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone=ET)

    # --- Data collectors ---

    # Front month + forward curve: every 30 minutes during market hours Mon-Fri
    scheduler.add_job(
        PriceCollector().run,
        CronTrigger(day_of_week="mon-fri", hour="9-17", minute="0,30", timezone=ET),
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
    scheduler.add_job(
        EIAStorageCollector().run,
        CronTrigger(day_of_week="thu", hour=10, minute=45, timezone=ET),
        id="eia_storage",
        name="EIA weekly storage report",
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

    # News Wire: every 15 minutes (EIA, FERC RSS feeds — free public)
    scheduler.add_job(
        NewsWireCollector().run,
        CronTrigger(minute="2,17,32,47"),
        id="news_wire",
        name="News Wire (EIA / FERC RSS feeds)",
        misfire_grace_time=300,
    )

    # AIS LNG vessel tracking is handled by the Go cmd/ais binary (persistent
    # WebSocket connection). No Python job needed here.

    # --- Feature recomputation (staggered across the hour to avoid DB contention) ---

    scheduler.add_job(
        _notify_after(compute_price_features, "feat_price"),
        CronTrigger(minute=10),
        id="feat_price",
        name="Price + curve features",
    )
    scheduler.add_job(
        _notify_after(compute_storage_features, "feat_storage"),
        CronTrigger(minute=15),
        id="feat_storage",
        name="Storage features (deficit, EOS projection)",
    )
    scheduler.add_job(
        _notify_after(compute_weather_features, "feat_weather"),
        CronTrigger(minute=20),
        id="feat_weather",
        name="Weather features (HDD, revision delta)",
    )
    scheduler.add_job(
        _notify_after(compute_cpc_features, "feat_cpc"),
        CronTrigger(minute=22),
        id="feat_cpc",
        name="CPC outlook features (6-10 / 8-14 day weighted prob below)",
    )
    scheduler.add_job(
        _notify_after(compute_cot_features, "feat_cot"),
        CronTrigger(minute=25),
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

    # LNG export features: every 10 minutes (Go cmd/ais writes vessel data every 5 min)
    scheduler.add_job(
        _notify_after(compute_lng_features, "feat_lng"),
        CronTrigger(minute="5,15,25,35,45,55"),
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

    # Market Brief: Gemini synthesis of all signals — runs after fairvalue at :45
    # No-op when GEMINI_API_KEY is not set.
    scheduler.add_job(
        _notify_after(compute_market_brief, "market_brief"),
        CronTrigger(minute=45),
        id="market_brief",
        name="Market Brief (Gemini multi-signal synthesis)",
    )

    return scheduler


def main() -> None:
    logging.config.dictConfig(_LOGGING_CONFIG)
    logger = logging.getLogger("scheduler")

    scheduler = _build_scheduler()

    logger.info("Scheduler starting — %d jobs registered", len(scheduler.get_jobs()))
    for job in scheduler.get_jobs():
        logger.info("  job: %s | next: %s", job.name, getattr(job, "next_run_time", "pending"))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    main()
