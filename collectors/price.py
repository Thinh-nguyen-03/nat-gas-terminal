import logging
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from fredapi import Fred

from collectors.base import CollectorBase
from config.settings import FRED_API_KEY, connect_db

logger = logging.getLogger("collectors")

# NYMEX NG month codes — used to build forward curve contract tickers
# F=Jan G=Feb H=Mar J=Apr K=May M=Jun N=Jul Q=Aug U=Sep V=Oct X=Nov Z=Dec
MONTH_CODES: list[str] = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

CURVE_MONTHS = 13      # how many forward months to collect
HISTORY_PERIOD = "90d"  # yfinance lookback for front-month OHLCV
FRED_START = "2023-01-01"

# FRED series to collect: {series_id: (series_name, unit)}
FRED_SERIES: dict[str, tuple[str, str]] = {
    "DHHNGSP":     ("ng_spot_price",    "USD/MMBtu"),
    "DHOILNYH":    ("heating_oil_spot", "USD/gal"),
    # European TTF benchmark (IMF series, USD/MMBtu, monthly cadence).
    # Used to compute the US LNG export arbitrage spread in features_price.py.
    "PNGASEUUSDM": ("ttf_spot",         "USD/MMBtu"),
}

_OHLCV_FIELDS = ["Open", "High", "Low", "Close", "Volume"]

_INSERT_SQL = """
    INSERT INTO facts_time_series
        (source_name, series_name, region, observation_time,
         ingest_time, value, unit, frequency)
    VALUES (?, ?, 'US', ?, ?, ?, ?, ?)
    ON CONFLICT (source_name, series_name, region, observation_time)
    DO UPDATE SET value = excluded.value, ingest_time = excluded.ingest_time
"""


def build_contract_tickers(n_months: int = CURVE_MONTHS) -> list[str]:
    """Return the next n_months of NYMEX NG futures tickers in Yahoo Finance format.

    NYMEX NG contracts expire ~3 business days before end of the month prior to
    delivery (typically around the 22nd-26th). By the time the calendar month
    rolls over, the current-month delivery contract is already expired. Use an
    offset of +1 normally, or +2 after the ~25th (when next-month has also rolled).
    """
    today = datetime.now(timezone.utc)
    year, month = today.year, today.month
    offset = 2 if today.day >= 25 else 1
    month += offset
    if month > 12:
        month -= 12
        year += 1
    tickers = []
    for _ in range(n_months):
        tickers.append(f"NG{MONTH_CODES[month - 1]}{str(year)[2:]}.NYM")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return tickers


class PriceCollector(CollectorBase):
    source_name = "price"

    def collect(self) -> dict:
        now_str = datetime.now(timezone.utc).isoformat()

        # Phase 1: fetch all data (no DB held during HTTP I/O)
        front_rows, front_raw = self._fetch_front_month(now_str)
        curve_rows, curve_snapshot = self._fetch_forward_curve(now_str)
        fred_rows = self._fetch_fred_spot(now_str)

        # Phase 2: write all rows in one short DB session
        all_rows = front_rows + curve_rows + fred_rows
        conn = connect_db()
        try:
            for row in all_rows:
                conn.execute(_INSERT_SQL, row)
        finally:
            conn.close()

        if front_raw:
            self.save_raw(front_raw, subdir="price")
        self.save_raw(curve_snapshot, subdir="price")

        return {"status": "ok", "records_written": len(all_rows)}

    def _fetch_front_month(self, now_str: str) -> tuple[list[list], str]:
        """Fetch 90 days of OHLCV for the current front-month contract.

        Uses the dynamically computed front-month ticker (e.g. NGJ26.NYM) instead
        of NG=F so it rolls automatically when the prompt-month contract expires.
        Tries up to 3 forward contracts in case the first is expired or illiquid.
        """
        candidates = build_contract_tickers(3)
        hist = pd.DataFrame()
        ticker_used = ""
        for ticker in candidates:
            h = yf.download(ticker, period=HISTORY_PERIOD, interval="1d",
                            progress=False, auto_adjust=True)
            if not h.empty:
                hist = h
                ticker_used = ticker
                break
            logger.warning("[price] front-month %s returned no data, trying next", ticker)

        if hist.empty:
            logger.warning("[price] no front-month data found for any candidate: %s", candidates)
            return [], ""

        logger.info("[price] front-month using %s", ticker_used)
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)

        raw_json = hist.reset_index().to_json()
        rows = []
        for idx, row in hist.iterrows():
            ts = idx
            if hasattr(ts, "tz_localize"):
                ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
            obs_time = ts.isoformat()
            for field in _OHLCV_FIELDS:
                val = row.get(field)
                if val is None or (isinstance(val, float) and val != val):
                    continue
                unit = "contracts" if field == "Volume" else "USD/MMBtu"
                rows.append(["yfinance", f"ng_front_{field.lower()}", obs_time,
                              now_str, float(val), unit, "daily"])
        return rows, raw_json

    def _fetch_forward_curve(self, now_str: str) -> tuple[list[list], dict]:
        """Snapshot the 13-month forward curve from Yahoo Finance."""
        obs_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        tickers = build_contract_tickers()
        curve_snapshot: dict[str, float] = {}
        rows = []

        data = yf.download(tickers, period="5d", interval="1d",
                           progress=False, auto_adjust=True)
        if not data.empty and "Close" in data.columns.get_level_values(0):
            close = data["Close"]
            last_prices = close.iloc[-1]
            for ticker_str in tickers:
                try:
                    price = last_prices.get(ticker_str)
                    if price is None or (isinstance(price, float) and price != price):
                        continue
                    series_name = f"ng_curve_{ticker_str.replace('.NYM', '').lower()}"
                    rows.append(["yfinance", series_name, obs_time, now_str,
                                 float(price), "USD/MMBtu", "intraday"])
                    curve_snapshot[ticker_str] = float(price)
                except Exception as e:
                    logger.warning("[price] curve contract %s failed: %s", ticker_str, e)

        return rows, curve_snapshot

    def _fetch_fred_spot(self, now_str: str) -> list[list]:
        """Fetch FRED spot price series (Henry Hub + heating oil)."""
        fred = Fred(api_key=FRED_API_KEY)
        rows = []
        for fred_id, (series_name, unit) in FRED_SERIES.items():
            try:
                series = fred.get_series(fred_id, observation_start=FRED_START)
                for date, value in series.items():
                    if value != value:  # NaN
                        continue
                    obs_time = date.strftime("%Y-%m-%dT00:00:00Z")
                    rows.append(["fred", series_name, obs_time, now_str,
                                 float(value), unit, "daily"])
            except Exception as e:
                logger.warning("[price] FRED series %s failed: %s", fred_id, e)
        return rows
