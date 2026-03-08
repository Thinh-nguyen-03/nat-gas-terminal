import logging
from datetime import datetime, timezone

import duckdb
import pandas as pd
import yfinance as yf
from fredapi import Fred

from collectors.base import CollectorBase
from config.settings import DB_PATH, FRED_API_KEY

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
    """Return the next n_months of NYMEX NG futures tickers in Yahoo Finance format."""
    today = datetime.now(timezone.utc)
    year, month = today.year, today.month
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
        records_written = 0
        conn = duckdb.connect(DB_PATH)
        now_str = datetime.now(timezone.utc).isoformat()

        try:
            records_written += self._collect_front_month(conn, now_str)
            records_written += self._collect_forward_curve(conn, now_str)
            records_written += self._collect_fred_spot(conn, now_str)
        finally:
            conn.close()

        return {"status": "ok", "records_written": records_written}

    def _collect_front_month(self, conn, now_str: str) -> int:
        """Collect 90 days of OHLCV for the front-month NG=F contract."""
        # yfinance >=1.0 returns a MultiIndex DataFrame from download()
        hist = yf.download("NG=F", period=HISTORY_PERIOD, interval="1d",
                           progress=False, auto_adjust=True)
        if hist.empty:
            logger.warning("[price] yfinance returned no data for NG=F")
            return 0

        # Flatten MultiIndex columns: (Price, Ticker) -> Price
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)

        self.save_raw(hist.reset_index().to_json(), subdir="price")

        count = 0
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
                conn.execute(_INSERT_SQL, [
                    "yfinance",
                    f"ng_front_{field.lower()}",
                    obs_time,
                    now_str,
                    float(val),
                    unit,
                    "daily",
                ])
                count += 1
        return count

    def _collect_forward_curve(self, conn, now_str: str) -> int:
        """Snapshot the 13-month forward curve from Yahoo Finance."""
        obs_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        tickers = build_contract_tickers()
        curve_snapshot: dict[str, float] = {}
        count = 0

        # Batch download is more reliable than per-ticker fast_info
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
                    conn.execute(_INSERT_SQL, [
                        "yfinance",
                        series_name,
                        obs_time,
                        now_str,
                        float(price),
                        "USD/MMBtu",
                        "intraday",
                    ])
                    curve_snapshot[ticker_str] = float(price)
                    count += 1
                except Exception as e:
                    logger.warning("[price] curve contract %s failed: %s", ticker_str, e)
                    continue

        self.save_raw(curve_snapshot, subdir="price")
        return count

    def _collect_fred_spot(self, conn, now_str: str) -> int:
        """Collect FRED spot price series (Henry Hub + heating oil)."""
        fred = Fred(api_key=FRED_API_KEY)
        count = 0
        for fred_id, (series_name, unit) in FRED_SERIES.items():
            try:
                series = fred.get_series(fred_id, observation_start=FRED_START)
                for date, value in series.items():
                    if value != value:  # NaN
                        continue
                    obs_time = date.strftime("%Y-%m-%dT00:00:00Z")
                    conn.execute(_INSERT_SQL, [
                        "fred", series_name, obs_time, now_str, float(value), unit, "daily",
                    ])
                    count += 1
            except Exception as e:
                logger.warning("[price] FRED series %s failed: %s", fred_id, e)
        return count
