import logging
from datetime import date, datetime, timezone

import duckdb

from config.settings import DB_PATH, connect_db

logger = logging.getLogger("collectors")

MONTH_CODES: list[str] = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

# Minimum $0.10/MMBtu spread to consider a winter premium meaningful
_SPREAD_BULLISH_THRESHOLD  = 0.50
_SPREAD_NEUTRAL_THRESHOLD  = 0.20
_SPREAD_BEARISH_THRESHOLD  = -0.10

# LNG export economics: all-in cost from US wellhead to European buyer.
# Includes liquefaction (~$2.25-$2.75) + shipping (~$0.50-$1.00).
# Midpoint of ranges from roadmap; recalibrate as market conditions change.
_TTF_SHIPPING_AND_LIQ_USD_MMBTU = 3.00

# Arbitrage spread interpretation thresholds (USD/MMBtu net-back vs HH spot)
_ARB_STRONG_PULL  = 0.50   # strong export demand; bullish for domestic market
_ARB_NEUTRAL_HIGH = 0.20
_ARB_NEUTRAL_LOW  = -0.20
_ARB_CLOSED       = -0.50  # arbitrage closed; export softness

_UPSERT_SQL = """
    INSERT INTO features_daily
        (feature_date, feature_name, region, value,
         interpretation, confidence, computed_at)
    VALUES (?, ?, 'US', ?, ?, ?, ?)
    ON CONFLICT (feature_date, feature_name, region)
    DO UPDATE SET value = excluded.value,
                 interpretation = excluded.interpretation,
                 computed_at = excluded.computed_at
"""


def compute_price_features() -> None:
    conn = connect_db()
    today = date.today()
    now = datetime.now(timezone.utc).isoformat()

    try:
        _compute_momentum(conn, today, now)
        _compute_curve_spreads(conn, today, now)
        _compute_ttf_netback(conn, today, now)
    finally:
        conn.close()


def _compute_momentum(conn, today: date, now: str) -> None:
    rows = conn.execute("""
        SELECT observation_time::DATE AS obs_date, value
        FROM facts_time_series
        WHERE source_name IN ('yfinance', 'fred')
          AND series_name IN ('ng_front_close', 'ng_spot_price')
        ORDER BY obs_date DESC
        LIMIT 22
    """).fetchall()

    if len(rows) < 2:
        return

    latest = rows[0][1]
    prior  = rows[1][1]
    wk_ago = rows[5][1]  if len(rows) > 5  else None
    mo_ago = rows[21][1] if len(rows) > 21 else None

    momentum = [
        ("ng_price_current",         latest,                   "neutral"),
        ("ng_price_daily_chg_pct",   _pct(latest, prior),      _price_interp(_pct(latest, prior))),
        ("ng_price_weekly_chg_pct",  _pct(latest, wk_ago),     _price_interp(_pct(latest, wk_ago))),
        ("ng_price_monthly_chg_pct", _pct(latest, mo_ago),     _price_interp(_pct(latest, mo_ago))),
    ]
    for name, value, interp in momentum:
        conn.execute(_UPSERT_SQL, [today, name, value, interp, "high", now])


def _compute_curve_spreads(conn, today: date, now: str) -> None:
    curve_rows = conn.execute("""
        SELECT series_name, value
        FROM facts_time_series
        WHERE source_name = 'yfinance'
          AND series_name LIKE 'ng_curve_%'
          AND observation_time >= NOW() - INTERVAL '2 hours'
        ORDER BY observation_time DESC
    """).fetchall()

    # Build a lookup keyed by contract code (e.g. "H26" -> price)
    curve: dict[str, float] = {
        r[0].replace("ng_curve_ng", "").replace(".nym", "").upper(): r[1]
        for r in curve_rows
    }

    nov = _get_contract(curve, "X", today)
    jan = _get_contract(curve, "F", today, offset_years=1)
    mar = _get_contract(curve, "H", today)
    oct = _get_contract(curve, "V", today)

    front_price  = curve_rows[0][1] if curve_rows else None
    strip_prices = [v for v in curve.values() if v and v > 0]
    strip_avg    = sum(strip_prices) / len(strip_prices) if strip_prices else None

    spreads = [
        ("ng_nov_jan_spread",  (nov - jan) if (nov and jan) else None,        _spread_interp((nov - jan) if (nov and jan) else None)),
        ("ng_mar_oct_spread",  (mar - oct) if (mar and oct) else None,        "neutral"),
        ("ng_12m_strip_avg",   strip_avg,                                      "neutral"),
        ("ng_strip_vs_spot",   (strip_avg - front_price) if (strip_avg and front_price) else None, "neutral"),
    ]
    for name, value, interp in spreads:
        if value is None:
            continue
        conn.execute(_UPSERT_SQL, [today, name, value, interp, "medium", now])


def _get_contract(
    curve: dict[str, float],
    month_code: str,
    today: date,
    offset_years: int = 0,
) -> float | None:
    month_num = MONTH_CODES.index(month_code) + 1
    year = today.year + offset_years
    if month_num <= today.month and offset_years == 0:
        year += 1
    key = f"{month_code}{str(year)[2:]}"
    return curve.get(key)


def _pct(new: float | None, old: float | None) -> float | None:
    if new is None or old is None or old == 0:
        return None
    return (new - old) / old * 100


def _price_interp(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct > 3:
        return "bullish"
    if pct > 0:
        return "mildly_bullish"
    if pct > -3:
        return "mildly_bearish"
    return "bearish"


def _spread_interp(spread: float | None) -> str:
    if spread is None:
        return "unknown"
    if spread > _SPREAD_BULLISH_THRESHOLD:
        return "bullish"
    if spread > _SPREAD_NEUTRAL_THRESHOLD:
        return "mildly_bullish"
    if spread > _SPREAD_BEARISH_THRESHOLD:
        return "neutral"
    return "bearish"


def _compute_ttf_netback(conn, today: date, now: str) -> None:
    """
    Compute the TTF → Henry Hub LNG export arbitrage spread.

    FRED series PNGASEUUSDM is already denominated in USD/MMBtu (IMF
    commodity price, European natural gas), so no unit conversion is needed.

    Writes to features_daily:
      ttf_spot_usd_mmbtu  — raw TTF price (USD/MMBtu, monthly cadence)
      ttf_hh_net_back     — TTF minus all-in LNG export cost (USD/MMBtu)
      ttf_hh_arb_spread   — net-back minus Henry Hub spot (USD/MMBtu)
                            >0 = arbitrage open (export pull); <0 = closed
    """
    ttf_row = conn.execute("""
        SELECT value
        FROM facts_time_series
        WHERE source_name = 'fred' AND series_name = 'ttf_spot'
        ORDER BY observation_time DESC
        LIMIT 1
    """).fetchone()

    hh_row = conn.execute("""
        SELECT value
        FROM facts_time_series
        WHERE source_name IN ('fred', 'yfinance')
          AND series_name IN ('ng_spot_price', 'ng_front_close')
        ORDER BY observation_time DESC
        LIMIT 1
    """).fetchone()

    if not ttf_row or not hh_row:
        return

    ttf_usd   = ttf_row[0]
    hh_spot   = hh_row[0]
    net_back  = ttf_usd - _TTF_SHIPPING_AND_LIQ_USD_MMBTU
    arb_spread = net_back - hh_spot

    features = [
        ("ttf_spot_usd_mmbtu", ttf_usd,    "neutral"),
        ("ttf_hh_net_back",    net_back,    "neutral"),
        ("ttf_hh_arb_spread",  arb_spread,  _arb_interp(arb_spread)),
    ]
    for name, value, interp in features:
        conn.execute(_UPSERT_SQL, [today, name, value, interp, "medium", now])


def _arb_interp(spread: float) -> str:
    """
    Interpret the TTF net-back vs Henry Hub spread.

    Positive spread means export economics are open — every available
    terminal runs at full capacity, which is a structural demand pull
    that tightens the domestic balance (bullish for HH price).
    Negative spread means the arb is closed (bearish demand signal).
    """
    if spread > _ARB_STRONG_PULL:
        return "bullish"
    if spread > _ARB_NEUTRAL_HIGH:
        return "mildly_bullish"
    if spread > _ARB_NEUTRAL_LOW:
        return "neutral"
    if spread > _ARB_CLOSED:
        return "mildly_bearish"
    return "bearish"
