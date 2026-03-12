"""
Market Brief transform.

Pulls the latest fundamental signals from the database, builds a structured
prompt, calls Gemini, and stores the JSON result in summary_outputs under
summary_type='market_brief'.

Skips silently when GEMINI_API_KEY is not configured.
Runs every 30 minutes via the scheduler (after summary at :30).
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone

import duckdb

from config.settings import DB_PATH, GEMINI_API_KEY, GEMINI_MODEL

logger = logging.getLogger("collectors")

_UPSERT_SQL = """
    INSERT INTO summary_outputs
        (summary_date, summary_type, content, generated_at)
    VALUES (?, 'market_brief', ?, ?)
    ON CONFLICT (summary_date, summary_type)
    DO UPDATE SET content = excluded.content,
                 generated_at = excluded.generated_at
"""


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_feature(name: str, conn, days_back: int = 7) -> float | None:
    row = conn.execute("""
        SELECT value FROM features_daily
        WHERE feature_name = ? AND region = 'US'
          AND feature_date >= CURRENT_DATE - ?::INTEGER
        ORDER BY feature_date DESC LIMIT 1
    """, [name, days_back]).fetchone()
    return row[0] if row else None


def _get_score(conn) -> tuple[float | None, str]:
    row = conn.execute("""
        SELECT content FROM summary_outputs
        WHERE summary_type = 'fundamental_score'
        ORDER BY summary_date DESC LIMIT 1
    """).fetchone()
    if not row:
        return None, "Neutral"
    try:
        d = json.loads(row[0])
        return d.get("score"), d.get("label", "Neutral")
    except Exception:
        return None, "Neutral"


def _get_recent_news(conn, n: int = 5) -> list[str]:
    rows = conn.execute("""
        SELECT title, sentiment FROM news_items
        WHERE fetched_at >= NOW() - INTERVAL '24 hours'
        ORDER BY score DESC LIMIT ?
    """, [n]).fetchall()
    return [f"[{r[1].upper()}] {r[0]}" for r in rows]


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

def _fmt(v: float | None, unit: str = "", precision: int = 1) -> str:
    return f"{v:.{precision}f}{unit}" if v is not None else "N/A"


def _build_prompt(conn, today: date) -> str:
    score, score_label = _get_score(conn)

    price   = _get_feature("ng_price_current", conn)
    deficit = _get_feature("storage_deficit_vs_5yr_bcf", conn)
    eos     = _get_feature("storage_eos_projection_bcf", conn)
    hdd     = _get_feature("weather_hdd_7d_weighted", conn)
    hdd_rev = _get_feature("weather_hdd_revision_delta", conn)
    lng     = _get_feature("lng_implied_exports_bcfd", conn)
    epi     = _get_feature("lng_export_pressure_index", conn)
    prod    = _get_feature("dry_gas_production_bcfd", conn)
    mm_pct  = _get_feature("cot_mm_net_pct_oi", conn)
    stress  = _get_feature("power_demand_stress_index", conn)

    news = _get_recent_news(conn)
    news_block = "\n".join(f"  • {h}" for h in news) or "  • No recent headlines"

    deficit_dir = "deficit" if (deficit is not None and deficit < 0) else "surplus"

    return f"""You are a natural gas market analyst. Today is {today.isoformat()}.

CURRENT MARKET SIGNALS:
- Fundamental Score: {_fmt(score, '', 1)} ({score_label})
- Henry Hub Price: ${_fmt(price, '/MMBtu')}
- Storage vs 5yr Avg: {_fmt(deficit, ' Bcf')} ({deficit_dir})
- EOS Storage Projection: {_fmt(eos, ' Bcf', 0)}
- Weather HDD (7-day weighted): {_fmt(hdd)}
- HDD Forecast Revision: {_fmt(hdd_rev, ' HDD')}
- LNG Implied Exports: {_fmt(lng, ' Bcfd')}
- LNG Export Pressure Index: {_fmt(epi, '/100', 0)}
- Dry Gas Production: {_fmt(prod, ' Bcfd')}
- Managed Money COT: {_fmt(mm_pct, '% of OI')}
- Power Demand Stress: {_fmt(stress, '/100', 0)}

RECENT HEADLINES:
{news_block}

Write a structured market brief. Respond with a JSON object containing exactly these three keys:
- "outlook": one sentence on near-term natural gas price direction (specific, no hedging)
- "drivers": array of exactly 3 short bullet strings citing specific numbers
- "risk": one sentence on the main tail risk that could flip the outlook"""


# ---------------------------------------------------------------------------
# Main transform
# ---------------------------------------------------------------------------

def compute_market_brief() -> None:
    """Generate a Gemini-powered market brief and store it in summary_outputs."""
    if not GEMINI_API_KEY:
        logger.info("[market_brief] GEMINI_API_KEY not set — skipping")
        return

    try:
        from google import genai
        from google.genai import types as genai_types
    except ImportError:
        logger.error("[market_brief] google-genai not installed — run: pip install google-genai")
        return

    client = genai.Client(api_key=GEMINI_API_KEY)

    conn = duckdb.connect(DB_PATH)
    today = date.today()
    now = datetime.now(timezone.utc).isoformat()

    try:
        prompt = _build_prompt(conn, today)
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.3,
            ),
        )

        parsed = json.loads(response.text)
        content = json.dumps({
            "outlook": str(parsed.get("outlook", "")),
            "drivers": list(parsed.get("drivers", [])),
            "risk":    str(parsed.get("risk", "")),
            "model":   GEMINI_MODEL,
            "generated_at": now,
        })

        conn.execute(_UPSERT_SQL, [today, content, now])
        logger.info("[market_brief] brief stored for %s", today)

    except Exception as e:
        logger.error("[market_brief] failed: %s", e)
    finally:
        conn.close()
