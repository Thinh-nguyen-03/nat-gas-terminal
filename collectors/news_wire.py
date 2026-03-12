"""
News Wire collector.

Fetches free public RSS feeds relevant to natural gas markets, scores each
headline for relevance and sentiment, and upserts into the news_items table.

Runs every 15 minutes via the scheduler.

RSS sources:
  EIA Today in Energy  — https://www.eia.gov/rss/todayinenergy.xml
  FERC Press Releases  — https://www.ferc.gov/rss/news.xml
"""

from __future__ import annotations

import hashlib
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import duckdb
import requests

from collectors.base import CollectorBase
from config.settings import DB_PATH

logger = logging.getLogger("collectors")

# ---------------------------------------------------------------------------
# RSS feed registry
# ---------------------------------------------------------------------------

_FEEDS: list[tuple[str, str]] = [
    # EIA Today in Energy — confirmed public RSS, updated daily
    ("EIA", "https://www.eia.gov/rss/todayinenergy.xml"),
]

# Browser-like UA is required by some government feeds (e.g. FERC)
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Keyword scoring table: (keyword, weight, direction)
# direction in 'bullish' | 'bearish' | 'neutral'
# ---------------------------------------------------------------------------

_KEYWORDS: list[tuple[str, float, str]] = [
    # --- bullish signals ---
    ("cold snap",         20, "bullish"),
    ("polar vortex",      20, "bullish"),
    ("blizzard",          15, "bullish"),
    ("freeze",            12, "bullish"),
    ("below normal temperatures", 12, "bullish"),
    ("storage deficit",   15, "bullish"),
    ("force majeure",     15, "bullish"),
    ("pipeline outage",   15, "bullish"),
    ("curtailment",       12, "bullish"),
    ("supply disruption", 12, "bullish"),
    ("export surge",      10, "bullish"),
    ("higher demand",     10, "bullish"),
    # --- bearish signals ---
    ("warm weather",      12, "bearish"),
    ("above normal temperatures", 12, "bearish"),
    ("mild",               8, "bearish"),
    ("storage surplus",   12, "bearish"),
    ("ample supply",      10, "bearish"),
    ("weak demand",       12, "bearish"),
    ("record production", 10, "bearish"),
    ("oversupply",        12, "bearish"),
    ("injection season",   8, "bearish"),
    # --- relevance (neutral) ---
    ("natural gas",        5, "neutral"),
    ("henry hub",         12, "neutral"),
    ("storage report",    10, "neutral"),
    ("lng exports",       12, "neutral"),
    ("lng",                8, "neutral"),
    ("pipeline",           6, "neutral"),
    ("gas prices",         8, "neutral"),
    ("gas demand",         8, "neutral"),
    ("gas production",     6, "neutral"),
    ("eia storage",       10, "neutral"),
    ("ferc",               6, "neutral"),
    ("natural gas prices", 10, "neutral"),
    ("gas-fired",          6, "neutral"),
]

_ATOM_NS = "http://www.w3.org/2005/Atom"


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score(title: str, description: str) -> tuple[float, str, str]:
    """Return (score, sentiment, comma_separated_tags)."""
    text = (title + " " + description).lower()
    total = 0.0
    bull = 0.0
    bear = 0.0
    tags: list[str] = []

    for keyword, weight, direction in _KEYWORDS:
        if keyword in text:
            total += weight
            tags.append(keyword)
            if direction == "bullish":
                bull += weight
            elif direction == "bearish":
                bear += weight

    score = min(total, 100.0)
    if bull >= 10 and bull > bear:
        sentiment = "bullish"
    elif bear >= 10 and bear > bull:
        sentiment = "bearish"
    else:
        sentiment = "neutral"

    return score, sentiment, ",".join(tags)


# ---------------------------------------------------------------------------
# RSS / Atom parser
# ---------------------------------------------------------------------------

def _parse_feed(source: str, xml_text: str) -> list[tuple]:
    """Parse RSS 2.0 or Atom XML; return list of DB row tuples."""
    rows: list[tuple] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.warning("[news_wire] %s XML parse error: %s", source, e)
        return rows

    now_str = datetime.now(timezone.utc).isoformat()

    # RSS 2.0 <item> or Atom <entry>
    items = root.findall(".//item") or root.findall(f".//{{{_ATOM_NS}}}entry")

    for item in items:
        def _t(rss_tag: str, atom_tag: str | None = None) -> str:
            el = item.find(rss_tag)
            if el is None and atom_tag:
                el = item.find(f"{{{_ATOM_NS}}}{atom_tag}")
            return (el.text or "").strip() if el is not None else ""

        title   = _t("title")
        # Atom <link> is an element with href attribute, not text
        link_el = item.find("link") or item.find(f"{{{_ATOM_NS}}}link")
        link = ""
        if link_el is not None:
            link = (link_el.text or link_el.get("href") or "").strip()
        desc    = _t("description") or _t("summary", "summary")
        pub_raw = _t("pubDate") or _t("published", "published") or _t("updated", "updated")

        if not title or not link:
            continue

        item_id = hashlib.sha1(link.encode()).hexdigest()[:16]

        # Parse publish timestamp — try RFC 2822 then ISO 8601
        pub_ts: str | None = None
        if pub_raw:
            try:
                pub_ts = parsedate_to_datetime(pub_raw).astimezone(timezone.utc).isoformat()
            except Exception:
                try:
                    pub_ts = datetime.fromisoformat(
                        pub_raw.replace("Z", "+00:00")
                    ).astimezone(timezone.utc).isoformat()
                except Exception:
                    pass

        score, sentiment, tags = _score(title, desc)
        if score == 0:
            continue  # irrelevant to nat-gas markets

        rows.append((item_id, source, title, link, pub_ts, now_str, score, sentiment, tags))

    return rows


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

_UPSERT_SQL = """
    INSERT INTO news_items
        (id, source, title, url, published_at, fetched_at, score, sentiment, tags)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (id) DO UPDATE SET
        fetched_at = excluded.fetched_at,
        score      = excluded.score,
        sentiment  = excluded.sentiment
"""


class NewsWireCollector(CollectorBase):
    source_name = "news_wire"

    def collect(self) -> dict:
        conn = duckdb.connect(DB_PATH)
        written = 0
        try:
            for source, url in _FEEDS:
                try:
                    resp = requests.get(
                        url, timeout=20,
                        headers={"User-Agent": _UA},
                    )
                    resp.raise_for_status()
                    rows = _parse_feed(source, resp.text)
                    for row in rows:
                        conn.execute(_UPSERT_SQL, list(row))
                        written += 1
                    logger.info("[news_wire] %s: %d relevant items", source, len(rows))
                except Exception as e:
                    logger.warning("[news_wire] %s fetch failed: %s", source, e)
        finally:
            conn.close()
        return {"status": "ok", "items_written": written}
