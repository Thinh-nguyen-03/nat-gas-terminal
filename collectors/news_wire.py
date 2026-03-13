"""
News Wire collector — v2.

Pipeline:
  1. Fetch all 10 RSS/query feeds
  2. Parse articles, deduplicate against DB (skip already-stored IDs)
  3. Batch new articles (≤20) → Gemini AI scoring
  4. Drop irrelevant; upsert relevant ones with AI-generated implication
  5. Notify SSE broker

Runs every 15 minutes via the scheduler.
"""

from __future__ import annotations

import hashlib
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import NamedTuple

import duckdb
import requests

from collectors.base import CollectorBase
from config.settings import DB_PATH, GEMINI_API_KEY, GEMINI_MODEL, connect_db

logger = logging.getLogger("collectors")

_FEEDS: list[tuple[str, str]] = [
    # Direct RSS — specific publishers
    ("EIA",              "https://www.eia.gov/rss/todayinenergy.xml"),
    ("EIA-PR",           "https://www.eia.gov/rss/press_rss.xml"),
    ("EIA-NEW",          "https://www.eia.gov/about/new/WNtest3.php"),
    ("OilPrice",         "https://oilprice.com/rss/main"),
    ("Rigzone",          "https://www.rigzone.com/news/rss/rigzone_latest.aspx"),
    ("NGI",              "https://www.naturalgasintel.com/feed/"),
    # Google News queries — aggregates Reuters, FT, WSJ, Bloomberg, etc.
    ("GNews:NG Price",   "https://news.google.com/rss/search?q=natural+gas+price&hl=en-US&gl=US&ceid=US:en"),
    ("GNews:LNG",        "https://news.google.com/rss/search?q=LNG+exports+US&hl=en-US&gl=US&ceid=US:en"),
    ("GNews:HH",         "https://news.google.com/rss/search?q=Henry+Hub&hl=en-US&gl=US&ceid=US:en"),
    ("GNews:Storage",    "https://news.google.com/rss/search?q=EIA+natural+gas+storage&hl=en-US&gl=US&ceid=US:en"),
]

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_ATOM_NS = "http://www.w3.org/2005/Atom"


class Article(NamedTuple):
    item_id:     str
    source:      str
    title:       str
    url:         str
    pub_ts:      str | None
    description: str  # truncated to 500 chars for AI prompt


def _parse_feed(source: str, xml_text: str) -> list[Article]:
    """Parse RSS 2.0 or Atom XML; return list of Article objects."""
    articles: list[Article] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.warning("[news_wire] %s XML parse error: %s", source, e)
        return articles

    items = root.findall(".//item") or root.findall(f".//{{{_ATOM_NS}}}entry")

    if not items:
        # Log root tag so we can tell if the feed returned an error page,
        # an empty channel, or an unexpected XML structure.
        logger.debug("[news_wire] %s: 0 items in XML (root tag: %s)", source, root.tag)

    for item in items:
        def _t(rss_tag: str, atom_tag: str | None = None) -> str:
            el = item.find(rss_tag)
            if el is None and atom_tag:
                el = item.find(f"{{{_ATOM_NS}}}{atom_tag}")
            return (el.text or "").strip() if el is not None else ""

        title   = _t("title")
        link_el = item.find("link")
        if link_el is None:
            link_el = item.find(f"{{{_ATOM_NS}}}link")
        link    = ""
        if link_el is not None:
            link = (link_el.text or link_el.get("href") or "").strip()
        desc    = _t("description") or _t("summary", "summary")
        pub_raw = _t("pubDate") or _t("published", "published") or _t("updated", "updated")

        if not title or not link:
            continue

        item_id = hashlib.sha1(link.encode()).hexdigest()[:16]

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

        articles.append(Article(
            item_id=item_id,
            source=source,
            title=title,
            url=link,
            pub_ts=pub_ts,
            description=desc[:500],
        ))

    return articles


# Prompt prefix — articles JSON is concatenated directly to avoid .format() escaping issues
_SCORE_PROMPT_PREFIX = """\
You are a natural gas market analyst. For each article below, determine whether it is relevant \
to US natural gas prices, LNG exports, storage reports, weather demand, pipeline operations, \
or geopolitical events that affect energy supply/demand.

Return a JSON array with exactly one object per article, in the same order as the input:
[
  {
    "relevant": true,
    "sentiment": "bullish",
    "score": 75,
    "implication": "Bullish — cold snap across Midwest expected to drive storage withdrawals above 5-year average pace."
  },
  {
    "relevant": false,
    "sentiment": "neutral",
    "score": 0,
    "implication": ""
  }
]

Rules:
- score: integer 0–100 (0=irrelevant, 50=notable, 100=major market-moving event)
- sentiment: exactly "bullish", "bearish", or "neutral"
- implication: one sentence starting with "Bullish —", "Bearish —", or "Neutral —"; empty if not relevant
- Return ONLY the JSON array, no other text

Articles:
"""


def _score_with_gemini(articles: list[Article]) -> list[dict]:
    """Send a batch of articles to Gemini for relevance + sentiment scoring.

    Returns a list of score dicts in the same order as input.
    Falls back to neutral/score=0 on any failure.
    """
    fallback = [{"relevant": True, "sentiment": "neutral", "score": 0, "implication": ""} for _ in articles]

    if not GEMINI_API_KEY:
        return fallback

    try:
        from google import genai
        from google.genai.types import GenerateContentConfig

        client = genai.Client(api_key=GEMINI_API_KEY)

        articles_json = json.dumps(
            [{"title": a.title, "description": a.description} for a in articles],
            ensure_ascii=False,
        )
        prompt = _SCORE_PROMPT_PREFIX + articles_json

        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.1,
            ),
        )

        results = json.loads(response.text)
        if not isinstance(results, list) or len(results) != len(articles):
            logger.warning(
                "[news_wire] Gemini returned %s results for %d articles",
                len(results) if isinstance(results, list) else "invalid",
                len(articles),
            )
            return fallback

        return results

    except Exception as e:
        logger.warning("[news_wire] Gemini scoring failed: %s", e)
        return fallback


_UPSERT_SQL = """
    INSERT INTO news_items
        (id, source, title, url, published_at, fetched_at, score, sentiment, tags, implication)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (id) DO NOTHING
"""


class NewsWireCollector(CollectorBase):
    source_name = "news_wire"

    def collect(self) -> dict:
        # Brief DB read — connection held for <1s then released before network I/O.
        conn = connect_db()
        try:
            existing_ids: set[str] = {
                row[0] for row in conn.execute("SELECT id FROM news_items").fetchall()
            }
        finally:
            conn.close()

        # RSS fetching + Gemini scoring — no DB connection held during network I/O.
        all_articles: list[Article] = []
        seen_ids: set[str] = set()
        all_feed_ids: set[str] = set()
        for source, url in _FEEDS:
            try:
                resp = requests.get(url, timeout=20, headers={"User-Agent": _UA})
                resp.raise_for_status()
                logger.debug("[news_wire] %s: HTTP %d, %d bytes, content-type=%s",
                             source, resp.status_code, len(resp.content),
                             resp.headers.get("Content-Type", "?")[:40])
                raw = _parse_feed(source, resp.text)
                all_feed_ids.update(a.item_id for a in raw)
                new = [
                    a for a in raw
                    if a.item_id not in existing_ids and a.item_id not in seen_ids
                ]
                seen_ids.update(a.item_id for a in new)
                all_articles.extend(new)
                logger.info("[news_wire] %s: %d new (of %d fetched)", source, len(new), len(raw))
            except Exception as e:
                logger.warning("[news_wire] %s fetch failed: %s", source, e)

        now_str = datetime.now(timezone.utc).isoformat()

        # Score new articles via Gemini (still no DB held)
        scored_rows: list[tuple] = []
        if all_articles:
            logger.info("[news_wire] scoring %d new articles via Gemini", len(all_articles))
            batch_size = 20
            for i in range(0, len(all_articles), batch_size):
                batch  = all_articles[i : i + batch_size]
                scores = _score_with_gemini(batch)
                for article, scored in zip(batch, scores):
                    if not scored.get("relevant", True):
                        continue
                    scored_rows.append((
                        article.item_id, article.source, article.title,
                        article.url, article.pub_ts, now_str,
                        float(scored.get("score", 0)),
                        scored.get("sentiment", "neutral"),
                        "",
                        scored.get("implication") or None,
                    ))

        # Brief DB write — connection held for <1s.
        written = 0
        conn = connect_db()
        try:
            for row in scored_rows:
                conn.execute(_UPSERT_SQL, list(row))
                written += 1

            still_in_feed = all_feed_ids & existing_ids
            if still_in_feed:
                placeholders = ", ".join(["?"] * len(still_in_feed))
                conn.execute(
                    f"UPDATE news_items SET fetched_at = ? WHERE id IN ({placeholders})",
                    [now_str] + list(still_in_feed),
                )
                logger.info("[news_wire] refreshed fetched_at for %d existing articles", len(still_in_feed))
        finally:
            conn.close()

        logger.info("[news_wire] wrote %d relevant articles (of %d new)", written, len(all_articles))
        return {"status": "ok", "items_written": written}
