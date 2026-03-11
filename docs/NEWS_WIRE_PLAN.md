# NEWS WIRE Panel — Implementation Plan

Panel name: **NEWS WIRE**
Dashboard position: Row 4, col-span-7 (next to CATALYST CALENDAR col-span-5)
SSE event: `signal_feed`

---

## Data Sources

| Source key | URL | Label | Signal quality | Method |
|-----------|-----|-------|---------------|--------|
| `eia_press` | `https://www.eia.gov/rss/press_releases.xml` | EIA | High | RSS |
| `eia_today` | `https://www.eia.gov/rss/todayinenergy.xml` | EIA | High | RSS |
| `ferc_news` | `https://www.ferc.gov/news-releases/news-releases.xml` | FERC | Very High | RSS |
| `noaa_alerts` | `https://alerts.weather.gov/cap/us.php?x=1` | NOAA | High | RSS (CAP/XML) |
| `google_ng` | `https://news.google.com/rss/search?q=natural+gas+Henry+Hub+OR+LNG+exports&hl=en-US&gl=US&ceid=US:en` | NEWS | Medium | RSS |
| `google_ferc` | `https://news.google.com/rss/search?q=FERC+pipeline+natural+gas&hl=en-US&gl=US&ceid=US:en` | NEWS | Medium | RSS |

> Bloomberg Energy RSS is paywalled — skip.
> NOAA: filter to relevant states (TX, LA, WY, MT, CO for supply; NE, MW, NE for demand).

---

## 1. DB Schema

Add to `db/schema.py`:

```sql
CREATE TABLE IF NOT EXISTS signal_feed (
    id              VARCHAR PRIMARY KEY,   -- sha256(source + url)[:32]
    source          VARCHAR NOT NULL,      -- 'eia', 'ferc', 'noaa', 'google_news'
    source_label    VARCHAR NOT NULL,      -- display badge: 'EIA', 'FERC', 'NOAA', 'NEWS'
    title           VARCHAR NOT NULL,
    summary         VARCHAR,
    url             VARCHAR,
    published_at    TIMESTAMPTZ NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    relevance_score FLOAT,                 -- 0–100, blended keyword + AI score
    impact          VARCHAR,               -- 'high', 'medium', 'low'
    tags            VARCHAR[]              -- ['pipeline','weather','lng','storage','price','supply','demand']
)
```

Retention: purge rows older than 7 days on each collection run.

---

## 2. Collector — `collectors/news_feed.py`

Extends `CollectorBase`. Fetches all RSS sources via `feedparser`, scores each item, deduplicates by id, writes to `signal_feed`.

### Step 1 — Keyword scoring (primary, no API dependency)

```
HIGH impact (score 70–100):
  force majeure, pipeline outage, curtailment, freeze-off, polar vortex,
  explosion, fire, emergency, FERC order, LNG terminal outage,
  Sabine Pass, Freeport, Corpus Christi, Cameron LNG, Calcasieu Pass

MEDIUM impact (score 40–69):
  Henry Hub, storage draw, storage injection, production decline,
  LNG export record, pipeline capacity, basis spread, cold snap,
  winter storm, heat wave, power demand

LOW impact (score 10–39):
  natural gas, LNG, natgas, ngas, bcf/d, mcf, methane
```

Score = sum of matched keyword weights, capped at 100.
Impact tier derived from score: `>=70 → high`, `>=40 → medium`, else `low`.

### Step 2 — Gemini Flash-Lite AI scoring (optional enhancement)

After keyword scoring, if `GEMINI_API_KEY` is set, batch all collected headlines into a single Gemini API call:

**Model:** `gemini-2.0-flash-lite` (free tier: 1,500 req/day; our usage: 2 req/run × 48 runs/day = 96/day = 6% quota)

**Prompt:**
```
You are an expert natural gas commodity analyst. For each headline below,
rate its potential influence on Henry Hub natural gas spot prices from
0 (no influence) to 100 (extreme influence). Consider supply disruptions,
demand shocks, regulatory decisions, LNG export events, and weather impacts.
Return ONLY a JSON array of integers in the same order as the headlines.

Headlines:
1. [title 1]
2. [title 2]
...
```

**Score blending:** `final = 0.4 × keyword_score + 0.6 × ai_score`

Impact tier re-derived from blended score. If the API call fails or key is absent, keyword score is used as-is — the panel works without it.

**Why AI adds value here:** keyword scoring misses indirect phrasing ("Gulf Coast facilities face disruptions" scores 0 keywords but high AI) and can't distinguish direction ("FERC approves pipeline" vs "FERC rejects pipeline" — same keywords, opposite signals).

### Tag assignment

| Tag | Trigger keywords |
|-----|-----------------|
| `pipeline` | pipeline, transco, rockies, algonquin, tennessee, REX, ANR |
| `weather` | storm, blizzard, freeze, polar vortex, cold, heat, NOAA |
| `lng` | LNG, liquefied, export terminal, import terminal |
| `storage` | storage, injection, withdrawal, working gas, EIA weekly |
| `price` | Henry Hub, price, basis, spot, futures |
| `supply` | production, drilling, rig count, wells, Haynesville, Permian, Appalachia |
| `demand` | demand, consumption, industrial, power burn, residential |

### NOAA special handling

CAP alert feed uses different XML schema. Map alert types:
- Blizzard Warning / Winter Storm Warning / Ice Storm → `high`, tags: `weather`
- Freeze Watch / Wind Chill Advisory → `medium`, tags: `weather`
- Filter to: TX, LA, WY, MT, CO, ND, PA, WV, OH, NY, NE, IA (major production + demand states)

---

## 3. Go API — `api/internal/handler/signals.go`

### Route: `GET /api/signals`

Query params (optional):
- `?impact=high` — filter by impact tier
- `?source=ferc` — filter by source

Response:
```json
{
  "items": [
    {
      "id": "abc123",
      "source_label": "FERC",
      "title": "Notice of Force Majeure — Transco Zone 6",
      "summary": "Transcontinental Gas Pipe Line Company declared...",
      "url": "https://ferc.gov/...",
      "published_at": "2026-03-11T14:30:00Z",
      "relevance_score": 85.0,
      "impact": "high",
      "tags": ["pipeline", "supply"]
    }
  ],
  "updated_at": "2026-03-11T15:02:00Z"
}
```

Query: `SELECT ... FROM signal_feed ORDER BY published_at DESC LIMIT 50`
Route registration in `api/main.go`: `mux.HandleFunc("GET /api/signals", h.Signals)`

---

## 4. Scheduler — `scheduler/jobs.py`

```python
scheduler.add_job(
    NewsFeedCollector().run,
    CronTrigger(minute="5,35"),   # every 30 min
    id="news_feed",
    name="News Wire RSS collector",
    misfire_grace_time=120,
)
```

SSE notify: `signal_feed` → triggers panel refresh.

New dependencies: `feedparser`, `google-generativeai` (add to `requirements.txt`).
New env var: `GEMINI_API_KEY` (optional — feature degrades gracefully without it).

---

## 5. Frontend — `ui/components/panels/NewsWirePanel.tsx`

### Item card layout
```
[FERC]  Notice of Force Majeure — Transco Zone 6         ● HIGH
        14m ago  ·  pipeline · supply
```

### Source badge colors
| Label | Color |
|-------|-------|
| FERC | `#fbbf24` (amber) — regulatory, highest signal |
| EIA | `#22d3ee` (cyan) — data releases |
| NOAA | `#4ade80` (green) — weather |
| NEWS | `#94a3b8` (slate) — aggregated news |

### Impact dot colors
| Impact | Color |
|--------|-------|
| high | `#f87171` (red) |
| medium | `#fbbf24` (amber) |
| low | `#475569` (muted) |

### Behavior
- Scrollable list, newest at top
- Title is a clickable link (`target="_blank"`) to source URL
- Summary shown as a second line in muted text (truncated to 2 lines)
- Time shown as relative ("14m ago", "2h ago", "3d ago")
- Refreshes via SSE source `signal_feed`
- `usePanel<SignalsResponse>('/api/signals', ['signal_feed'])`

### Types to add in `ui/lib/types.ts`
```typescript
export interface SignalItem {
  id: string
  source_label: string
  title: string
  summary: string | null
  url: string | null
  published_at: string
  relevance_score: number | null
  impact: string | null
  tags: string[]
}

export interface SignalsResponse {
  items: SignalItem[]
  updated_at: string | null
}
```

---

## 6. MARKET BRIEF Panel (Phase 2)

Panel name: **MARKET BRIEF**
Dashboard position: TBD (likely Row 6, full-width or col-span-12)
SSE event: `market_brief`

A separate feature that synthesizes *all live panel data* into a 2-sentence trader-grade brief, updated every 30 minutes. This is distinct from the NEWS WIRE — it doesn't summarize headlines, it reasons across fundamentals.

### What it does

Takes structured feature data already computed by the existing transform jobs:

```
Storage deficit: -18% vs 5yr avg, draw trend accelerating
Weather: HDD 142 this week, polar vortex risk D+7 (prob 65%)
LNG: utilization 94%, 8 vessels queued
COT: MM net longs at 6-month high (+12% WoW)
Price: $3.42 front month, +8% WoW
Top signals: [top 3 NEWS WIRE headlines by relevance_score]
```

Prompts Gemini Flash-Lite to produce:

> "Structurally bullish — storage deficit widening against a 5-year normal while a D+7 polar vortex event threatens freeze-offs in Haynesville and Permian. LNG queue depth at 8 vessels suggests export pull remains strong. Near-term price risk skews upside into the EIA print Thursday."

### Model

**`gemini-2.0-flash-lite`** — free tier, sufficient for constrained structured-input narration. A stronger model (Flash or Sonnet) adds polish but not correctness, since the data does the analytical heavy lifting.

### Why not a stronger model

The prompt is tightly constrained structured input → short prose. This is pattern completion, not open-ended reasoning. The model narrates what the data already says. Flash-Lite handles this well at zero cost.

### Optional upgrade: multi-step reasoning chain

Instead of one prompt, chain 3 calls where each output feeds the next:

**Call 1 — Per-signal read** (what does each indicator say in isolation?)
```
Storage -18% vs 5yr avg → bearish supply buffer
LNG utilization 94%, 8 queued → strong export pull
Weather: polar vortex D+7 → demand spike incoming
COT: MM net longs at 6-month high → market already positioned long
```

**Call 2 — Conflict resolution** (do signals contradict each other?)
```
Conflict: market already long (COT) but demand shock not yet priced (D+7 weather)
→ positioning may lag the event, upside still available
```

**Call 3 — Synthesis** (generate the final brief from the above)
```
→ Final 2-sentence brief paragraph
```

**Cost:** 3 calls/run × 48 runs/day = 144 calls/day — still within free tier (1,500/day).
**Latency:** ~4–6s total vs ~2s single-call. Runs in background scheduler, not a request path — nobody feels it.
**Added complexity:** ~20 extra lines in `features_brief.py`. No changes to DB, API, scheduler, or frontend.

**When to use:** build single-call first. If briefs feel shallow or miss obvious cross-signal connections after a week of real output, retrofit the chain — it's entirely contained inside `features_brief.py`.

### Implementation sketch

- New table: `market_brief` (date, content, inputs_hash, generated_at)
- New transform: `transforms/features_brief.py` — assembles feature snapshot, calls Gemini, stores result
- New Go handler: `GET /api/brief`
- Runs at `:30` (after all feature transforms complete)
- Skips generation if inputs_hash unchanged (avoids identical output repeated every 30 min)

---

## Build Order

### Phase 1 — NEWS WIRE panel
1. `db/schema.py` — add `signal_feed` table + run migration
2. `requirements.txt` — add `feedparser`, `google-generativeai`
3. `config/settings.py` — add `GEMINI_API_KEY`
4. `.env.example` — add `GEMINI_API_KEY`
5. `collectors/news_feed.py` — RSS fetch + keyword scoring + optional Gemini blend
6. `scheduler/jobs.py` — wire in job at `:05,:35`
7. `api/internal/handler/signals.go` — query + response types
8. `api/main.go` — register `GET /api/signals`
9. `ui/lib/types.ts` — add `SignalItem`, `SignalsResponse`
10. `ui/components/panels/NewsWirePanel.tsx` — panel component
11. `ui/app/page.tsx` — swap placeholder div with `<NewsWirePanel />`

### Phase 2 — MARKET BRIEF panel
1. `db/schema.py` — add `market_brief` table
2. `transforms/features_brief.py` — feature assembly + Gemini call
3. `scheduler/jobs.py` — wire in at `:30`
4. `api/internal/handler/brief.go` — query + serve
5. `api/main.go` — register `GET /api/brief`
6. `ui/lib/types.ts` — add `BriefResponse`
7. `ui/components/panels/MarketBriefPanel.tsx` — panel component
8. `ui/app/page.tsx` — add panel to layout

---

## Future: X API (optional add-on)

If adding X curated-account monitoring ($30/month estimate):
- Monitor ~15–20 high-signal accounts via user timeline endpoint
- Accounts: FERC commissioners, EIA officials, NGI/Platts/Reuters energy reporters, major NG analysts
- Same scoring/tagging pipeline, source_label = 'X'
- Badge color: `#e2e8f0` (white-ish, X brand neutral)
- ~200 posts/day × $0.005 = ~$1/day = ~$30/month
