# Dashboard Panel Guide

This document describes every panel in the Natural Gas Terminal dashboard — what it shows, what it measures, how to read it, and why it matters for trading or analysis.

---

## Layout Overview

The dashboard is a 12-column grid arranged top-to-bottom as follows:

| Row | Panels |
|-----|--------|
| 1 | Market Brief (full width) |
| 2 | Composite Score · Weather · Power Demand |
| 3 | NG Price · EIA Storage |
| 4 | CFTC COT · Supply/Demand Balance + Historical Analogs |
| 5 | LNG Exports · Catalyst Calendar |
| 6 | News Wire (full width) |

Every panel shares a common shell: a **cyan header** with the panel title, a data source label, a last-updated timestamp, and an optional confidence badge. While data is loading, a pulsing overlay is shown. If data is missing or stale, an **AWAITING DATA** overlay appears. When fresh data arrives, the panel briefly glows cyan to signal the update.

---

## 1. Market Brief

**What it is:** An AI-generated narrative synthesis of current market conditions, regenerated automatically whenever underlying data updates.

**What it shows:**
- **Outlook** — A one-paragraph plain-language assessment of the current supply/demand balance and price direction. Framed in terms of whether the market is tight, loose, or in transition.
- **Key Drivers** — A bulleted list of the specific factors currently pushing the market in the direction described. These are ranked by estimated impact.
- **Tail Risk** — A paragraph on the single most important downside scenario that is not yet priced in. This is the thing to watch if it starts to materialize.
- **Model badge** — Which AI model produced the brief (e.g., Gemini), plus the generation date.

**How to read it:** Start here at the beginning of each session. The Outlook gives you a one-sentence handle on the market. Key Drivers tell you what's actually moving prices right now. Tail Risk tells you what could break the thesis. If the brief is stale (previous day), the timestamp will show it.

**Why it matters:** It synthesizes the rest of the dashboard into a coherent view so you do not have to manually reconcile twelve panels at once.

---

## 2. Composite Score

**What it is:** A single quantitative score summarizing the overall bullish/bearish tilt of all market signals, plus a fair value model estimate for Henry Hub.

**What it shows:**
- **Score** — A number typically ranging from roughly −40 to +40. Positive = net bullish, negative = net bearish.
- **Signal label** — BULLISH (>+20), MILDLY BULLISH (+5 to +20), NEUTRAL (−5 to +5), MILDLY BEARISH (−5 to −20), BEARISH (<−20).
- **What Changed** — Up to 4 features that moved the most since the last computation. Each shows the feature name, the new value, and whether it moved up ↑ or down ↓.
- **Key Drivers** — The top features contributing most to the current score direction.
- **Fair Value Model** — A statistical estimate of where Henry Hub should be trading based on current fundamentals:
  - **Model Range** — The 90% confidence band (low–high), shown in cyan.
  - **Mid Estimate** — The point estimate in $/MMBtu.
  - **Gap** — Current price minus fair value mid. Positive = market is trading above fair value (bearish lean); negative = below fair value (bullish lean). Labeled FAIRLY PRICED when gap is near zero.
- **90-day chart** — When fair value data is available, shows a composed chart with the fair value confidence band (cyan shading), the mid estimate (dashed cyan line), and actual Henry Hub price (amber line). Falls back to a 90-day composite score history chart when fair value is not yet computed.

**Color coding:**
- Green (>+5): bullish territory
- Gray (−5 to +5): neutral
- Red (<−5): bearish territory

**How to read it:** The score and label give you the quick verdict. The "What Changed" table tells you what flipped or reinforced the reading since the last refresh. In the fair value section, a large negative gap (price well below model mid) is a bullish signal from a valuation standpoint; a large positive gap (price above fair value) is a warning. The 90-day band chart shows whether the market has been persistently above or below model fair value — sustained deviations tend to mean-revert.

**About the Fair Value Model:** Two modes depending on whether the OLS regression has been fitted:
- *Lookup table mode* (default on fresh install): uses quintile bins of storage deficit × season dummy to estimate fair value. Wider confidence bands.
- *OLS mode* (activated by running `python -m scripts.refit_fairvalue`): linear regression on storage deficit, 7-day weighted HDD, COT managed money net % OI, and a winter season dummy. Tighter bands (~±$2/MMBtu at 90% confidence). Refit quarterly or when data materially changes.

**Why it matters:** Instead of reading 12 individual signals and weighing them yourself, the composite score does that aggregation for you. The fair value overlay anchors the score to a price — so you know not just that the market is bullish, but whether the current price already reflects that bullishness or not.

---

## 3. Weather

**What it is:** A heating/cooling degree day tracker combined with NOAA's Climate Prediction Center (CPC) outlook for the next 6–14 days.

**What it shows:**
- **HDD 7-Day Weighted** — Heating Degree Days accumulated over the rolling 7-day window, weighted by population (gas demand) distribution. Blue = cold (>80 HDD), white = moderate (30–80), amber = warm (<30).
- **Demand vs Normal** — How many BCF/d above or below the 30-year seasonal normal today's weather-driven demand is running. Green = above normal (bullish), red = below normal (bearish).
- **Implied Demand** — Estimated total gas burn from weather, in BCF/d.
- **HDD Revision Delta** — How much the HDD forecast changed from yesterday's run to today's. A large upward revision is bullish; a downward revision bearish.
- **CPC 6–10 Day / 8–14 Day** — NOAA's official temperature outlook. Shows a signal badge (e.g., BELOW_NORMAL) and the probability that temperatures will be below normal, i.e., more heating demand.
  - Green: P(below) > 55% — cold weather favored
  - Red: P(below) < 40% — warm weather favored
  - Gray: 40–55% — toss-up
- **City grid** — Up to 5 key demand-center cities with their 7-day HDD and high temperature. Blue = cold, amber = warm.
- **HDD/CDD history chart** — Solid blue line = HDDs, dashed amber line = CDDs.

**How to read it:** A reading of HDD > 80 in mid-winter means very high residential heating demand and a bullish weather backdrop. Watch the CPC outlook for forward signals — if 8–14 days is trending colder, that is a bullish setup even if today is mild. HDD revision deltas are often leading indicators for price action on weather-driven days.

**Why it matters:** Weather is the single largest driver of short-term US natural gas demand — a cold snap can swing demand by 5–10 BCF/d. This panel quantifies exactly how cold (or warm) it is and where it's headed.

---

## 4. Power Demand

**What it is:** Tracks natural gas demand from the power sector by monitoring electricity grid stress and real-time locational marginal prices (LMPs) across major ISO regions.

**What it shows:**
- **Stress Index** — A composite metric of how hard the electricity grid is working right now, normalized and expressed as a decimal. Higher = more power burn, therefore more gas demand.
- **Signal badge** — Interpretation of the stress index (e.g., ELEVATED, HIGH, NORMAL).
- **ISO table** — For each ISO (ERCOT, CAISO, PJM, SPP, MISO, NYISO):
  - LMP ($/MWh): current locational marginal price
  - Z-score: how many standard deviations from the historical norm. Red if >+1.5 (stressed), green if <−1.5 (slack), gray otherwise.
  - Signal badge: interpretation for that ISO specifically.
- **Stress index history chart** — 24-hour amber line chart showing how grid stress has moved intraday.

**Note:** If ISO LMP data is unavailable (e.g., due to feed issues), the panel shows "ISO LMP DATA PENDING" and falls back to estimated metrics only.

**How to read it:** A spike in ERCOT LMP during summer combined with a high z-score means Texas is burning large quantities of gas for air conditioning. When multiple ISOs show elevated z-scores simultaneously, total power sector gas demand is likely running well above normal. Watch the intraday chart to see if stress is building or fading going into the evening peak.

**Why it matters:** Power burn is the second-largest and most volatile demand component — it can swing 3–6 BCF/d on a single summer afternoon. This panel gives you a real-time read on it, not a next-day estimate.

---

## 5. NG Price

**What it is:** Real-time and historical Henry Hub natural gas price with forward curve and LNG export arbitrage metrics.

**What it shows:**
- **Front month price** — Current settlement or last trade for the front-month NYMEX contract ($X.XXX/MMBtu).
- **Daily change** — Absolute (±$Y.YYY) and percentage (±Z%) move from the prior close. Green = up, red = down.
- **Signal badge** — Interpretation (e.g., BEARISH_MISPRICING, OVERSOLD, NEUTRAL).
- **LNG Arbitrage:**
  - **TTF Spot** — European Title Transfer Facility gas price converted to $/MMBtu.
  - **HH Netback** — The implied US LNG export parity price (TTF minus liquefaction + shipping cost).
  - **Arb Spread** — HH Netback minus Henry Hub. Green = arb is open (US LNG exports are economical, supportive of US prices). Red = arb is closed or negative.
- **Price history chart** — An amber line of rolling close prices.
- **Forward curve** — A bar chart of monthly NYMEX futures showing the term structure. An upward-sloping curve (contango) suggests near-term weakness; downward-sloping (backwardation) signals tightness.

**How to read it:** The arb spread is one of the most actionable inputs on the panel — when it is wide and positive, LNG exporters are incentivized to run at full capacity, pulling supply from the US market and supporting prices. The forward curve tells you where the market expects prices to be over the next several months, which informs hedging and storage decisions.

**Why it matters:** Henry Hub is the pricing benchmark for most US gas contracts. Understanding the real-time price, direction, and term structure is foundational to any trade or analysis.

---

## 6. EIA Storage

**What it is:** The official US natural gas storage inventory report from the Energy Information Administration, with historical context and injection/withdrawal estimates.

**What it shows:**
- **Total BCF** — The most recent EIA reported inventory figure in billion cubic feet.
- **Surplus vs 5-year average** — How many BCF above or below the 5-year seasonal average. Green = below average (deficit = bullish), red = above average (surplus = bearish).
- **Week ending date** — The EIA report period.
- **Signal badge** — Directional interpretation.
- **Consensus vs Model estimate** — The market consensus for the upcoming weekly change (from broker surveys) vs. the model's own estimate. Divergence between these is a potential trade setup.
- **Historical band chart** — A composed chart showing:
  - Cyan line: current storage level
  - Gray dashed: 5-year average
  - Amber dashed: 5-year maximum
  - Red dashed: 5-year minimum
  - Shaded band: the historical range

**How to read it:** A storage level sitting below the 5-year minimum is deeply bullish — the market has structurally drawn down. A level at the top of the range is bearish. The consensus vs. model comparison is most useful on EIA report Thursdays: if the model expects a much smaller withdrawal than consensus, and the actual number confirms, that is a setup for a price spike. If the actual is worse than both, the market typically sells off.

**Why it matters:** Storage is the market's "checking account." The balance relative to seasonal norms is the most widely cited fundamental data point in gas markets — it's what analysts, traders, and journalists refer to constantly.

---

## 7. CFTC COT (Commitment of Traders)

**What it is:** CFTC Commitment of Traders positioning data for NYMEX natural gas, updated weekly. Shows how speculative money managers are positioned — net long or net short.

**What it shows:**
- **MM Net Contracts** — Money manager net position (longs minus shorts). Green = net long (speculators are bullish), red = net short (speculators are bearish).
- **% of Open Interest** — Net position as a fraction of total open interest, giving a normalized read of how crowded the position is.
- **Signal badge** — Interpretation (e.g., CROWDED_LONG, CROWDED_SHORT, NEUTRAL, OVERSOLD).
- **Features list** — Additional COT-derived metrics (e.g., change in net position, producer hedging).
- **Net position bar chart** — Weekly bars colored green (long weeks) or red (short weeks), showing the historical trend in positioning.

**Color coding for signal:**
- CROWDED_LONG / CROWDED_SHORT: amber (warning — crowded positions are prone to sharp reversals)
- BULLISH / BEARISH: green / red
- NEUTRAL: gray

**How to read it:** COT is a contrarian as well as trend signal. When money managers are at extreme net long or net short positions (amber CROWDED signals), the risk of a positioning squeeze is elevated. If the market is net short but weather turns cold, shorts cover aggressively and prices can spike. If the market is extremely long and a bearish storage report hits, longs liquidate quickly.

**Why it matters:** Knowing whether professional speculators are overextended in one direction tells you about the potential velocity of any move. It's a key input to the composite score.

---

## 8. Supply / Demand Balance

**What it is:** A real-time fundamental flow model breaking down US gas supply and demand into components, producing a net balance estimate.

**What it shows:**
- **Net Balance** — `Supply − Demand` in BCF/d. Green if >+0.5 (oversupplied), red if <−0.5 (undersupplied), gray if near-zero (balanced).
- **Implied Weekly** — The net balance projected forward to a weekly storage change, helping you anticipate the EIA report number.
- **OFO Count** — Number of active Operational Flow Orders from pipeline operators. These are emergency curtailment or balancing notices. Amber if >0, since any active OFO signals a local supply/demand imbalance.
- **Supply breakdown** — Component-level dry gas supply figures (BCF/d): production, imports, LNG sendout, storage withdrawals, etc.
- **Demand breakdown** — Component-level demand figures: residential/commercial, industrial, power burn, LNG feedgas, exports, etc.
- **Model summary** — The model's estimate of the next storage change vs. the most recent actual, with the model error in BCF. Red if |error| > 5 BCF (model is unreliable this week).

**How to read it:** When net balance is deeply negative (−1.0 BCF/d or more), the market is physically tight and draws are accelerating — bullish. A surplus means storage is refilling faster than expected — bearish. The OFO count is a leading indicator of pipeline stress: multiple active OFOs in winter often precede a price spike. Compare the implied weekly figure to the consensus estimate from the Storage panel — if the model's implied draw is larger, that's a bullish surprise setup.

**Why it matters:** The supply/demand balance is the "income statement" of the gas market, translating physical flows into a storage implication that directly drives prices.

---

## 9. Historical Analogs

**What it is:** A machine learning feature-matching system that finds the most historically similar periods to today and shows how prices performed in the weeks that followed.

**What it shows:**
- Up to 3 analog cards ranked by similarity score.
- **Similarity score** — What percentage of today's key features matched that historical period. Higher is more comparable.
- **Period date** — When the analog occurred.
- **Label** — A human-readable label if the period has one (e.g., "Winter Draw 2021").
- **Price outcomes** — What happened to Henry Hub prices in the 4, 8, and 12 weeks following that date. Green = price rose, red = price fell, gray = no data.
- **Feature matches** — Which of the key features (e.g., "storage_deficit_vs_5yr", "hdd_7d_weighted") matched (✓ green) or did not match (✗ red) the current environment.

**How to read it:** Look at the feature match list first. If the top analog matches on 5 of 6 features, the historical outcome is more informative. If the analogs consistently show positive 4W and 8W returns (all green), the historical precedent is bullish. If they are mixed or negative, it's a warning sign. A single very high-similarity analog is more informative than three low-similarity ones.

**Why it matters:** Quantitative context for the current setup. Instead of asking "is this market expensive or cheap?", analogs answer "the last few times the market looked exactly like this, here's what happened." It adds historical base rates to fundamental analysis.

---

## 10. LNG Exports

**What it is:** A live view of US LNG export activity combining vessel tracking (AIS) with terminal operational data and EIA flow estimates.

**What it shows:**
- **Implied Exports** — Estimated US LNG export volumes in BCF/d, derived from vessel positions and EIA feedgas data.
- **Utilization %** — What percentage of total US LNG export capacity is currently online and operating.
- **Total Capacity** — US nameplate LNG export capacity in BCF/d (includes Sabine Pass, Corpus Christi, Cove Point, Freeport, Cameron, Sabine, Plaquemines, etc.).
- **Export Pressure Index (EPI)** — A 0–100 composite score of how hard the export complex is working:
  - Green (≥70): strong export pull on the gas market — bullish
  - Amber (40–70): moderate utilization
  - Red (<40): low utilization — reduced gas demand from export sector
- **QUEUED** — Number of vessels currently anchored outside terminals, waiting to load. Shown in amber when >0. A queue indicates demand exceeds loading capacity in the short term — a bullish signal.
- **EU %** — Fraction of vessels with a known European destination. High EU% can reflect geopolitical flows (e.g., post-Ukraine demand for US LNG) which tend to sustain export demand.
- **Terminal table** — Per terminal: operating status dot (green = operational, amber = maintenance/reduced, red = offline), ships currently loading, ships anchored, and nameplate capacity.
- **Vessel rows** — For each vessel tracked under a terminal: vessel name, status (loading/anchored/unconfirmed), dwell time in hours, and destination port.
- **Implied exports history chart** — Rolling area chart of export volumes.

**How to read it:** A high EPI (>70) with a non-zero QUEUED count is one of the most bullish short-term LNG signals — the complex is fully utilized with a backlog building. Watch for terminal outages (red status dots) as they immediately reduce feedgas demand. EU% is a geopolitical overlay: high EU demand tends to keep the arb open (see Price panel) which sustains LNG feedgas burn.

**Why it matters:** LNG feedgas has grown from near-zero to 12–15 BCF/d and is now the largest US gas export. Changes in export utilization have a direct, near-real-time impact on Henry Hub supply/demand balance.

---

## 11. Catalyst Calendar

**What it is:** A forward-looking schedule of upcoming market-moving events and data releases.

**What it shows:**
- Events grouped by time horizon: **TODAY**, **TOMORROW**, **IN N DAYS**.
- For each event:
  - **Event type** — The category (e.g., EIA_STORAGE, FOMC, WEATHER_REPORT, PIPELINE_MAINTENANCE, EARNINGS).
  - **Description** — What the event is.
  - **Impact** — AMBER for HIGH, cyan for MEDIUM, gray for LOW.
  - **Time ET** — When it's due.
  - **Notes** — Additional context if available.

**How to read it:** Focus on HIGH-impact events today and tomorrow. The EIA storage report (Thursdays at 10:30 ET) is the single highest-impact scheduled event each week and typically drives the largest intraday price moves. NOAA weather updates shift the weather panel and can move the score. Major pipeline maintenance notices affect regional supply.

**Why it matters:** Markets move on known catalysts. Knowing what's coming today prevents being caught off-guard by a scheduled event that moves the market sharply.

---

## 12. News Wire

**What it is:** A real-time feed of natural gas market news items, each automatically tagged with a sentiment and a market-impact score.

**What it shows:**
- A scrollable list of news items, each with:
  - **Title** — The headline.
  - **Implication** — A one-sentence interpretation of what this means for the market, colored by sentiment.
  - **Sentiment badge** — ▲ (bullish, green), ▼ (bearish, red), — (neutral, gray).
  - **Source tag** — Where the story came from (e.g., EIA, Bloomberg, Reuters).
  - **Score** — How market-moving this item is estimated to be. Amber if ≥30 (high-impact), gray if 10–30 (moderate). Items below 10 are displayed without a score.
  - **Age** — How long ago the item was published (Xm, Xh, Xd).
- Left border colored by sentiment for quick scanning.

**Sort modes (top-right buttons):**
- **RECENT** — Chronological, newest first.
- **▲ BULL** — Bullish items first (most relevant when looking for upside catalysts).
- **▼ BEAR** — Bearish items first (most relevant when assessing downside risk).
- **SCORE** — Highest-impact items first regardless of sentiment.

**How to read it:** In SCORE mode, you see the most market-relevant headlines at the top regardless of recency. In RECENT mode, you stay on top of the news flow. Switch to BULL or BEAR to build a one-sided case quickly. Scan the colored left borders as a quick sentiment filter — a wall of red borders means the news flow is uniformly bearish.

**Why it matters:** Fundamental data panels update daily or weekly; news updates in real time. A major pipeline explosion, an LNG terminal outage, or an unexpected EIA revision will show here before it affects any other panel.

---

## Signal Color Reference

All panels use a consistent color language for signal badges:

| Color | Meaning | Examples |
|-------|---------|---------|
| Green (`#4ade80`) | Bullish / Oversold / Below Normal | BULLISH, MILDLY_BULLISH, OVERSOLD, BELOW_NORMAL |
| Red (`#f87171`) | Bearish / Overbought / Above Normal | BEARISH, MILDLY_BEARISH, OVERBOUGHT, ELEVATED, ABOVE_NORMAL |
| Amber (`#fbbf24`) | Crowded / Stressed / Warning | CROWDED_LONG, CROWDED_SHORT, HIGH |
| Gray (`#94a3b8`) | Neutral / No Data | NEUTRAL, NEAR_NORMAL, NO_DATA |
| Cyan (`#22d3ee`) | Labels / Info / Medium Impact | Panel headers, source tags, medium-impact events |

---

## Confidence Badges

Some panels show a **HIGH** (green) or **LOW** (amber) confidence badge next to the timestamp. This reflects data quality:
- **HIGH**: Data is fresh, complete, and from primary sources (e.g., live AIS, same-day EIA).
- **LOW**: Data is estimated, interpolated, or from a fallback source (e.g., LNG exports estimated from EIA monthly data when AIS is unavailable).

When a panel shows LOW confidence, treat the values as directionally indicative rather than precise.
