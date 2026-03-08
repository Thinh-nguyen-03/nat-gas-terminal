# Roadmap — Next-Generation Features

This document describes the planned features that would transform the terminal from a data aggregator into an institutional-grade intelligence platform. The guiding thesis is the same one behind commercial products like Genscape, Kpler, and Platts: **individually mundane public data sources, fused and normalized into a single live view, produce intelligence that traders currently pay five to six figures per year for.** All data sources described here are free and public.

Each feature is described with: what it is, why it matters to a nat gas trader, how the data works technically, how it integrates with the current Python/DuckDB/Go/Next.js stack, a concrete implementation plan, and what the UI panel would look like.

---

## Implementation Status (last updated 2026-03-08)

| Feature | Status | Notes |
|---------|--------|-------|
| Feature 1 — Pipeline EBB | ⬜ Not started | Highest-effort item; schema table `pipeline_events` already created |
| Feature 2 — AIS LNG Vessel Tracking | ✅ Implemented | `collectors/lng_vessels.py`, `transforms/features_lng.py`, `GET /api/lng` |
| Feature 3 — ISO LMP | ✅ Implemented | `collectors/iso_lmp.py`, `transforms/features_power_demand.py`, `GET /api/power`; NYISO/MISO/CAISO live; PJM/ERCOT/ISO-NE stubbed (require registration) |
| Feature 4 — Weather-to-Demand | ✅ Implemented | `transforms/demand_coefficients.py`, additions to `features_weather.py` |
| Feature 5 — TTF Spread | ✅ Implemented | TTF added to `collectors/price.py` (FRED); netback + arb spread in `transforms/features_price.py`; exposed in `GET /api/price` |
| Feature 6 — Historical Analog Finder | ✅ Implemented | `transforms/features_analog.py`; accumulates daily snapshots; reports top-5 once ≥52 snapshots exist |
| Feature 7 — Catalyst Calendar | ✅ Implemented | `collectors/catalyst_calendar.py`, `GET /api/calendar` |
| Feature 8 — Storage Consensus Tracker | ✅ Implemented | `transforms/features_storage.py` additions, `consensus_inputs` table; exposed in `GET /api/storage` |
| Feature 9 — Fair Value Model | 🔶 In progress | Price backfill done; lookup table transform next. See data availability notes in Feature 9 section |
| **Backfill script** | ✅ Implemented | `scripts/backfill_history.py` — EIA storage (5,064 rows) + CFTC COT (7,596 rows) + NG=F OHLCV (20,345 rows) + FRED spot/TTF (8,318 rows); all 2010–2026 |
| **Schema additions** | ✅ Implemented | `feature_snapshots`, `pipeline_events` tables added to `db/schema.py` |
| **Go API stubs** | ✅ Implemented | `GET /api/balance`, `/api/lng`, `/api/power`, `/api/calendar`, `/api/analogs` all live |

---

## Feature 1 — Pipeline Electronic Bulletin Board (EBB) Flow Aggregation
> **Status: ⬜ Not started.** Schema table `pipeline_events` exists. This is the highest-effort feature and the biggest remaining intelligence gap. Start with Phase 1 (Transco + Tennessee) to prove the storage estimation model.

### What it is

Every major interstate natural gas pipeline in the United States is required by FERC Order 636 to publish real-time operational data on a publicly accessible Electronic Bulletin Board website. This includes scheduled volumes, actual flows, capacity constraints, and Operational Flow Orders (OFOs). There is no API key, no paywall, and no rate limit — the data is just sitting on dozens of obscure pipeline websites, updated hourly or daily, formatted inconsistently, and almost entirely ignored by retail participants.

Key pipelines and their approximate capacity in the market:

| Pipeline | Operator | Key market points | Capacity |
|----------|----------|------------------|----------|
| Transcontinental (Transco) | Williams | Leidy Hub PA, Zone 6 NY/NJ, Zone 5 Southeast | ~18 Bcf/d |
| Tennessee Gas Pipeline | Kinder Morgan | Zones 4, 5, 6 (northeast focus) | ~11 Bcf/d |
| Texas Eastern (Tetco) | Enbridge | M2 (Appalachia), M3 (PJM area) | ~11 Bcf/d |
| El Paso Natural Gas | Kinder Morgan | Permian Basin, Southwest to CA | ~6 Bcf/d |
| Columbia Gas Transmission | TC Energy | Appalachia to Midwest | ~6 Bcf/d |
| ANR Pipeline | TC Energy | Gulf to Midwest/Chicago | ~5 Bcf/d |
| Panhandle Eastern | Southern Union | Gulf to Midwest | ~5 Bcf/d |
| Iroquois Gas | Iroquois | New England / Zone 6 | ~1.6 Bcf/d |

### Why it matters

The weekly EIA storage report (Thursday 10:30 ET) is the single most market-moving number in nat gas. Prices regularly move 3-8% on the day of release. The entire buy-side community spends the week guessing what it will say.

Genscape sells a product called `NatGas Pipeline Flow Intelligence` that does exactly one thing: it scrapes all the pipeline EBBs, normalizes the data, and gives you a running estimate of storage changes and supply flows. Traders use it to estimate Thursday's number before it's released. It is priced at approximately $60,000-$80,000 per year.

An EBB aggregator would let you:
1. **Estimate Thursday's EIA storage number** from flow data, updated daily
2. **Detect supply disruptions** in real time (a pipeline constraint at Leidy Hub shows up as a capacity reduction on the EBB hours before it moves prices)
3. **Track regional tightness** — the M2/M3 spread on Tetco, the Zone 6 basis on Transco — which reflects how cold the Northeast is relative to supply capacity
4. **Detect Operational Flow Orders (OFOs)** — when a pipeline issues an OFO, it means physical flows are out of balance. An OFO is a near-term price catalyst before it shows up anywhere else

### How the data works

Each pipeline EBB is a different website with a different format. Some use XML feeds. Some are HTML tables you scrape. Some have CSV downloads. All are updated on NAESB (North American Energy Standards Board) reporting schedules — typically hourly for nominations and daily for capacity postings.

Key data fields available on most pipelines:
- **Scheduled quantity** (MMcf/d): How much gas is nominated to flow at each point
- **Operational capacity** (MMcf/d): How much the pipe can physically move
- **Actual flows**: What actually moved in the prior period
- **OFO status**: Whether the pipeline has issued a balance order
- **Imbalance posting**: Accumulated over/under-deliveries

The storage estimation logic is based on the supply/demand balance: if you know how much gas entered the grid (production + imports) and how much left (exports + power burn + residential/commercial), storage is the residual. You don't need every pipeline — the big 5 cover ~70% of Lower 48 flows and are enough to get a directionally accurate estimate.

### Integration with current system

**New collector:** `collectors/pipeline_ebb.py`
- One scraper class per pipeline (subclasses of a common `EBBScraper` base)
- Runs hourly at :10, after the EIA-930 power burn collector
- Writes to `facts_time_series` with `source_name='pipeline_ebb'`, `series_name` encoding the pipeline and point (e.g. `transco_z6_scheduled_mmcfd`), `region` = the market point name
- Stores `frequency='hourly'`

**New transform:** `transforms/features_balance.py`
- Aggregates pipeline flows into a US-level supply/demand balance
- Computes implied daily storage change: `production + imports - (exports + power_burn + modeled_resi_comm)`
- Computes a rolling 7-day implied storage injection/withdrawal
- Writes `supply_demand_balance_mmcfd`, `implied_storage_change_bcf_7d`, `storage_estimate_error_vs_eia` to `features_daily`

**New API endpoint:** `GET /api/balance`
- Returns the live supply/demand table with all components
- Returns the trailing 52-week history of the implied vs actual EIA number (accuracy tracking)

**Schema additions:**
- New `source_name` values in `facts_time_series`: `pipeline_ebb`
- New `features_daily` features: balance components and implied storage

### Implementation plan

**Phase 1 — Two pipelines, prove the model:**
1. Build Transco EBB scraper (Williams publishes XML at `http://www.tetco.com/pipeline/ebb/`) — largest US pipeline, most market-relevant
2. Build Tennessee EBB scraper
3. Build `features_balance.py` with these two pipes + EIA-930 power burn (already collected) + EIA supply production (already collected)
4. Run for 4 weeks, compare implied storage delta to actual EIA Thursday number
5. Measure accuracy — expected error ±15 Bcf at this coverage

**Phase 2 — Five pipelines, production accuracy:**
6. Add Tetco, El Paso, Columbia Gas scrapers
7. Retune the residual demand model using HDD regression
8. Expected accuracy improves to ±8 Bcf — comparable to sell-side estimates

**Phase 3 — OFO detection and alerts:**
9. Add OFO monitoring: detect when any pipeline issues a balance order
10. Write to a new `pipeline_events` table
11. Surface as an alert in the dashboard

### UI panel

The balance panel is a live table in the center of the dashboard, updated hourly:

```
━━━━━━━━━━━ SUPPLY/DEMAND BALANCE (today, MMcf/d) ━━━━━━━━━━━

SUPPLY                          DEMAND
─────────────────────           ──────────────────────────
Dry gas production  104.2       Power burn (EIA-930)  42.1
Canada imports        6.1       Residential/Comm*     28.4
─────────────────────           LNG exports           13.8
  Total supply      110.3       Mexico pipeline        7.2
                                ──────────────────────────
                                  Total demand        91.5

  Net balance: +18.8 MMcf/d → implied weekly: +132 Bcf
  (* modeled from HDD)

━━━━━━━━━━━ STORAGE ESTIMATE ━━━━━━━━━━━
  This week (partial): +87 Bcf through Wednesday
  Full-week estimate:  +118 ± 12 Bcf
  EIA consensus:       +109 Bcf
  Last week actual:    +94 Bcf (model said +98, error: +4)

  ● No OFOs active on monitored pipelines
```

Below the table: a 52-week chart showing the model's implied weekly injection/withdrawal vs the actual EIA number, to visualize model accuracy over time. The chart is the proof-of-concept — when it shows a tight tracking, you've reproduced a core piece of what Genscape sells.

---

## Feature 2 — AIS LNG Vessel Tracking → Export Prediction
> **Status: ✅ Implemented** (`collectors/lng_vessels.py`, `transforms/features_lng.py`). AISHub free-tier polling every 30 min; bounding box covers all 7 terminals in one request. Set `AIS_HUB_USERNAME` in `.env` to activate. Data flows to `GET /api/lng`.

### What it is

AIS (Automatic Identification System) is a VHF radio broadcast that all ships above 300 gross tons are legally required to transmit. It carries the vessel's identity, position, speed, heading, and destination, updated every few seconds. The signal is received by coastal stations and low-earth-orbit satellites, then aggregated into real-time global vessel tracking databases. Several of these databases are publicly accessible via free API.

For natural gas, the relevant vessels are LNG tankers — large cryogenic ships that carry liquefied natural gas from US export terminals to buyers in Europe and Asia. The US has 7 active LNG export terminals, with a combined export capacity of approximately 14 Bcf/d.

### Why it matters

LNG exports are the single largest new demand variable in the US gas market over the past decade. Between 2016 and 2024, US LNG exports grew from zero to ~14 Bcf/d — a structural demand increase equivalent to adding 14% to total US consumption. Any disruption to export capacity (terminal outage, tanker diversion, weather delay) removes that demand from the market immediately and is very bullish for domestic prices.

The problem: the EIA reports LNG exports on a **monthly** basis with a 6-8 week lag. Traders trying to estimate current exports are flying blind. Kpler and Vortexa sell real-time LNG cargo tracking for $20,000-$40,000/year. Their product is built on AIS data, which is public.

By tracking which LNG tankers are berthed at US export terminals vs. anchored offshore vs. in transit, you can estimate:
1. **Current daily export rate** (ships loading = gas leaving the system)
2. **Export disruptions** (ships at anchor or diverting = terminal offline or congested)
3. **Forward export pull** (ships in transit toward Europe vs. Asia affects return timing)

The most impactful signal is when Freeport, Sabine Pass, or Corpus Christi goes offline. In June 2022, a fire at Freeport LNG took 2 Bcf/d of export demand offline for 6 months, causing a massive price collapse. That was visible in AIS data the day it happened — months before EIA monthly data showed it.

### How the data works

**Terminal coordinates:** The 7 US LNG export terminals have known GPS coordinates. A ship is "loading" if it is within ~0.5 nautical miles of a terminal berth and has speed < 1 knot for > 2 hours.

**Key terminals:**

| Terminal | Location | Capacity (Bcf/d) | Coordinates (approx) |
|----------|----------|-----------------|----------------------|
| Sabine Pass | Cameron Parish, LA | 5.0 | 29.71°N, 93.87°W |
| Corpus Christi | Corpus Christi, TX | 2.4 | 27.84°N, 97.45°W |
| Freeport LNG | Freeport, TX | 2.4 | 28.96°N, 95.33°W |
| Cameron LNG | Hackberry, LA | 2.1 | 29.84°N, 93.30°W |
| Calcasieu Pass | Calcasieu, LA | 1.4 | 29.78°N, 93.35°W |
| Cove Point | Lusby, MD | 0.75 | 38.41°N, 76.38°W |
| Elba Island | Savannah, GA | 0.35 | 31.98°N, 81.08°W |

**AIS data sources:**
- **AISHub** (`https://www.aishub.net/api`) — free tier allows 100 requests/hour, returns vessels within a bounding box
- **VesselFinder** — free tier, limited to vessel lookups
- **MarineTraffic** — free tier with rate limits; paid tier has bulk access
- **AIS decoder libraries** — `pyais` Python package can decode raw NMEA sentences from AIS receivers

The collector queries a bounding box around each terminal every 30 minutes. Any vessel with MMSI identifying it as an LNG tanker (vessel type 84 in AIS = liquefied gas tanker) that is stationary at a berth is classified as loading.

**LNG tanker identification:** The IMO maintains a public database of vessel types. The collector maintains a local lookup of known LNG tankers operating in US waters (approximately 200-250 vessels), keyed by MMSI. This list is seeded once from public IMO records and updated periodically.

### Integration with current system

**New collector:** `collectors/lng_vessels.py`
- Queries AIS bounding boxes around each of the 7 terminals every 30 minutes
- Maintains a `data/lng_vessels/known_tankers.json` lookup of LNG MMSI → vessel name
- Writes to `facts_time_series` with `source_name='ais'`:
  - `series_name='lng_ships_loading'`, `region='<terminal_name>'`, `value=<count>`
  - `series_name='lng_ships_anchored'`, `region='<terminal_name>'`, `value=<count>`
- Scheduled: every 30 minutes (same cadence as price collector)

**New transform:** `transforms/features_lng.py`
- Counts total loading berths across all terminals
- Estimates implied export rate: `loading_count × avg_cargo_bcf_per_day_per_berth`
- Computes 7-day rolling implied export rate
- Writes to `features_daily`: `lng_vessels_loading`, `lng_implied_exports_bcfd`, `lng_terminal_utilization_pct`

**New API endpoint:** `GET /api/lng`
- Returns current berth status for all 7 terminals
- Returns 90-day history of implied export rate vs EIA monthly actuals (when available for comparison)

### UI panel

```
━━━━━━━━━━━━━━━━━━━ LNG EXPORT TRACKER ━━━━━━━━━━━━━━━━━━━

  Implied exports today:  11.2 Bcf/d   ▼ -1.8 vs 7-day avg
  Terminal utilization:   80%

  TERMINAL           STATUS        SHIPS      LAST UPDATE
  ──────────────────────────────────────────────────────
  Sabine Pass        ● ACTIVE      3 loading  12 min ago
  Corpus Christi     ● ACTIVE      2 loading  8 min ago
  Freeport           ◌ REDUCED     0 loading  15 min ago
                                   2 at anchor ← signal
  Cameron            ● ACTIVE      2 loading  22 min ago
  Calcasieu Pass     ● ACTIVE      1 loading  11 min ago
  Cove Point         ● ACTIVE      1 loading  18 min ago
  Elba Island        ─ IDLE        0          45 min ago

  ⚠  Freeport: 2 tankers anchored offshore, 0 at berth.
     Possible terminal outage or congestion. Monitor.
```

Below the terminal table: a 90-day line chart comparing AIS-implied export rate (daily, in Bcf/d) vs EIA monthly actuals plotted as a step function. The gap closes over time as the model is calibrated. This is the "Kpler in your browser" moment.

---

## Feature 3 — ISO Real-Time Power Prices (LMP) → Demand Stress Signal
> **Status: ✅ Implemented** (`collectors/iso_lmp.py`, `transforms/features_power_demand.py`). NYISO (Zone J), MISO (Illinois Hub), and CAISO (NP15) are live via free public APIs. PJM, ERCOT, and ISO-NE are stubbed — they require registration (dataminer2.pjm.com, api.ercot.com, webservices.iso-ne.com). Composite 0–100 stress index writes to `features_daily` and `features_intraday`. Data flows to `GET /api/power`.

### What it is

Locational Marginal Pricing (LMP) is the real-time price of electricity at a specific node in the power grid, set by each ISO (Independent System Operator) based on the marginal cost of the next unit of generation needed to meet load. All major US ISOs publish LMPs via free REST APIs, updated every 5 minutes.

Relevant ISOs for nat gas demand:

| ISO | Region | API |
|-----|--------|-----|
| PJM | Mid-Atlantic, Ohio, Midwest | `dataminer2.pjm.com` (free, no key) |
| MISO | Midwest, South | `api.misoenergy.org` (free) |
| ERCOT | Texas | `api.ercot.com` (free, registration required) |
| ISO-NE | New England | `webservices.iso-ne.com` (free) |
| NYISO | New York | `mis.nyiso.com` (free CSV) |
| CAISO | California | `oasis.caiso.com` (free) |
| SPP | Central US | `marketplace.spp.org` (free) |

### Why it matters

Gas-fired power plants are price-takers in the electricity market. When electricity demand spikes (very cold morning, very hot afternoon) and LMPs rise, gas generators bid higher prices for fuel to stay profitable. A spike in LMPs at PJM hub on a cold January morning is a leading indicator that gas burn will be unusually high that day — before EIA-930 data publishes with its 48-hour lag.

The LMP signal is most useful for:

1. **Cold morning demand spikes**: Sub-freezing temperatures across PJM/New England drive residential heating AND power demand simultaneously. LMP spikes at PSEG Hub (New Jersey) and Mass Hub (Boston) on those mornings signal that gas demand is running above normal forecasts.

2. **Fuel-switching thresholds**: In dual-fuel regions (power plants that can burn either gas or oil), the LMP relative to the cost of oil determines which fuel is used. Above a certain LMP, operators switch back to gas even if gas is expensive. This is visible in real-time LMP data before it shows up in any fundamental series.

3. **Negative LMPs in summer (bearish signal)**: In ERCOT and CAISO during sunny afternoons, solar overproduction drives LMPs negative. When LMPs are negative, gas-fired plants ramp down or shut off — reducing gas demand. Persistent negative LMPs in summer signal a structural shift in gas burn floor.

4. **Congestion signals at key nodes**: High congestion cost at the Algonquin Citygate node (New England entry point) signals that the pipeline is constrained — gas is trapped south of the constraint and demand in New England is being met by oil or LNG peakers. This directly affects Transco Zone 6 and Algonquin basis pricing.

### How the data works

Each ISO publishes a REST endpoint returning the current 5-minute LMP for each pricing node. For this use case, we only need hub prices (one per ISO), not individual node prices:

- PJM: `PJMWH` (Western Hub) or `PSEG` (New Jersey)
- MISO: `ILLINOIS.HUB`
- ERCOT: `HB_NORTH` (Houston area)
- ISO-NE: `4001` (Mass Hub)
- NYISO: Zone J (NYC) and Zone A (West)
- CAISO: NP15 (Northern CA), SP15 (Southern CA)

The collector fetches the current 5-minute LMP once per hour (no need for 5-minute granularity; the directional signal is sufficient). It stores a 72-hour rolling window.

**Transform logic:** Compute a `lmp_stress_score` per ISO = how far current LMP is above the 30-day average for the same hour-of-day, in standard deviations. A score > 2 means "abnormally high gas demand signal." Weight by regional gas demand share and produce a composite US power demand stress index.

### Integration with current system

**New collector:** `collectors/iso_lmp.py`
- Queries hub LMP for all 6 ISOs once per hour at :15
- Writes to `facts_time_series` with `source_name='iso_lmp'`
- `series_name='lmp_hub'`, `region=<iso_name>`, `value=<lmp_usd_mwh>`, `frequency='hourly'`

**New transform:** `transforms/features_power_demand.py`
- Computes `lmp_stress_score` per ISO (z-score vs 30-day same-hour average)
- Computes weighted composite: `power_demand_stress_index` (0-100)
- Interpretation: >70 = high gas demand pull, <30 = low demand / excess renewable
- Writes to `features_daily`; also writes to `features_intraday` for the current day

**Additions to existing endpoints:**
- `GET /api/weather` response: add `power_demand` section alongside weather summary
- Alternatively, new `GET /api/power` endpoint

### UI panel

The LMP panel lives next to the weather panel, since both drive near-term demand:

```
━━━━━━━━━━━━ REAL-TIME POWER DEMAND ━━━━━━━━━━━━

  Power demand stress index:  74 / 100  ● HIGH
  (Composite of 6 ISO hub LMPs vs 30-day avg)

  ISO          HUB LMP    30d avg    Z-score   Signal
  ────────────────────────────────────────────────────
  PJM          $48.20     $28.10     +2.1σ     ●
  ISO-NE       $62.40     $31.80     +2.4σ     ●
  NYISO        $55.10     $29.40     +2.2σ     ●
  MISO         $34.20     $22.60     +0.9σ     ─
  ERCOT        $28.80     $27.10     +0.2σ     ─
  CAISO        $18.40     $24.30     -0.8σ     ○ (solar)

  ↑ Northeast grid stressed. Algonquin basis likely
    elevated. Gas burn running above NWS forecast.

  Updated 14:15 ET
```

Below: a 72-hour chart of the composite stress index overlaid on the NWS HDD line, showing how power demand pressure tracks (and sometimes leads) weather.

---

## Feature 4 — Weather-to-Demand Model (HDD/CDD → Bcf/d)
> **Status: ✅ Implemented** (`transforms/demand_coefficients.py`, additions to `transforms/features_weather.py`). Seasonal linear regression coefficients (winter/summer) translate `hdd_7d_weighted` into `weather_implied_resi_comm_bcfd` and `weather_demand_vs_normal_bcfd`. Exposed in `GET /api/weather`.

### What it is

The current system shows HDD and CDD as unitless weather metrics. The institutional product translates those into actual gas demand in Bcf/d — the unit that traders think in when estimating the supply/demand balance. This requires a simple empirical model relating population-weighted HDD to residential/commercial heating demand.

### Why it matters

A 7-day weighted HDD of 85 is a weather signal. "Expected residential/commercial demand of 31.2 Bcf/d vs a 30-day average of 24.1 Bcf/d" is a trading signal. The latter is directly comparable to production (104 Bcf/d), exports (14 Bcf/d), and the other balance components. Without this translation, the weather panel and the supply/demand balance panel are disconnected.

EIA publishes retrospective demand data by sector on a monthly basis (already partially collected via `power_sector_burn_mmcf`). The model is calibrated against this historical data: for each HDD value, what was actual residential/commercial demand? A simple linear regression by season gives coefficients accurate enough for directional trading signals.

### How the data works

**Model inputs:**
- Population-weighted HDD (already computed in `features_daily`)
- Population-weighted CDD (already computed)
- Month-of-year (seasonal baseline)

**Model outputs:**
- Estimated residential/commercial demand in Bcf/d
- Estimated power burn in Bcf/d (from LMP stress index × baseline burn)
- Total modeled demand

**Calibration:** Use EIA historical residential/commercial demand (available quarterly via `NG.N3010US2.M`) regressed against historical HDD for the same period. Separate coefficients for heating season (Oct–Mar) and non-heating season (Apr–Sep).

The model doesn't need to be precise — ±2 Bcf/d accuracy is fine. The purpose is to make the weather metric speak the language of the supply/demand balance table, not to replace an actual demand model.

### Integration with current system

**Additions to `transforms/features_weather.py`:**
- After computing `weather_hdd_7d_weighted`, apply regression coefficients to produce `weather_implied_resi_comm_bcfd`
- Store seasonally-calibrated coefficients in a constants file `transforms/demand_coefficients.py`

**Additions to `transforms/features_balance.py`** (from Feature 1):
- Use `weather_implied_resi_comm_bcfd` as the residential/commercial component in the balance table
- This closes the loop between the weather panel and the balance panel

**No new data collection needed.** This is a pure transform on existing features.

### UI panel change

The weather panel summary line changes from:

```
7-day weighted HDD:  85.2 HDD  ● Bullish
```

to:

```
7-day weighted HDD:  85.2 HDD → est. demand 31.2 Bcf/d  ● Bullish
                     (+7.1 Bcf/d above seasonal normal)
```

This single change makes the weather panel's output directly comparable to the supply numbers. The delta vs seasonal normal (+7.1 Bcf/d) is a more actionable number than the raw HDD.

---

## Feature 5 — TTF/NBP vs Henry Hub Spread → LNG Export Pull Signal
> **Status: ✅ Implemented**. TTF monthly spot (`PNGASEUUSDM`) added to `collectors/price.py` via FRED. Netback formula (`ttf_net_back = ttf_usd_mmbtu - $3.00 shipping/liq`) and arb spread computed in `transforms/features_price.py`. TTF history and LNG arb section exposed in `GET /api/price`.

### What it is

TTF (Title Transfer Facility) is the Dutch natural gas benchmark — the European equivalent of Henry Hub. NBP (National Balancing Point) is the UK benchmark. When European gas prices rise above US prices on a BTU-adjusted, LNG-transport-cost-adjusted basis, it becomes profitable to send every available US LNG cargo to Europe rather than Asia. This is called the "LNG export pull" and it functions as a demand ceiling for US domestic gas: when spreads are wide, US production is effectively competing with global demand for the same molecules.

### Why it matters

The LNG export boom fundamentally changed the US gas market's relationship with global prices. Before 2016, Henry Hub was almost entirely driven by domestic supply and demand. Now, it has a structural floor set by the marginal cost of LNG liquefaction plus the cost of shipping to Europe. When TTF is at $10/MMBtu and HH is at $3/MMBtu (net-back economics: TTF minus shipping and liquefaction cost of ~$2.50-$3.50 gives the breakeven HH price), every available export terminal runs at capacity.

Conversely, when TTF falls toward HH on a net-back basis, export demand softens — cargoes that would have gone to Europe go to Asia or stay on the water. This is currently (2024-2026) a key variable: European storage fills faster in mild winters, reducing their gas import demand, which feeds back to softer US export demand.

TTF is freely available via FRED (`PNGASEUUSDM` is monthly; ICE and Refinitiv publish daily data; there are also free scrapers for the ICE public market data page). NBP is available similarly.

**Net-back formula:**
```
HH_equivalent = TTF_eur_mwh × (1/energy_conversion) × EUR/USD
                - shipping_cost_usd_mmbtu
                - liquefaction_cost_usd_mmbtu
```

When `HH_equivalent > Henry Hub spot`, export economics are positive (bullish demand pull). When `HH_equivalent < Henry Hub spot`, the arbitrage window is closed.

### Integration with current system

**New series in `collectors/price.py`:**
- Add TTF to `FRED_SERIES` dict: `"PNGASEUUSDM": ("ttf_spot", "USD/MMBtu")` — this gives monthly data
- For daily TTF: add a scraper for ICE or Quandl (NASDAQ Data Link has free tier for TTF)

**New transform:** `transforms/features_price.py` additions:
- Compute `ttf_hh_net_back_spread` = TTF equivalent price minus Henry Hub spot
- Interpretation: >$0.50 = strong export pull (bullish), -$0.50 to $0.50 = neutral, <-$0.50 = export drag (bearish)
- Adds to `features_daily`

**Addition to `GET /api/price` response:**
- Add `ttf_history` array alongside `spot_history` and `heating_oil_history`
- Add `lng_arbitrage_spread` to the features section

### UI panel

On the price panel, below the forward curve:

```
━━━━━━━━━━━━━━━━━━ CROSS-COMMODITY SIGNALS ━━━━━━━━━━━━━━━━━━

  LNG EXPORT ARBITRAGE (TTF net-back vs HH)
  ─────────────────────────────────────────
  TTF spot:           $9.84/MMBtu (EUR converted)
  HH spot:            $4.21/MMBtu
  Net-back to HH:     $7.12/MMBtu (TTF minus $2.72 shipping/liq.)
  Arbitrage spread:   +$2.91/MMBtu  ● Strong export pull

  → At current spread, all available export capacity
    should be running. Monitor Freeport/Sabine utilization.

  HEATING OIL vs NAT GAS (fuel-switching)
  ────────────────────────────────────────
  Heating oil:        $2.51/gal → $17.37/MMBtu
  Nat gas:            $4.21/MMBtu
  Fuel-switching gap: $13.16/MMBtu  ● Gas strongly preferred
  (Northeast dual-fuel: gas preferred above ~$12/MMBtu gap)
```

The two signals together tell the trader: where is the demand ceiling (LNG pull) and where is the demand floor (fuel-switching threshold).

---

## Feature 6 — Historical Analog Finder
> **Status: ✅ Implemented** (`transforms/features_analog.py`). Builds a 7-feature vector (storage deficit, WoW change, EOS projection, COT net pct OI, HDD, weather demand) and persists daily snapshots to `feature_snapshots`. Cosine similarity search activates once ≥52 historical snapshots have accumulated (~1 year). Top-5 analogs written to `summary_outputs` as `analog_finder`. Exposed in `GET /api/analogs`.

### What it is

Experienced nat gas traders maintain a mental library of historical analogs: "this feels like November 2022 because storage is similarly undersupplied, weather is similar, and positioning is crowded short." The analog is used as a base case for expected price action, adjusted for any differences in current fundamentals.

This feature formalizes and automates that mental process. It computes a similarity score between the current feature snapshot and every week in the historical database, returning the top 3-5 most similar historical periods with what happened to price in the following 4-8 weeks.

### Why it matters

The fundamental score gives you a directional signal: bullish, mildly bullish, etc. The analog finder gives you a **quantitative context**: "The last time conditions were this similar was November 2022, when the front month went from $6.50 to $9.00 over the next 30 days before reversing." That is a different quality of insight — it surfaces the distribution of historical outcomes rather than a single point estimate.

This is the "AI reasoning over data" feature that makes the tool feel qualitatively different from a data aggregator. No machine learning required — it's cosine similarity on normalized features, which is 10 lines of Python.

### How the data works

**Feature vector:** At any given week, collect a snapshot of the key features:
- `storage_deficit_vs_5yr_bcf` (normalized by historical std dev)
- `weather_hdd_7d_weighted` (normalized)
- `weather_hdd_revision_delta` (normalized)
- `ng_price_monthly_chg_pct` (normalized)
- `cot_mm_net_pct_oi` (normalized)
- `ng_strip_vs_spot` (contango/backwardation, normalized)

Optionally (once available):
- `lng_implied_exports_bcfd` (normalized)
- `power_demand_stress_index` (normalized)

**Similarity computation:**
```python
from sklearn.metrics.pairwise import cosine_similarity  # or manual implementation
current_vector = normalize(current_features)
historical_matrix = normalize(all_historical_feature_snapshots)
scores = cosine_similarity([current_vector], historical_matrix)
top_analogs = argsort(scores)[-5:]  # top 5 most similar
```

For each analog, the output includes:
- The date of the analog
- The similarity score (0-1)
- Feature-by-feature comparison (which features matched, which diverged)
- What the front-month price did over the next 4, 8, and 12 weeks (return)
- A brief label: "Storage undersupplied, cold weather setup, crowded short positioning"

**Data requirement:** The analog finder gets better with more history. With 3 years of weekly snapshots (~150 observations), the analogs are rough. With 10+ years of data, they become genuinely useful. The current system's DuckDB database can be backfilled with historical EIA data for the storage and supply series (EIA publishes history going back to the 1990s), and CFTC publishes full COT history.

### Integration with current system

**New transform:** `transforms/features_analog.py`
- Runs daily at :35, after the summary job at :30
- Computes current feature vector from `features_daily`
- Computes cosine similarity against all historical vectors stored in a `feature_snapshots` table
- Writes top 5 analogs to `summary_outputs` as `analog_finder` summary type

**New schema table:** `feature_snapshots`
- One row per date containing the full feature vector as a JSON column
- Populated by `features_analog.py` as a side effect (append today's snapshot)
- Historical backfill script: `scripts/backfill_snapshots.py`

**New API endpoint:** `GET /api/analogs`
- Returns the current top 5 historical analogs
- Each analog includes: date, similarity score, feature comparison, price outcome data

**Historical data backfill:** The EIA v2 API allows querying storage history back to 1993. A one-time backfill script pulls this data and populates `facts_time_series`, which then allows `features_storage.py` to compute historical features. CFTC publishes disaggregated COT history back to 2006.

### UI panel

```
━━━━━━━━━━━━━━━ HISTORICAL ANALOG FINDER ━━━━━━━━━━━━━━━━━━

  Current conditions most closely resemble:

  #1  Week of Nov 14, 2022   ██████████████░░  89% match
      Storage: -312 Bcf vs 5yr avg    (now: -287 Bcf)
      HDD 7d weighted: 82.1            (now: 85.2)
      COT net: -52K contracts crowded  (now: -47K)
      Spread: backwardated -0.42       (now: -0.38)
      ─────────────────────────────────────────────
      NG=F outcome:  +4w: +38%  +8w: +47%  +12w: -22%
      (Front month went $6.50 → $9.20, then reversed)

  #2  Week of Jan 8, 2018    ████████████░░░░  76% match
      Storage: -268 Bcf vs 5yr avg    (now: -287 Bcf)
      HDD 7d weighted: 91.4            (now: 85.2)
      Divergence: weather warmer now, storage more bearish
      ─────────────────────────────────────────────
      NG=F outcome:  +4w: +22%  +8w: +8%  +12w: -15%

  #3  Week of Feb 3, 2014    ███████████░░░░░  71% match
      [...]

  Key shared characteristic: storage well below avg in
  cold weather with crowded short COT positioning.
  All 3 analogs resolved higher over 4-8 weeks.
```

---

## Feature 7 — Catalyst Calendar
> **Status: ✅ Implemented** (`collectors/catalyst_calendar.py`). Generates EIA Thursdays, Baker Hughes Fridays, CFTC Fridays, STEO 2nd Tuesdays, and FOMC dates for the next 60 days using deterministic IDs (idempotent). Runs daily at 6 AM ET. Exposed in `GET /api/calendar` with `days_until` countdown.

### What it is

A structured, forward-looking timeline of known market-moving events specific to natural gas. This replaces the implicit knowledge a trader maintains in their head ("EIA is Thursday, STEO is next Tuesday, there's a Fed meeting in two weeks") with a live, auto-updating calendar in the dashboard.

### Why it matters

Nat gas prices often move in anticipation of known events, not just in response to them. A crowded short position ahead of an EIA report that might show a surprise draw is a different risk profile than the same short position in the middle of a quiet week. The catalyst calendar provides context for the current positioning signals.

### Structured event types

**Recurring, auto-schedulable:**
- EIA weekly storage report: every Thursday 10:30 ET (except federal holidays)
- CFTC COT report: every Friday 3:30 ET for the prior Tuesday
- EIA STEO (Short-Term Energy Outlook): second Tuesday of each month
- EIA Monthly Energy Review: third week of each month
- NOAA seasonal temperature outlook: updated monthly
- Fed FOMC meetings: 8 per year (published schedule)
- BLS CPI/PPI releases (affects energy demand outlook): monthly
- Baker Hughes rig count: every Friday 1:00 ET

**Scraped/monitored:**
- LNG terminal maintenance windows: published on FERC EBB and terminal operator websites
- FERC pipeline capacity notices: posted to FERC eLibrary
- NOAA winter/summer outlooks: CPC website, monthly
- EIA drilling productivity report: monthly

**Manual entry (future):**
- Custom notes: "Polar vortex watch — NOAA ensemble initialized today"
- Custom alerts: "Watch for Freeport restart — expected late March"

### Integration with current system

**New table:** `catalyst_calendar`
- `(event_date, event_type, title, description, source_url, is_confirmed, created_at)`
- Populated by: a `RecurringEventGenerator` that runs on startup and fills the next 60 days of known recurring events; plus periodic scrapers for FERC notices

**New collector:** `collectors/catalyst_calendar.py`
- Generates recurring EIA/CFTC/FOMC dates from their published schedules
- Scrapes NOAA CPC for seasonal outlook dates
- Runs daily at 6:00 AM ET

**New API endpoint:** `GET /api/calendar`
- Returns events for the next 30 days, sorted by date
- Includes a `days_until` field for countdown display

### UI panel

```
━━━━━━━━━━━━━━━━━━━ CATALYST CALENDAR ━━━━━━━━━━━━━━━━━━━

  TODAY: Thu Mar 12                         ▸ EIA REPORT DAY

  ● TODAY  10:30 ET  EIA Weekly Storage Report
                     Consensus: -38 Bcf | Last week: -81 Bcf
                     Model estimate: -42 ± 10 Bcf

  ─ Fri Mar 13  1:00 PM  Baker Hughes Rig Count
  ─ Fri Mar 13  3:30 PM  CFTC COT Report (Tue Mar 11 data)
  ─ Tue Mar 18  9:00 AM  EIA STEO Monthly Outlook
  ─ Thu Mar 19 10:30 ET  EIA Weekly Storage Report
  ─ Wed Mar 19  2:00 PM  FOMC Rate Decision
  ─ Fri Mar 21  1:00 PM  Baker Hughes Rig Count
  ─ Fri Mar 28          NOAA Spring Outlook Update (est.)
  ─ Thu Mar 26 10:30 ET  EIA Weekly Storage Report

  UPCOMING TERMINAL EVENTS:
  ⚠ Sabine Pass Train 2: scheduled maintenance Mar 20–Apr 2
    (source: Cheniere FERC notice, filed Mar 8)
    Estimated impact: -0.5 Bcf/d exports for 13 days
```

---

## Feature 8 — EIA Storage Consensus Tracker
> **Status: ✅ Implemented**. `consensus_inputs` table schema was already in place. `transforms/features_storage.py` now computes `storage_consensus_bcf` and `storage_eia_surprise_bcf` after each Thursday EIA release. Consensus can be entered manually via the `consensus_inputs` table. Surprise history and consensus exposed in `GET /api/storage`.

### What it is

Before every Thursday EIA storage release, financial media (Bloomberg, Reuters, Platts) conduct analyst surveys and publish a consensus estimate of the expected injection or withdrawal. The range (high/low estimates) and the consensus number are used by the market to calibrate the expected surprise. Prices often move more in response to the **surprise vs. consensus** than to the absolute number itself.

This feature scrapes or calculates a consensus estimate, tracks it alongside the model's own estimate (from Feature 1, the pipeline EBB model), and then records the actual EIA outcome. Over time this builds a comparative accuracy record — and eventually produces a credible model-derived estimate that can itself be used as a reference.

### Why it matters

The market prices EIA reports relative to expectations, not in absolute terms. A -100 Bcf draw is bullish if the consensus was -80 Bcf, and bearish if the consensus was -120 Bcf. Without tracking consensus, the storage panel shows only the historical data series — it can't contextualize today's number relative to what the market already knows.

**Consensus sources (free):**
- Natural Gas Intelligence (NGI) publishes weekly surveys on their website, sometimes accessible without subscription
- Reuters and Bloomberg quote ranges in their pre-report articles
- The Dallas Fed EIA survey is published weekly
- Platts Analytics and PointLogic publish free weekly previews
- Reddit's r/NaturalGas and StockTwits aggregate community estimates (crude but real)

Alternatively, the model can generate its own estimate from Feature 1 (pipeline EBB flows) and Feature 4 (weather-to-demand), display it as the "terminal estimate," and track accuracy vs. the actual EIA number. This internal tracking builds the credibility of the model over time.

### Integration with current system

**New collector:** `collectors/storage_consensus.py`
- Runs Wednesday evening to scrape available consensus estimates
- Falls back to a simple weather-regression model if no consensus scraped
- Writes to a new `consensus_inputs` table: `(report_date, source, low_estimate, consensus_estimate, high_estimate, scraped_at)`

**Additions to `features_storage.py`:**
- After EIA data is collected Thursday, compute `storage_surprise = actual - consensus`
- Store as a feature: `storage_eia_surprise_bcf` with interpretation thresholds

**Additions to `GET /api/storage` response:**
- Add `consensus` section: `{ "low": -90, "consensus": -80, "high": -65, "model_estimate": -78, "source": "...", "report_date": "..." }`
- Add `surprise_history`: last 52 weeks of `(report_date, actual, consensus, surprise)` — this produces the "surprise chart" that traders use to assess model accuracy

### UI panel change

The storage panel's header section becomes:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━ STORAGE ━━━━━━━━━━━━━━━━━━━━━━━━━━━

  NEXT REPORT: Thursday 10:30 ET (in 1 day, 18h)
  ─────────────────────────────────────────────────────────────
  Market consensus:    -80 Bcf   (range: -65 to -95)
  Terminal model:      -78 Bcf   ± 12 Bcf
  Last week actual:    -81 Bcf   (consensus was -73, surprise: -8 Bcf)

  CURRENT STORAGE
  ─────────────────────────────────────────────────────────────
  Latest:   1,742 Bcf (week ending Mar 7)
  vs 5yr avg:   -287 Bcf  ● Bullish
  vs year ago:  -312 Bcf  ● Bullish
```

And a new sub-panel showing the 52-week surprise history as a bar chart (positive = bullish surprise, negative = bearish surprise), with model estimate accuracy overlay.

---

## Synthesis — The Live Balance Sheet

All 8 features, when complete, converge on a single unified panel: the **live supply/demand balance sheet**. This is the nat gas equivalent of the geospatial tool's unified 3D canvas where all data layers are visible simultaneously.

```
━━━━━━━━━━━━━━━━━━ LIVE SUPPLY/DEMAND BALANCE ━━━━━━━━━━━━━━━━━━

  SUPPLY (Bcf/d)                    DEMAND (Bcf/d)
  ─────────────────────────         ──────────────────────────────
  Dry gas production  104.2    ←EIA  Residential/comm.  31.2  ←HDD model
  Canada imports        6.1    ←EIA  Power burn          38.4  ←EIA-930+LMP
  Other imports         0.3    ←EIA  LNG exports         11.2  ←AIS
  ─────────────────────────         Mexico pipeline       7.2  ←EIA
  Total supply        110.6         Total pipeline exp.  14.1  ←EBB
                                    ──────────────────────────────
                                    Total demand        102.1

  Net balance:   +8.5 Bcf/d
  Implied weekly: +59 Bcf                 Consensus: +52 Bcf
  Model estimate: +56 ± 8 Bcf            Report in: 2d 4h

━━━━━━━━━━━━ FORWARD SIGNALS ━━━━━━━━━━━━
  LNG arb spread:  +$2.91 ● Strong export pull    (bearish — demand ceiling high)
  TTF net-back:    $7.12 vs HH $4.21
  CPC 6-10 day:   62% prob below normal  ● Bullish
  CPC 8-14 day:   45% prob below normal  ● Mildly bullish
  LMP stress:     74/100  ● High demand from NE grid
  Top analog:     Nov 2022 (89%) — resolved +38% over 4 weeks

━━━━━━━━━━━━ SCORE ━━━━━━━━━━━━
  Fundamental:   +42  ● Bullish
```

This is the "intelligence monopoly is over" moment — a live, multi-layer market intelligence view assembled entirely from public data, reproducing what institutional traders pay for access to individually across five or six separate paid data terminals.

---

## Feature 9 — Fair Value Price Model
> **Status: 🔶 In progress.** Price backfill is complete (Step 1 done). Next: implement `transforms/features_fairvalue.py` using the lookup table fallback. OLS regression requires the NOAA HDD backfill (Step 2) before it can be trained.

### What it is

Every other feature in this system is a *data layer* — it tells you what is happening (storage is undersupplied, weather is cold, LNG exports are high). The fair value model is the *synthesis layer* — it takes all those inputs and answers the question traders actually care about: **given current fundamentals, where should natural gas be trading?**

The comparison of the model's fair value estimate against the actual market price is the trade signal. If fundamentals justify $5.20/MMBtu and the market is at $4.21/MMBtu, the model is saying the market is mispriced by nearly $1.00 to the downside. That gap — and the direction of its change day over day — is what EBW Analytics Group means by "daily demand represented as fundamentally-justifiable changes in price."

This is the feature that turns the terminal from an information dashboard into an analytical tool with a point of view.

### Why it matters

Without a fair value model, the terminal shows you that storage is -287 Bcf vs 5yr avg, HDD is 85.2, and LNG exports are running high — all bullish. But it can't tell you whether the current price of $4.21/MMBtu already reflects all of that, or whether the market is under-reacting, or over-reacting. The fair value model answers that.

This is how institutional analysts use fundamental research in practice. The workflow is:

1. Collect fundamental inputs (storage deficit, weather, supply run rate, LNG pull)
2. Run them through the fair value model → get an implied price range
3. Compare to actual market price → identify the gap
4. Size the position based on conviction in the model and the size of the gap

The model doesn't need to be right every day. It needs to be right more than 50% of the time on the direction of the gap — that's sufficient for a systematic trading edge.

### How the data works

The fair value model is a multiple linear regression of weekly price changes against fundamental deviations, calibrated on historical data. The inputs and their expected signs:

| Input | Sign | Rationale |
|-------|------|-----------|
| `storage_deficit_vs_5yr_bcf` | Negative coefficient | Larger deficit → higher price; input is deficit (negative = below avg) |
| `weather_hdd_7d_weighted` | Positive | More HDDs → higher demand → higher price |
| `weather_hdd_revision_delta` | Positive | Colder revision → surprise demand → upward price pressure |
| `lng_implied_exports_bcfd` (Feature 2) | Positive | Higher exports → tighter domestic balance |
| `cot_mm_net_pct_oi` | Negative | Contrarian: crowded long → price likely to fall; crowded short → likely to rise |
| `ng_strip_vs_spot` | Negative | Backwardation (negative) = market pricing in tightness → supports spot |
| Season dummy (Oct–Mar = winter) | Positive | Winter baseline demand premium |

**Regression form:**
```
ΔFairValue_$/MMBtu = β₀
                   + β₁ × ΔStorageDeficit
                   + β₂ × HDD_weighted
                   + β₃ × HDD_revision_delta
                   + β₄ × LNG_exports_deviation
                   + β₅ × COT_mm_net_pct_oi
                   + β₆ × Season
                   + ε
```

The model outputs a **fair value range**: a point estimate plus ±1 standard deviation of the residual, e.g. `$4.80 ± $0.35/MMBtu`. The range reflects genuine uncertainty — fundamentals don't fully determine price, they constrain it.

**Calibration approach:**
- Build a dataset of weekly observations going back as far as available history (EIA storage back to 2006, CFTC COT back to 2006, price from yfinance/FRED)
- Run OLS regression; check R², residuals for structure
- Refit quarterly using a rolling 3-year window to avoid stale coefficients
- Store coefficients in `transforms/fairvalue_coefficients.json`, updated by a refit script

**Important caveat:** The model is calibrated on a regime that includes the post-2016 LNG export era and the pre-2016 era. These are structurally different markets. The rolling 3-year window helps by weighting recent history more heavily, but the LNG export variable should only be used in the model from 2016 onward.

**Simpler fallback (before enough history accumulates):** A lookup table approach — bin the storage deficit into quintiles and HDD into quintiles, compute the average front-month price for each bin combination over the past 5 years. This gives a fair value range without needing a regression. Less precise but robust to small sample sizes and requires no scikit-learn dependency.

### Data Availability Assessment

Audited 2026-03-08, updated 2026-03-08. The regression training data lives in `facts_time_series` (not `features_daily`, which only has live data going back days). Current state:

| Series | Coverage | Rows | Status |
|--------|----------|------|--------|
| `eia_storage / storage_total` | 2010–2026 (weekly) | 844 | ✅ Ready — backfilled |
| `cftc / cot_mm_net_pct_oi` | 2010–2026 (weekly) | 844 | ✅ Ready — backfilled |
| `yfinance / ng_front_close` | 2010–2026 (daily) | 4,069 trading days | ✅ Ready — backfilled via `--section prices` |
| `fred / ng_spot_price` | 2010–2026 (daily) | 4,074 | ✅ Ready — backfilled via `--section prices` |
| `fred / heating_oil_spot` | 2010–2026 (daily) | 4,051 | ✅ Ready — backfilled via `--section prices` |
| `fred / ttf_spot` | 2010–2026 (monthly) | 193 | ✅ Ready — backfilled via `--section prices` |
| `eia_supply / lng_exports_mmcf` | 2021–2025 (monthly) | 52 | ⚠️ Limited — monthly only, post-2021 |
| Historical HDD | None | 0 | ❌ New data source required (NOAA CDO API) |

**What needs to be done before the regression can be trained:**

**~~Step 1 — Price history backfill~~ ✅ Done (2026-03-08):**
`scripts/backfill_history.py --section prices` is implemented and has been run. Results: 20,345 rows of NG=F OHLCV (4,069 trading days × 5 fields, 2010–2026), 4,074 rows of Henry Hub spot (`DHHNGSP`), 4,051 rows of heating oil, and 193 rows of TTF (monthly). All written to `facts_time_series` using the same series names as the live `PriceCollector`.

**Step 2 — Historical HDD backfill (medium, ~1–2 days):**
`hdd_7d_weighted` is a *forecast* metric and cannot be reconstructed from historical archives. For the regression training set, substitute **realized historical HDD** from NOAA's Climate Data Online (CDO) API:
- Free API, requires a token from `https://www.ncdc.noaa.gov/cdo-web/webservices/v2`
- Endpoint: `https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=HDD&stationid=...`
- Use the same 8 population-weighted cities as the live WeatherCollector
- Write to `facts_time_series` as a new source (e.g., `source_name='noaa_hdd_historical'`) going back to 2010
- In the refit script, fall back to this series when `features_daily.hdd_7d_weighted` has no historical row for a given week

**Step 3 — Training dataset assembly:**
The refit script (`scripts/refit_fairvalue.py`) should join at the **weekly** level:
- Storage deficit: computed from `eia_storage` vs 5yr rolling average (same logic as `features_storage.py`)
- COT net pct OI: directly from `cftc / cot_mm_net_pct_oi`
- Price: weekly average of `yfinance / ng_front_close` (or `fred / ng_spot_price`)
- HDD: from `features_daily` for live history, `noaa_hdd_historical` for older history
- LNG exports: from `eia_supply / lng_exports_mmcf` (monthly → interpolated weekly, post-2016 only)
- Season dummy: `1` for Oct–Mar, `0` for Apr–Sep

**Step 4 — Lookup table fallback (ready immediately after Step 1):**
The simpler fallback requires only storage history (ready) and price history (after Step 1 backfill). Bin `storage_deficit_vs_5yr_bcf` into quintiles × season (winter/summer), compute the 5th/50th/95th percentile Henry Hub price per bin. No regression, no scikit-learn, no HDD needed. Implement this first as `transforms/features_fairvalue.py` with a flag to switch to OLS mode once Step 2 is complete.

**Recommended implementation sequence:**
1. ~~Add `--section prices` to `scripts/backfill_history.py`~~ ✅ Done — 28,663 rows written 2026-03-08
2. Implement `transforms/features_fairvalue.py` using the lookup table fallback ← **next step**
3. Add NOAA HDD backfill (`scripts/backfill_history.py --section hdd`)
4. Implement `scripts/refit_fairvalue.py` (OLS regression, writes `transforms/fairvalue_coefficients.json`)
5. Switch `features_fairvalue.py` to OLS mode; keep lookup table as fallback when coefficients are stale

### Integration with current system

**New transform:** `transforms/features_fairvalue.py`
- Runs daily at :35, after the summary job at :30
- Reads current feature snapshot from `features_daily`
- Loads calibrated coefficients from `transforms/fairvalue_coefficients.json`
- Computes point estimate and ±1σ range
- Computes the gap: `fairvalue_gap = fair_value_mid - ng_price_current`
- Interprets gap: `>$0.50 bullish mispricing`, `$0.20-$0.50 mildly bullish`, `±$0.20 fairly priced`, `<-$0.20 mildly bearish mispricing`, `<-$0.50 bearish mispricing`
- Writes to `features_daily`:
  - `fairvalue_mid` — point estimate in USD/MMBtu
  - `fairvalue_low` — lower bound of range
  - `fairvalue_high` — upper bound of range
  - `fairvalue_gap` — actual price minus fair value (positive = overpriced, negative = underpriced)
  - `fairvalue_interpretation` — the interpretation string

**Refit script:** `scripts/refit_fairvalue.py`
- Run manually quarterly, or scheduled monthly
- Reads full history from DuckDB, runs OLS, writes new coefficients to JSON
- Prints R², coefficient table, residual diagnostics to stdout for review

**Addition to `GET /api/score` response:**
- Add `fair_value` section: `{ "mid": 4.80, "low": 4.45, "high": 5.15, "gap": -0.59, "interpretation": "bullish_mispricing" }`
- Add `fairvalue_history` to the score endpoint: last 90 days of `(date, fair_value_mid, actual_price, gap)` — this is the chart that shows how the model has tracked vs market

**No new data collection needed.** All inputs are already in `features_daily`.

### UI panel

The fair value panel is the highest-signal element in the entire dashboard. It sits prominently in the score/summary section, directly below the composite fundamental score:

```
━━━━━━━━━━━━━━━━━━━━━━ FUNDAMENTAL SCORE ━━━━━━━━━━━━━━━━━━━━━━━

  Score:   +42  ● Bullish
  Drivers: Storage 287 Bcf below 5yr avg | HDD 85.2 (cold) |
           LNG exports elevated | COT crowded short (contrarian bullish)

━━━━━━━━━━━━━━━━━━━━━━ FAIR VALUE MODEL ━━━━━━━━━━━━━━━━━━━━━━━━

  Fundamentally justified price:   $4.45 — $5.15 / MMBtu
  Mid estimate:                    $4.80 / MMBtu

  Current market price:            $4.21 / MMBtu
  Gap:                            -$0.59  ● Market UNDERVALUED vs fundamentals

  ──────────────────────────────────────────────────────────────
  What's driving the gap:
    + Storage deficit adds ~$0.42 to fair value
    + Cold weather (HDD 85.2) adds ~$0.31
    + LNG export pull adds ~$0.18
    - COT (crowded long) subtracts -$0.12
    - Model uncertainty: ±$0.35
  ──────────────────────────────────────────────────────────────
  Model accuracy (last 52 weeks):
    Direction correct:  61%   (gap predicted direction of next-week move)
    Mean absolute error: $0.28/MMBtu
    R²: 0.54
```

Below: a 90-day chart with two lines — the fair value range (shaded band) and the actual front-month price. Periods where price is below the band are shaded green (undervalued); periods where price is above the band are shaded red (overvalued). This is the visual that makes the model's track record immediately legible.

The accuracy metrics are essential. They give the user calibration on how much to trust the signal, and they force honest accounting of the model's limitations. A 61% directional accuracy with a $0.28 MAE is a useful but imperfect tool — exactly what the display should convey.

---

## Synthesis — The Live Balance Sheet

All 9 features, when complete, converge on a single unified panel: the **live supply/demand balance sheet**. This is the nat gas equivalent of the geospatial tool's unified 3D canvas where all data layers are visible simultaneously.

```
━━━━━━━━━━━━━━━━━━ LIVE SUPPLY/DEMAND BALANCE ━━━━━━━━━━━━━━━━━━

  SUPPLY (Bcf/d)                    DEMAND (Bcf/d)
  ─────────────────────────         ──────────────────────────────
  Dry gas production  104.2    ←EIA  Residential/comm.  31.2  ←HDD model
  Canada imports        6.1    ←EIA  Power burn          38.4  ←EIA-930+LMP
  Other imports         0.3    ←EIA  LNG exports         11.2  ←AIS
  ─────────────────────────         Mexico pipeline       7.2  ←EIA
  Total supply        110.6         Total pipeline exp.  14.1  ←EBB
                                    ──────────────────────────────
                                    Total demand        102.1

  Net balance:   +8.5 Bcf/d
  Implied weekly: +59 Bcf                 Consensus: +52 Bcf
  Model estimate: +56 ± 8 Bcf            Report in: 2d 4h

━━━━━━━━━━━━ FORWARD SIGNALS ━━━━━━━━━━━━
  LNG arb spread:  +$2.91 ● Strong export pull    (bearish — demand ceiling high)
  TTF net-back:    $7.12 vs HH $4.21
  CPC 6-10 day:   62% prob below normal  ● Bullish
  CPC 8-14 day:   45% prob below normal  ● Mildly bullish
  LMP stress:     74/100  ● High demand from NE grid
  Top analog:     Nov 2022 (89%) — resolved +38% over 4 weeks

━━━━━━━━━━━━ FAIR VALUE ━━━━━━━━━━━━
  Model range:   $4.45 — $5.15/MMBtu
  Market price:  $4.21/MMBtu
  Gap:          -$0.59  ● Undervalued

━━━━━━━━━━━━ SCORE ━━━━━━━━━━━━
  Fundamental:   +42  ● Bullish
```

This is the "intelligence monopoly is over" moment — a live, multi-layer market intelligence view assembled entirely from public data, reproducing what institutional traders pay for access to individually across five or six separate paid data terminals.

---

## Implementation Priority

| Feature | Effort | Impact | Priority | Status |
|---------|--------|--------|----------|--------|
| Pipeline EBB aggregation | High (many scrapers) | Very High | 1 | ⬜ Not started |
| Historical analog finder | Low (pure transform) | High | 2 | ✅ Done |
| Catalyst calendar | Medium | High | 3 | ✅ Done |
| AIS LNG vessel tracking | Medium | High | 4 | ✅ Done |
| Storage consensus tracker | Low-Medium | Medium-High | 5 | ✅ Done |
| ISO LMP collector | Medium | Medium | 6 | ✅ Done (3 of 6 ISOs live) |
| TTF/LNG arb spread | Low (FRED + formula) | Medium | 7 | ✅ Done |
| Weather-to-demand model | Low (regression only) | Medium | 8 | ✅ Done |
| Fair value price model | Medium (regression + refit) | Very High | 9 | 🔶 Price backfill done; lookup table next |

**Remaining work (in order):**

1. ~~**Price history backfill**~~ ✅ Done — 28,663 rows written (NG=F OHLCV + FRED DHHNGSP/TTF/heating oil, 2010–2026).
2. **Feature 9 lookup table** — implement `transforms/features_fairvalue.py` using quintile bins on storage deficit × season. All data is ready. No scikit-learn needed.
3. **Feature 9 OLS regression** — add NOAA HDD historical backfill, write `scripts/refit_fairvalue.py`. ~2 days. Upgrades Feature 9 from lookup table to calibrated regression.
4. **Feature 1 (Pipeline EBB)** — highest-effort remaining feature. Start with Phase 1 (Transco + Tennessee only) to validate the storage estimation approach before building all scrapers.
5. **ISO LMP registration** — register for PJM Dataminer2, ERCOT API, and ISO-NE webservices to complete Feature 3's missing 3 ISOs.

Feature 9 is deceptively high impact for medium effort. It requires no new data collection beyond the price backfill — only a calibration script and a new transform. The output is the single number that ties every other feature together into an actionable signal. The fair value model should use the lookup table approach first and graduate to OLS once the NOAA HDD backfill is complete.
