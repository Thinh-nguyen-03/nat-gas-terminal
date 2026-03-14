'use client'

import { useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'

const PANELS = [
  {
    name: 'MARKET BRIEF',
    color: '#22d3ee',
    desc: 'AI-generated narrative synthesizing all dashboard signals into a plain-language view: Outlook (directional assessment), Key Drivers (what\'s moving the market), and Tail Risk (the scenario not yet priced in). Regenerated automatically on each data cycle.',
  },
  {
    name: 'COMPOSITE SCORE',
    color: '#4ade80',
    desc: 'Single aggregated score (roughly −40 to +40) across all fundamental signals. Green > +5 = bullish, red < −5 = bearish. "What Changed" highlights which inputs moved since the last refresh. 7-day chart shows trend.',
  },
  {
    name: 'WEATHER',
    color: '#93c5fd',
    desc: 'Heating/Cooling Degree Day tracker + NOAA CPC outlook (6–10d, 8–14d). Shows demand vs seasonal normal in BCF/d and HDD revision delta. Cold surprises are the fastest driver of short-term price spikes. Green P(below) > 55% = cold favored.',
  },
  {
    name: 'POWER DEMAND',
    color: '#fbbf24',
    desc: 'Grid stress index + per-ISO LMP z-scores (ERCOT, CAISO, PJM, SPP, MISO, NYISO). High stress = elevated gas burn for power generation. Z-score red > +1.5 = stressed, green < −1.5 = slack. 24-hour intraday chart.',
  },
  {
    name: 'NG PRICE',
    color: '#fbbf24',
    desc: 'Henry Hub front-month price, daily change, and signal. LNG arb section shows TTF spot, HH netback (export parity), and arb spread — green spread = export arb open, supportive of US prices. Forward curve bar chart shows term structure (contango vs backwardation).',
  },
  {
    name: 'EIA STORAGE',
    color: '#22d3ee',
    desc: 'EIA weekly inventory vs 5-year range. Green surplus = below average (bullish); red surplus = above average (bearish). Consensus vs model estimate highlights potential surprises on Thursday report days. Historical band chart shows seasonality context.',
  },
  {
    name: 'CFTC COT',
    color: '#4ade80',
    desc: 'CFTC Commitment of Traders — money manager net long/short contracts and % of open interest. Amber CROWDED signal = positioning is extreme and prone to violent reversal. Use as a contrarian overlay: record shorts + cold weather = squeeze risk.',
  },
  {
    name: 'SUPPLY / DEMAND BALANCE',
    color: '#4ade80',
    desc: 'Physical flow model: supply components minus demand components = net balance (BCF/d). Green > +0.5 = oversupplied; red < −0.5 = undersupplied. Implied weekly figure anticipates the next EIA number. Active OFOs (amber) signal pipeline stress.',
  },
  {
    name: 'HISTORICAL ANALOGS',
    color: '#22d3ee',
    desc: 'ML feature-matching to find the most similar past market periods. Each analog card shows similarity %, which features matched (✓/✗), and what happened to prices 4W / 8W / 12W later. High-similarity analogs with consistent outcomes are the most actionable.',
  },
  {
    name: 'LNG EXPORTS',
    color: '#22d3ee',
    desc: 'Live US LNG export utilization via vessel tracking (AIS) + EIA feedgas data. EPI (Export Pressure Index 0–100): green ≥70 = strong export pull, red < 40 = weak. QUEUED count (amber) = vessels waiting to load, a short-term bullish signal. EU% reflects geopolitical demand.',
  },
  {
    name: 'CATALYST CALENDAR',
    color: '#fbbf24',
    desc: 'Upcoming scheduled market events: EIA storage reports (Thu 10:30 ET), NOAA weather updates, pipeline maintenance, Fed decisions. HIGH impact (amber) = events that historically move prices. Prevents being caught off-guard by known catalysts.',
  },
  {
    name: 'NEWS WIRE',
    color: '#94a3b8',
    desc: 'Real-time sentiment-tagged news with AI-generated market implications. Score ≥ 30 (amber) = high estimated market impact. Sort by BULL/BEAR to build a one-sided case quickly, or SCORE to see the most impactful headlines first. Left border color = sentiment at a glance.',
  },
]

const SIGNALS = [
  { color: '#4ade80', label: 'Bullish / Oversold / Below Normal' },
  { color: '#f87171', label: 'Bearish / Overbought / Above Normal' },
  { color: '#fbbf24', label: 'Crowded / Stressed / Warning' },
  { color: '#94a3b8', label: 'Neutral / No Data' },
]

export function HelpButton() {
  const [open, setOpen] = useState(false)

  return (
    <>
      {/* inline help button */}
      <button
        onClick={() => setOpen(true)}
        className="flex items-center gap-1.5 px-2.5 py-1 text-xs font-bold transition-all"
        style={{
          fontFamily: 'JetBrains Mono, monospace',
          color: '#22d3ee',
          border: '1px solid #22d3ee',
          backgroundColor: '#0c0e11',
          boxShadow: '0 0 8px rgba(34,211,238,0.25)',
          letterSpacing: '0.08em',
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.backgroundColor = 'rgba(34,211,238,0.08)'
          e.currentTarget.style.boxShadow = '0 0 14px rgba(34,211,238,0.45)'
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.backgroundColor = '#0c0e11'
          e.currentTarget.style.boxShadow = '0 0 8px rgba(34,211,238,0.25)'
        }}
        title="Panel guide"
      >
        <span style={{ fontSize: 11 }}>?</span>
        <span style={{ fontSize: 10 }}>GUIDE</span>
      </button>

      {/* Modal overlay */}
      <AnimatePresence>
        {open && (
          <motion.div
            key="help-backdrop"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="fixed inset-0 z-50 flex items-center justify-center"
            style={{ backgroundColor: 'rgba(0,0,0,0.75)' }}
            onClick={() => setOpen(false)}
          >
            <motion.div
              key="help-modal"
              initial={{ opacity: 0, scale: 0.97, y: 10 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.97, y: 10 }}
              transition={{ duration: 0.15 }}
              className="relative flex flex-col"
              style={{
                width: 740,
                maxHeight: '82vh',
                backgroundColor: '#0c0e11',
                border: '1px solid #22d3ee',
                fontFamily: 'JetBrains Mono, monospace',
                boxShadow: '0 0 40px rgba(34,211,238,0.12)',
              }}
              onClick={(e) => e.stopPropagation()}
            >
              {/* Header */}
              <div
                className="flex items-center justify-between px-5 py-3 shrink-0"
                style={{ borderBottom: '1px solid #1e2433' }}
              >
                <div className="flex items-center gap-3">
                  <span className="text-xs font-bold tracking-widest" style={{ color: '#22d3ee' }}>
                    PANEL GUIDE
                  </span>
                  <span className="text-xs" style={{ color: '#475569' }}>|</span>
                  <span className="text-xs" style={{ color: '#94a3b8' }}>
                    12 PANELS · REAL-TIME FUNDAMENTALS
                  </span>
                </div>
                <button
                  onClick={() => setOpen(false)}
                  className="text-xs transition-colors px-1"
                  style={{ color: '#475569' }}
                  onMouseEnter={(e) => (e.currentTarget.style.color = '#f87171')}
                  onMouseLeave={(e) => (e.currentTarget.style.color = '#475569')}
                >
                  ✕
                </button>
              </div>

              {/* Scrollable body */}
              <div
                className="flex-1 overflow-y-auto p-5"
                style={{ scrollbarWidth: 'thin', scrollbarColor: '#1e2433 transparent' }}
              >
                {/* Panel cards grid */}
                <div
                  className="grid gap-2"
                  style={{ gridTemplateColumns: '1fr 1fr' }}
                >
                  {PANELS.map((p) => (
                    <div
                      key={p.name}
                      className="flex flex-col gap-1.5 p-3"
                      style={{
                        backgroundColor: '#141720',
                        borderLeft: `3px solid ${p.color}`,
                        border: `1px solid #1e2433`,
                        borderLeftWidth: 3,
                        borderLeftColor: p.color,
                      }}
                    >
                      <span
                        className="text-xs font-bold tracking-wider"
                        style={{ color: p.color, fontSize: 10 }}
                      >
                        {p.name}
                      </span>
                      <span
                        className="text-xs leading-relaxed"
                        style={{ color: '#e2e8f0', fontSize: 10, lineHeight: 1.6 }}
                      >
                        {p.desc}
                      </span>
                    </div>
                  ))}
                </div>

                {/* Signal legend */}
                <div
                  className="mt-4 pt-4 flex flex-col gap-2"
                  style={{ borderTop: '1px solid #1e2433' }}
                >
                  <span className="text-xs tracking-widest" style={{ color: '#94a3b8', fontSize: 10 }}>
                    SIGNAL COLOR REFERENCE
                  </span>
                  <div className="flex flex-wrap gap-2">
                    {SIGNALS.map((s) => (
                      <div
                        key={s.label}
                        className="flex items-center gap-1.5 px-2 py-1"
                        style={{ border: '1px solid #1e2433', backgroundColor: '#141720' }}
                      >
                        <div
                          className="w-1.5 h-1.5 rounded-full shrink-0"
                          style={{ backgroundColor: s.color }}
                        />
                        <span style={{ color: '#e2e8f0', fontSize: 10 }}>{s.label}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  )
}
