import { ScorePanel } from '@/components/panels/ScorePanel'
import { PricePanel } from '@/components/panels/PricePanel'
import { StoragePanel } from '@/components/panels/StoragePanel'
import { WeatherPanel } from '@/components/panels/WeatherPanel'
import { COTPanel } from '@/components/panels/COTPanel'
import { LNGPanel } from '@/components/panels/LNGPanel'
import { PowerPanel } from '@/components/panels/PowerPanel'
import { CalendarPanel } from '@/components/panels/CalendarPanel'
import { AnalogsPanel } from '@/components/panels/AnalogsPanel'
import { BalancePanel } from '@/components/panels/BalancePanel'
import { NewsPanel } from '@/components/panels/NewsPanel'
import { BriefPanel } from '@/components/panels/BriefPanel'
import { StatusBar } from '@/components/StatusBar'

export default function DashboardPage() {
  return (
    <div className="flex flex-col min-h-screen" style={{ backgroundColor: '#0c0e11' }}>
      {/* Header bar */}
      <div
        className="flex items-center justify-between px-4 py-2 shrink-0"
        style={{ borderBottom: '1px solid #1e2433' }}
      >
        <div className="flex items-center gap-3">
          <span
            className="text-sm font-bold tracking-widest"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: '#22d3ee' }}
          >
            NG TERMINAL
          </span>
          <span
            className="text-xs"
            style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace' }}
          >
            NATURAL GAS FUNDAMENTALS
          </span>
        </div>
        <span
          className="text-xs"
          style={{ color: '#64748b', fontFamily: 'JetBrains Mono, monospace' }}
        >
          v0.1.0
        </span>
      </div>

      {/* Main grid */}
      <div className="flex-1 p-3 flex flex-col gap-3 overflow-auto">
        {/* Row 1: Score (4) + Weather (4) + Power (4) — compact metric panels */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 240 }}>
          <div className="col-span-4 h-full">
            <ScorePanel />
          </div>
          <div className="col-span-4 h-full">
            <WeatherPanel />
          </div>
          <div className="col-span-4 h-full">
            <PowerPanel />
          </div>
        </div>

        {/* Row 2: Price (6) + Storage (6) — chart panels, half-width each */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 300 }}>
          <div className="col-span-6 h-full">
            <PricePanel />
          </div>
          <div className="col-span-6 h-full">
            <StoragePanel />
          </div>
        </div>

        {/* Row 3: LNG (7) + COT (5) — chart-heavy panels */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 320 }}>
          <div className="col-span-7 h-full">
            <LNGPanel />
          </div>
          <div className="col-span-5 h-full">
            <COTPanel />
          </div>
        </div>

        {/* Row 4: Calendar (5) + News Wire (7) */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 280 }}>
          <div className="col-span-5 h-full">
            <CalendarPanel />
          </div>
          <div className="col-span-7 h-full">
            <NewsPanel />
          </div>
        </div>

        {/* Row 5: Balance (6) + Analogs (6) */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 260 }}>
          <div className="col-span-6 h-full">
            <BalancePanel />
          </div>
          <div className="col-span-6 h-full">
            <AnalogsPanel />
          </div>
        </div>

        {/* Row 6: Market Brief (12) — full-width Gemini synthesis */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 160 }}>
          <div className="col-span-12 h-full">
            <BriefPanel />
          </div>
        </div>
      </div>

      {/* Status bar */}
      <StatusBar />
    </div>
  )
}
