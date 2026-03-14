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
import { HelpButton } from '@/components/ui/HelpButton'

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
          <HelpButton />
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
        {/* Row 1: Market Brief (12) — full-width Gemini synthesis at top */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 190 }}>
          <div className="col-span-12 h-full">
            <BriefPanel />
          </div>
        </div>

        {/* Row 2: Score (4) + Weather (4) + Power (4) — compact metric panels */}
        <div className="grid grid-cols-12 gap-3 overflow-hidden" style={{ height: 620, gridTemplateRows: '620px' }}>
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

        {/* Row 3: Price (5) + Storage (7) */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 450 }}>
          <div className="col-span-5 h-full">
            <PricePanel />
          </div>
          <div className="col-span-7 h-full">
            <StoragePanel />
          </div>
        </div>

        {/* Row 4: COT (5) + [Balance / Analogs stacked] (7) — COT chart expands to match */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 460 }}>
          <div className="col-span-5 h-full">
            <COTPanel />
          </div>
          <div className="col-span-7 h-full flex flex-col gap-3">
            <div style={{ height: 220, flexShrink: 0 }}>
              <BalancePanel />
            </div>
            <div className="flex-1 min-h-0">
              <AnalogsPanel />
            </div>
          </div>
        </div>

        {/* Row 5: LNG (7) + Calendar (5) — both tall/scrollable, natural pairing */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 420 }}>
          <div className="col-span-7 h-full">
            <LNGPanel />
          </div>
          <div className="col-span-5 h-full">
            <CalendarPanel />
          </div>
        </div>

        {/* Row 6: News Wire (12) — full-width */}
        <div className="grid grid-cols-12 gap-3" style={{ minHeight: 260 }}>
          <div className="col-span-12 h-full">
            <NewsPanel />
          </div>
        </div>
      </div>

      {/* Status bar */}
      <StatusBar />
    </div>
  )
}
