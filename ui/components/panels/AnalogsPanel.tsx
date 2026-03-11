'use client'

import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import { fmt, fmtSign } from '@/lib/fmt'
import type { AnalogsResponse, Analog } from '@/lib/types'

const SSE_SOURCES = ['feat_analog']

function returnColor(v: number | null): string {
  if (v === null) return '#94a3b8'
  if (v > 0) return '#4ade80'
  if (v < 0) return '#f87171'
  return '#94a3b8'
}

function AnalogCard({ analog }: { analog: Analog }) {
  const simPct = (analog.similarity_score * 100).toFixed(0)
  const { return_4w_pct, return_8w_pct, return_12w_pct } = analog.price_outcome

  return (
    <div
      className="flex flex-col gap-1.5 p-2"
      style={{ backgroundColor: '#0c0e11', border: '1px solid #1e2433' }}
    >
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span
            className="text-xs font-bold num"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: '#22d3ee' }}
          >
            #{analog.rank}
          </span>
          <span
            className="text-xs"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: '#e2e8f0' }}
          >
            {analog.period_date}
          </span>
        </div>
        <div className="flex items-center gap-1">
          <span
            className="text-xs"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: '#94a3b8', fontSize: 9 }}
          >
            SIM:
          </span>
          <span
            className="text-xs font-bold num"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: '#fbbf24' }}
          >
            {simPct}%
          </span>
        </div>
      </div>

      {/* Label */}
      {analog.label && (
        <div
          className="text-xs"
          style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', textTransform: 'uppercase', fontSize: 10 }}
        >
          {analog.label}
        </div>
      )}

      {/* Price Outcomes */}
      <div
        className="flex gap-3 text-xs"
        style={{ borderTop: '1px solid #1e2433', paddingTop: 4 }}
      >
        <div className="flex flex-col items-center gap-0.5">
          <span style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}>4W</span>
          <span
            className="num font-semibold"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: returnColor(return_4w_pct) }}
          >
            {return_4w_pct !== null ? `${fmtSign(return_4w_pct, 1)}%` : '—'}
          </span>
        </div>
        <div className="flex flex-col items-center gap-0.5">
          <span style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}>8W</span>
          <span
            className="num font-semibold"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: returnColor(return_8w_pct) }}
          >
            {return_8w_pct !== null ? `${fmtSign(return_8w_pct, 1)}%` : '—'}
          </span>
        </div>
        <div className="flex flex-col items-center gap-0.5">
          <span style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}>12W</span>
          <span
            className="num font-semibold"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: returnColor(return_12w_pct) }}
          >
            {return_12w_pct !== null ? `${fmtSign(return_12w_pct, 1)}%` : '—'}
          </span>
        </div>
      </div>

      {/* Feature matches */}
      {analog.features && analog.features.length > 0 && (
        <div className="flex flex-wrap gap-1 mt-1">
          {analog.features.slice(0, 6).map((f) => (
            <span
              key={f.feature}
              className="text-xs px-1"
              style={{
                fontFamily: 'JetBrains Mono, monospace',
                fontSize: 9,
                color: f.matched ? '#4ade80' : '#f87171',
                backgroundColor: f.matched ? 'rgba(74,222,128,0.1)' : 'rgba(248,113,113,0.1)',
                border: `1px solid ${f.matched ? 'rgba(74,222,128,0.2)' : 'rgba(248,113,113,0.2)'}`,
              }}
            >
              {f.feature.replace(/_/g, ' ')} {f.matched ? '✓' : '✗'}
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

export function AnalogsPanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<AnalogsResponse>('/api/analogs', SSE_SOURCES)

  const analogs = data?.analogs ?? []

  return (
    <PanelShell
      title="HISTORICAL ANALOGS"
      source="MODEL"
      updatedAt={updatedAt}
      flash={flash}
      loading={loading}
      error={error}
    >
      <div className="flex flex-col h-full p-3 gap-2 overflow-y-auto">
        {!loading && !error && analogs.length === 0 && (
          <div
            className="flex flex-col items-center justify-center h-full gap-2"
            style={{ color: '#fbbf24', fontFamily: 'JetBrains Mono, monospace' }}
          >
            <span className="text-sm tracking-widest">ANALOG FINDER INITIALIZING</span>
            <span className="text-xs" style={{ color: '#94a3b8' }}>
              Requires sufficient feature history
            </span>
          </div>
        )}
        {analogs.slice(0, 3).map((analog) => (
          <AnalogCard key={analog.rank} analog={analog} />
        ))}
        {data?.computed_at && (
          <div
            className="text-xs text-right mt-auto"
            style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}
          >
            Computed: {data.computed_at.slice(0, 19).replace('T', ' ')}Z
          </div>
        )}
      </div>
    </PanelShell>
  )
}
