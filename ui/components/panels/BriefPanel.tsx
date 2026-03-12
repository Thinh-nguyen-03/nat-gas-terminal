'use client'

import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import type { BriefResponse } from '@/lib/types'

const SSE_SOURCES = ['market_brief']

export function BriefPanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<BriefResponse>('/api/brief', SSE_SOURCES)

  const content = data?.content

  return (
    <PanelShell
      title="MARKET BRIEF"
      source="GEMINI"
      updatedAt={updatedAt}
      flash={flash}
      loading={loading}
      error={error}
    >
      <div className="flex flex-col h-full p-3 gap-3 overflow-hidden">
        {content && (
          <div className="flex flex-col gap-3 h-full">
            {/* Two-column layout: outlook+drivers on left, risk on right */}
            <div className="flex gap-4 flex-1 min-h-0">

              {/* Left: Outlook + Drivers */}
              <div className="flex flex-col gap-2 flex-1 min-w-0">
                {/* Outlook */}
                <div
                  className="text-sm leading-relaxed"
                  style={{
                    color: '#e2e8f0',
                    fontFamily: 'Inter, sans-serif',
                    borderLeft: '2px solid #22d3ee',
                    paddingLeft: 10,
                  }}
                >
                  {content.outlook}
                </div>

                {/* Key Drivers */}
                {content.drivers.length > 0 && (
                  <div className="flex flex-col gap-1">
                    <div
                      className="text-xs uppercase tracking-widest"
                      style={{ color: '#475569', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}
                    >
                      Key Drivers
                    </div>
                    <ul className="flex flex-col gap-0.5">
                      {content.drivers.map((d, i) => (
                        <li key={i} className="flex gap-1.5 items-start text-xs">
                          <span style={{ color: '#22d3ee', flexShrink: 0, fontFamily: 'JetBrains Mono, monospace' }}>›</span>
                          <span style={{ color: '#94a3b8', fontFamily: 'Inter, sans-serif' }}>{d}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>

              {/* Right: Tail Risk */}
              {content.risk && (
                <div
                  className="flex flex-col gap-1 shrink-0"
                  style={{ width: '28%', minWidth: 160 }}
                >
                  <div
                    className="text-xs uppercase tracking-widest"
                    style={{ color: '#475569', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}
                  >
                    Tail Risk
                  </div>
                  <div
                    className="text-xs leading-relaxed"
                    style={{
                      color: '#94a3b8',
                      fontFamily: 'Inter, sans-serif',
                      borderLeft: '2px solid #f87171',
                      paddingLeft: 8,
                    }}
                  >
                    {content.risk}
                  </div>
                </div>
              )}
            </div>

            {/* Footer: model badge + date */}
            <div
              className="flex items-center gap-2 shrink-0"
              style={{ borderTop: '1px solid #1e2433', paddingTop: 4 }}
            >
              <span
                className="text-xs px-1.5"
                style={{
                  fontFamily: 'JetBrains Mono, monospace',
                  fontSize: 9,
                  color: '#22d3ee',
                  backgroundColor: 'rgba(34,211,238,0.08)',
                  border: '1px solid rgba(34,211,238,0.15)',
                }}
              >
                {content.model}
              </span>
              <span
                className="text-xs num"
                style={{ color: '#334155', fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}
              >
                {data?.date}
              </span>
            </div>
          </div>
        )}
      </div>
    </PanelShell>
  )
}
