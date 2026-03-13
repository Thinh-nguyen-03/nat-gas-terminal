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
      <div className="mono h-full overflow-hidden" style={{ display: 'flex', padding: '10px 0' }}>
        {content && <>
          {/* Left column: Outlook headline + Key Drivers below */}
          <div style={{
            flex: '1 1 0',
            minWidth: 0,
            borderLeft: '2px solid #22d3ee',
            marginLeft: 12,
            paddingLeft: 12,
            paddingRight: 24,
            display: 'flex',
            flexDirection: 'column',
            gap: 0,
          }}>
            {/* Outlook */}
            <div style={{ color: '#22d3ee', fontSize: 10, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 5 }}>
              Outlook
            </div>
            <div style={{ color: '#f1f5f9', fontSize: 13, lineHeight: 1.55, fontWeight: 500 }}>
              {content.outlook}
            </div>

            {/* Key Drivers */}
            {content.drivers.length > 0 && (
              <div style={{ marginTop: 10, paddingTop: 8, borderTop: '1px solid #1e2433' }}>
                <div style={{ color: '#fbbf24', fontSize: 10, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 5 }}>
                  Key Drivers
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
                  {content.drivers.map((d, i) => (
                    <div key={i} style={{ display: 'flex', gap: 7, alignItems: 'baseline' }}>
                      <span style={{ color: '#22d3ee', flexShrink: 0, fontSize: 11 }}>›</span>
                      <span style={{ color: '#b8c4d0', fontSize: 11, lineHeight: 1.45 }}>{d}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Right column: Tail Risk + footer */}
          {content.risk && (
            <div style={{
              flexShrink: 0,
              width: '30%',
              minWidth: 200,
              borderLeft: '2px solid rgba(248,113,113,0.35)',
              marginRight: 12,
              paddingLeft: 14,
              paddingRight: 12,
              display: 'flex',
              flexDirection: 'column',
              gap: 0,
            }}>
              <div style={{ color: '#f87171', fontSize: 10, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 5 }}>
                Tail Risk
              </div>
              <div style={{ color: '#b8c4d0', fontSize: 11, lineHeight: 1.55, flex: 1 }}>
                {content.risk}
              </div>
              <div style={{ display: 'flex', gap: 7, alignItems: 'center', marginTop: 10 }}>
                <span style={{
                  fontSize: 8,
                  color: '#22d3ee',
                  backgroundColor: 'rgba(34,211,238,0.07)',
                  border: '1px solid rgba(34,211,238,0.18)',
                  padding: '1px 5px',
                  whiteSpace: 'nowrap',
                }}>
                  {content.model}
                </span>
                <span style={{ color: '#94a3b8', fontSize: 8 }}>{data?.date}</span>
              </div>
            </div>
          )}
        </>}
      </div>
    </PanelShell>
  )
}
