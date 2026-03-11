'use client'

import { useMemo } from 'react'
import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import type { CalendarResponse, CalendarEvent } from '@/lib/types'

const SSE_SOURCES = ['catalyst_calendar']

function impactColor(impact: string | null): string {
  if (!impact) return '#94a3b8'
  const lower = impact.toLowerCase()
  if (lower === 'high') return '#fbbf24'
  if (lower === 'medium') return '#22d3ee'
  return '#94a3b8'
}

function dayLabel(daysUntil: number): string {
  if (daysUntil === 0) return 'TODAY'
  if (daysUntil === 1) return 'TOMORROW'
  return `IN ${daysUntil} DAYS`
}

interface GroupedEvents {
  daysUntil: number
  label: string
  events: CalendarEvent[]
}

export function CalendarPanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<CalendarResponse>('/api/calendar', SSE_SOURCES)

  const grouped = useMemo<GroupedEvents[]>(() => {
    if (!data?.events) return []
    const map = new Map<number, CalendarEvent[]>()
    for (const event of data.events) {
      if (!map.has(event.days_until)) map.set(event.days_until, [])
      map.get(event.days_until)!.push(event)
    }
    return Array.from(map.entries())
      .sort(([a], [b]) => a - b)
      .map(([days, events]) => ({
        daysUntil: days,
        label: dayLabel(days),
        events,
      }))
  }, [data])

  return (
    <PanelShell
      title="CATALYST CALENDAR"
      source="MULTI"
      updatedAt={updatedAt}
      flash={flash}
      loading={loading}
      error={error}
    >
      <div className="flex flex-col h-full p-3 gap-2 overflow-hidden">
        {grouped.length === 0 && !loading && !error && (
          <div
            className="text-xs"
            style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace' }}
          >
            NO EVENTS SCHEDULED
          </div>
        )}
        <div className="flex-1 min-h-0 overflow-y-auto flex flex-col gap-2">
        {grouped.map((group) => (
          <div key={group.daysUntil}>
            <div
              className="text-xs font-semibold tracking-widest mb-1"
              style={{
                color: group.daysUntil === 0 ? '#22d3ee' : '#94a3b8',
                fontFamily: 'JetBrains Mono, monospace',
                fontSize: 9,
              }}
            >
              {group.label}
            </div>
            <div className="flex flex-col gap-1">
              {group.events.map((event) => (
                <div
                  key={event.id}
                  className="flex flex-col gap-0.5 px-2 py-1"
                  style={{ backgroundColor: '#0c0e11', border: '1px solid #1e2433' }}
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="flex items-center gap-1.5 min-w-0">
                      {/* Event type tag */}
                      <span
                        className="text-xs shrink-0 px-1"
                        style={{
                          fontFamily: 'JetBrains Mono, monospace',
                          color: '#22d3ee',
                          backgroundColor: 'rgba(34,211,238,0.1)',
                          border: '1px solid rgba(34,211,238,0.2)',
                          fontSize: 9,
                        }}
                      >
                        {event.event_type.toUpperCase()}
                      </span>
                      <span
                        className="text-xs truncate"
                        style={{ color: '#b8c4d0', fontFamily: 'JetBrains Mono, monospace' }}
                      >
                        {event.description}
                      </span>
                    </div>
                    {event.impact && (
                      <span
                        className="text-xs shrink-0"
                        style={{
                          fontFamily: 'JetBrains Mono, monospace',
                          color: impactColor(event.impact),
                          fontSize: 9,
                        }}
                      >
                        [{event.impact.toUpperCase()}]
                      </span>
                    )}
                  </div>
                  <div className="flex gap-3 text-xs" style={{ color: '#94a3b8' }}>
                    <span
                      className="num"
                      style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}
                    >
                      {event.event_date}
                    </span>
                    {event.event_time_et && (
                      <span
                        className="num"
                        style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 9 }}
                      >
                        {event.event_time_et} ET
                      </span>
                    )}
                  </div>
                  {event.notes && (
                    <div
                      className="text-xs"
                      style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace', fontSize: 10 }}
                    >
                      {event.notes}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        ))}
        </div>
      </div>
    </PanelShell>
  )
}
