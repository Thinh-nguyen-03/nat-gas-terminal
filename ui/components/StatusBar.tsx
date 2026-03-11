'use client'

import { useState, useEffect, useCallback } from 'react'
import { useSSE } from '@/lib/hooks/useSSE'
import { fmtTime } from '@/lib/fmt'
import type { HealthResponse, CollectorStatus } from '@/lib/types'

const KEY_SOURCES = [
  { label: 'STORAGE', sources: ['eia_storage', 'eia_storage_stats'] },
  { label: 'PRICE', sources: ['price'] },
  { label: 'WEATHER', sources: ['weather'] },
  { label: 'COT', sources: ['cftc_cot'] },
  { label: 'POWER', sources: ['power_burn'] },
  { label: 'LNG', sources: ['lng_vessels'] },
  { label: 'SCORE', sources: ['summary'] },
]

function findCollector(collectors: CollectorStatus[], sources: string[]): CollectorStatus | undefined {
  return collectors.find((c) => sources.includes(c.source_name))
}

function statusColor(status: string | undefined): string {
  if (!status) return '#94a3b8'
  if (status === 'success') return '#4ade80'
  if (status === 'error' || status === 'failed') return '#f87171'
  if (status === 'running') return '#22d3ee'
  return '#94a3b8'
}

export function StatusBar() {
  const [health, setHealth] = useState<HealthResponse | null>(null)
  const [lastFetch, setLastFetch] = useState<Date | null>(null)
  const { subscribe } = useSSE()

  const fetchHealth = useCallback(async () => {
    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'
      const res = await fetch(`${apiUrl}/api/health`)
      if (!res.ok) throw new Error()
      const json: HealthResponse = await res.json()
      setHealth(json)
      setLastFetch(new Date())
    } catch {
      // keep stale data
    }
  }, [])

  useEffect(() => {
    fetchHealth()
    const interval = setInterval(fetchHealth, 60_000)
    return () => clearInterval(interval)
  }, [fetchHealth])

  useEffect(() => {
    const unsubscribe = subscribe('*', fetchHealth)
    return unsubscribe
  }, [subscribe, fetchHealth])

  const collectors = health?.collectors ?? []
  const dbOk = health?.db_ok ?? false
  const serverTime = health?.server_time

  return (
    <div
      className="flex items-center gap-4 px-3 py-1.5 overflow-x-auto"
      style={{
        backgroundColor: '#0c0e11',
        border: '1px solid #1e2433',
        borderTop: '1px solid #1e2433',
      }}
    >
      {/* DB Status */}
      <div className="flex items-center gap-1.5 shrink-0">
        <span
          style={{
            display: 'inline-block',
            width: 7,
            height: 7,
            borderRadius: '50%',
            backgroundColor: dbOk ? '#4ade80' : '#f87171',
          }}
        />
        <span
          className="text-xs"
          style={{ fontFamily: 'JetBrains Mono, monospace', color: dbOk ? '#4ade80' : '#f87171' }}
        >
          DB
        </span>
      </div>

      <span style={{ color: '#1e2433', fontSize: 10 }}>|</span>

      {/* Collector statuses */}
      {KEY_SOURCES.map(({ label, sources }) => {
        const collector = findCollector(collectors, sources)
        const color = statusColor(collector?.last_status)
        const lastSuccess = collector?.last_success
        const failures = collector?.consecutive_failures ?? 0

        return (
          <div key={label} className="flex items-center gap-1.5 shrink-0">
            <span
              style={{
                display: 'inline-block',
                width: 6,
                height: 6,
                borderRadius: '50%',
                backgroundColor: color,
              }}
            />
            <span
              className="text-xs"
              style={{ fontFamily: 'JetBrains Mono, monospace', color: '#94a3b8' }}
            >
              {label}
            </span>
            <span
              className="text-xs num"
              style={{ fontFamily: 'JetBrains Mono, monospace', color: '#64748b' }}
            >
              {lastSuccess ? fmtTime(lastSuccess) : '——:——:——'}
            </span>
            {failures > 0 && (
              <span
                className="text-xs num"
                style={{ fontFamily: 'JetBrains Mono, monospace', color: '#f87171' }}
              >
                ({failures}x)
              </span>
            )}
          </div>
        )
      })}

      {/* Spacer */}
      <div className="flex-1" />

      {/* Server time / last fetch */}
      <div className="flex items-center gap-3 shrink-0">
        {serverTime && (
          <span
            className="text-xs num"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: '#94a3b8' }}
          >
            SRV: {fmtTime(serverTime)}
          </span>
        )}
        {lastFetch && (
          <span
            className="text-xs num"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: '#64748b' }}
          >
            POLL: {fmtTime(lastFetch)}
          </span>
        )}
      </div>
    </div>
  )
}
