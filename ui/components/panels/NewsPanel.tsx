'use client'

import { useMemo } from 'react'
import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import type { NewsResponse, NewsItem } from '@/lib/types'

const SSE_SOURCES = ['news_wire']

function sentimentColor(s: string): string {
  if (s === 'bullish') return '#4ade80'
  if (s === 'bearish') return '#f87171'
  return '#64748b'
}

function sentimentLabel(s: string): string {
  if (s === 'bullish') return '▲'
  if (s === 'bearish') return '▼'
  return '—'
}

function ageLabel(publishedAt: string | null): string {
  if (!publishedAt) return ''
  const diff = Date.now() - new Date(publishedAt).getTime()
  const h = Math.floor(diff / 3_600_000)
  if (h < 1) return `${Math.floor(diff / 60_000)}m`
  if (h < 24) return `${h}h`
  return `${Math.floor(h / 24)}d`
}

export function NewsPanel() {
  const { data, loading, error, updatedAt, flash } = usePanel<NewsResponse>('/api/news', SSE_SOURCES)

  // Separate bullish/bearish from neutral for ordering in the UI
  const { directional, neutral } = useMemo(() => {
    const items = data?.items ?? []
    return {
      directional: items.filter((i) => i.sentiment !== 'neutral'),
      neutral: items.filter((i) => i.sentiment === 'neutral'),
    }
  }, [data])

  const allItems: NewsItem[] = [...directional, ...neutral]

  return (
    <PanelShell
      title="NEWS WIRE"
      source="EIA / RSS / AI"
      updatedAt={updatedAt}
      flash={flash}
      loading={loading}
      error={error}
    >
      <div className="flex flex-col h-full p-3 gap-2 overflow-hidden">
        {allItems.length === 0 && !loading && !error && (
          <div
            className="text-xs"
            style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace' }}
          >
            NO RECENT HEADLINES
          </div>
        )}

        <div className="flex-1 min-h-0 overflow-y-auto flex flex-col gap-1">
          {allItems.map((item) => (
            <NewsItemRow key={item.id} item={item} />
          ))}
        </div>
      </div>
    </PanelShell>
  )
}

function NewsItemRow({ item }: { item: NewsItem }) {
  const color = sentimentColor(item.sentiment)
  const age   = ageLabel(item.published_at)

  const inner = (
    <div
      className="flex flex-col gap-0.5 px-2 py-1.5 cursor-pointer"
      style={{
        backgroundColor: '#0c0e11',
        borderLeft: `2px solid ${color}`,
        border: '1px solid #1e2433',
        borderLeftWidth: 2,
        borderLeftColor: color,
      }}
    >
      <div
        className="text-xs leading-snug"
        style={{
          color: '#cbd5e1',
          fontFamily: 'JetBrains Mono, monospace',
          display: '-webkit-box',
          WebkitLineClamp: 2,
          WebkitBoxOrient: 'vertical',
          overflow: 'hidden',
        }}
      >
        {item.title}
      </div>

      {/* AI implication */}
      {item.implication && (
        <div
          className="text-xs leading-snug"
          style={{
            color: sentimentColor(item.sentiment),
            fontFamily: 'JetBrains Mono, monospace',
            fontSize: 10,
            fontStyle: 'italic',
            opacity: 0.85,
            display: '-webkit-box',
            WebkitLineClamp: 2,
            WebkitBoxOrient: 'vertical',
            overflow: 'hidden',
          }}
        >
          {item.implication}
        </div>
      )}

      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5">
          <span
            className="text-xs font-bold"
            style={{ color, fontFamily: 'JetBrains Mono, monospace', fontSize: 10 }}
          >
            {sentimentLabel(item.sentiment)}
          </span>
          <span
            className="text-xs px-1"
            style={{
              fontFamily: 'JetBrains Mono, monospace',
              fontSize: 9,
              color: '#22d3ee',
              backgroundColor: 'rgba(34,211,238,0.08)',
              border: '1px solid rgba(34,211,238,0.15)',
            }}
          >
            {item.source}
          </span>
        </div>

        <div className="flex items-center gap-2">
          {item.score >= 10 && (
            <span
              className="num text-xs"
              style={{
                fontFamily: 'JetBrains Mono, monospace',
                fontSize: 9,
                color: item.score >= 30 ? '#fbbf24' : '#64748b',
              }}
            >
              {Math.round(item.score)}
            </span>
          )}
          {age && (
            <span
              className="num text-xs"
              style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 9, color: '#64748b' }}
            >
              {age}
            </span>
          )}
        </div>
      </div>
    </div>
  )

  if (item.url) {
    return (
      <a href={item.url} target="_blank" rel="noopener noreferrer" className="block no-underline">
        {inner}
      </a>
    )
  }
  return <div>{inner}</div>
}
