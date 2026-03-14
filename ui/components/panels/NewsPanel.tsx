'use client'

import { useState } from 'react'
import { usePanel } from '@/lib/hooks/usePanel'
import { PanelShell } from '@/components/ui/PanelShell'
import type { NewsResponse, NewsItem } from '@/lib/types'

const SSE_SOURCES = ['news_wire']

type SortMode = 'recent' | 'bullish' | 'bearish' | 'score'

const SORT_COLOR: Record<SortMode, string> = {
  recent:  '#22d3ee',
  bullish: '#4ade80',
  bearish: '#f87171',
  score:   '#fbbf24',
}

const SORT_OPTIONS: { value: SortMode; label: string }[] = [
  { value: 'recent',  label: 'RECENT'  },
  { value: 'bullish', label: '▲ BULL'  },
  { value: 'bearish', label: '▼ BEAR'  },
  { value: 'score',   label: 'SCORE'   },
]

function sortItems(items: NewsItem[], mode: SortMode): NewsItem[] {
  const byDate = (a: NewsItem, b: NewsItem) => {
    const ta = a.published_at ? new Date(a.published_at).getTime() : 0
    const tb = b.published_at ? new Date(b.published_at).getTime() : 0
    return tb - ta
  }
  if (mode === 'recent') return [...items].sort(byDate)
  if (mode === 'score')  return [...items].sort((a, b) => b.score - a.score || byDate(a, b))
  const primary = mode // 'bullish' | 'bearish'
  return [...items].sort((a, b) => {
    const aMatch = a.sentiment === primary ? 0 : 1
    const bMatch = b.sentiment === primary ? 0 : 1
    return aMatch - bMatch || byDate(a, b)
  })
}

function sentimentColor(s: string): string {
  if (s === 'bullish') return '#4ade80'
  if (s === 'bearish') return '#f87171'
  return '#cbd5e1'
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
  const [sortMode, setSortMode] = useState<SortMode>('recent')

  const allItems = sortItems(data?.items ?? [], sortMode)

  const sortSelector = (
    <div className="flex items-center gap-0.5">
      {SORT_OPTIONS.map((opt) => (
        <button
          key={opt.value}
          onClick={() => setSortMode(opt.value)}
          style={{
            fontFamily: 'JetBrains Mono, monospace',
            fontSize: 8,
            padding: '1px 4px',
            border: '1px solid',
            borderColor: sortMode === opt.value ? SORT_COLOR[opt.value] : '#1e2433',
            backgroundColor: sortMode === opt.value ? `${SORT_COLOR[opt.value]}1a` : 'transparent',
            color: sortMode === opt.value ? SORT_COLOR[opt.value] : '#475569',
            cursor: 'pointer',
            letterSpacing: '0.05em',
          }}
        >
          {opt.label}
        </button>
      ))}
    </div>
  )

  return (
    <PanelShell
      title="NEWS WIRE"
      titleExtra={sortSelector}
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
              style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 9, color: '#cbd5e1' }}
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
