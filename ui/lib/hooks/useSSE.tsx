'use client'

import React, { createContext, useContext, useEffect, useRef, useCallback } from 'react'

type Listener = () => void

interface SSEContextValue {
  subscribe: (sources: string | string[], cb: Listener) => () => void
}

const SSEContext = createContext<SSEContextValue>({
  subscribe: () => () => {},
})

export function SSEProvider({ children }: { children: React.ReactNode }) {
  const listenersRef = useRef<Map<string, Set<Listener>>>(new Map())
  const esRef = useRef<EventSource | null>(null)
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const notify = useCallback((source: string) => {
    const set = listenersRef.current.get(source)
    if (set) {
      set.forEach((cb) => cb())
    }
    // Also notify wildcard listeners
    const wildcard = listenersRef.current.get('*')
    if (wildcard) {
      wildcard.forEach((cb) => cb())
    }
  }, [])

  const connect = useCallback(() => {
    if (esRef.current) {
      esRef.current.close()
    }

    const es = new EventSource('/api/stream')
    esRef.current = es

    es.addEventListener('collection_complete', (e: MessageEvent) => {
      const source = e.data?.trim()
      if (source) {
        notify(source)
      }
    })

    es.onerror = () => {
      es.close()
      esRef.current = null
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current)
      reconnectTimer.current = setTimeout(connect, 5000)
    }
  }, [notify])

  useEffect(() => {
    connect()
    return () => {
      if (esRef.current) {
        esRef.current.close()
        esRef.current = null
      }
      if (reconnectTimer.current) {
        clearTimeout(reconnectTimer.current)
      }
    }
  }, [connect])

  const subscribe = useCallback((sources: string | string[], cb: Listener): (() => void) => {
    const sourceList = Array.isArray(sources) ? sources : [sources]

    sourceList.forEach((src) => {
      if (!listenersRef.current.has(src)) {
        listenersRef.current.set(src, new Set())
      }
      listenersRef.current.get(src)!.add(cb)
    })

    return () => {
      sourceList.forEach((src) => {
        listenersRef.current.get(src)?.delete(cb)
      })
    }
  }, [])

  return (
    <SSEContext.Provider value={{ subscribe }}>
      {children}
    </SSEContext.Provider>
  )
}

export function useSSE() {
  return useContext(SSEContext)
}
