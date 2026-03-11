'use client'

import { useState, useEffect, useCallback, useRef } from 'react'
import { useSSE } from './useSSE'

interface PanelState<T> {
  data: T | null
  loading: boolean
  error: boolean
  updatedAt: Date | null
  flash: boolean
}

export function usePanel<T>(endpoint: string, sseSources: string[]): PanelState<T> {
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(false)
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null)
  const [flash, setFlash] = useState(false)
  const { subscribe } = useSSE()
  const flashTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  // Stable ref for sseSources so the subscribe effect doesn't re-run on every render
  const sseSourcesRef = useRef<string[]>(sseSources)

  const fetchData = useCallback(async (isRefresh = false) => {
    if (!isRefresh) setLoading(true)
    setError(false)
    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'
      const res = await fetch(`${apiUrl}${endpoint}`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const json: T = await res.json()
      setData(json)
      setUpdatedAt(new Date())
      if (isRefresh) {
        setFlash(true)
        if (flashTimer.current) clearTimeout(flashTimer.current)
        flashTimer.current = setTimeout(() => setFlash(false), 800)
      }
    } catch {
      setError(true)
    } finally {
      setLoading(false)
    }
  }, [endpoint])

  useEffect(() => {
    fetchData(false)
  }, [fetchData])

  useEffect(() => {
    const cb = () => fetchData(true)
    const unsubscribe = subscribe(sseSourcesRef.current, cb)
    return unsubscribe
    // subscribe is stable (from useCallback in SSEProvider), sseSourcesRef.current is stable
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [subscribe])

  useEffect(() => {
    return () => {
      if (flashTimer.current) clearTimeout(flashTimer.current)
    }
  }, [])

  return { data, loading, error, updatedAt, flash }
}
