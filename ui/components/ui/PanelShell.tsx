'use client'

import { motion, AnimatePresence } from 'framer-motion'
import { fmtTime } from '@/lib/fmt'

interface PanelShellProps {
  title: string
  titleExtra?: React.ReactNode
  source: string
  updatedAt: Date | null
  confidence?: string
  flash: boolean
  loading: boolean
  error: boolean
  children: React.ReactNode
  className?: string
}

export function PanelShell({
  title,
  titleExtra,
  source,
  updatedAt,
  confidence,
  flash,
  loading,
  error,
  children,
  className = '',
}: PanelShellProps) {
  const timeStr = updatedAt ? fmtTime(updatedAt) : '——:——:——'

  return (
    <motion.div
      className={`relative flex flex-col bg-[#141720] overflow-hidden h-full ${className}`}
      style={{ border: '1px solid #1e2433' }}
      animate={
        flash
          ? {
              boxShadow: [
                '0 0 0px 0px rgba(34,211,238,0)',
                '0 0 12px 2px rgba(34,211,238,0.5)',
                '0 0 0px 0px rgba(34,211,238,0)',
              ],
              borderColor: ['#1e2433', '#22d3ee', '#1e2433'],
            }
          : {
              boxShadow: '0 0 0px 0px rgba(34,211,238,0)',
              borderColor: '#1e2433',
            }
      }
      transition={{ duration: 0.8, ease: 'easeOut' }}
    >
      {/* Header */}
      <div
        className="flex items-center justify-between px-3 py-1.5 shrink-0"
        style={{ borderBottom: '1px solid #1e2433', backgroundColor: '#0c0e11' }}
      >
        <div className="flex items-center gap-2">
          <span
            className="text-xs font-semibold tracking-widest"
            style={{ fontFamily: 'JetBrains Mono, monospace', color: '#22d3ee' }}
          >
            {title}
          </span>
          {titleExtra}
        </div>
        <div
          className="flex items-center gap-3 text-xs"
          style={{ fontFamily: 'JetBrains Mono, monospace', color: '#94a3b8' }}
        >
          <span>{source}</span>
          <span>{timeStr}</span>
          {confidence && (
            <span style={{ color: confidence.toLowerCase() === 'high' ? '#4ade80' : '#fbbf24' }}>
              {confidence.toUpperCase()}
            </span>
          )}
        </div>
      </div>

      {/* Body */}
      <div className="relative flex-1 overflow-hidden">
        <AnimatePresence>
          {loading && !error && (
            <motion.div
              key="loading"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="absolute inset-0 flex items-center justify-center z-10"
              style={{ backgroundColor: 'rgba(20,23,32,0.8)' }}
            >
              <div className="flex gap-1">
                {[0, 1, 2].map((i) => (
                  <motion.div
                    key={i}
                    className="w-1 h-4"
                    style={{ backgroundColor: '#22d3ee' }}
                    animate={{ scaleY: [1, 2, 1] }}
                    transition={{ duration: 0.8, repeat: Infinity, delay: i * 0.15 }}
                  />
                ))}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {error && (
          <div
            className="absolute inset-0 flex items-center justify-center text-xs z-10"
            style={{ color: '#94a3b8', fontFamily: 'JetBrains Mono, monospace' }}
          >
            AWAITING DATA
          </div>
        )}

        <div className={`h-full ${error ? 'opacity-20' : loading ? 'opacity-50' : ''}`}>
          {children}
        </div>
      </div>
    </motion.div>
  )
}
