import { useState, useRef, useCallback, type KeyboardEvent, type ChangeEvent } from 'react'
import { cn } from '@/lib/utils'

interface ExpressionInputProps {
  value: string
  onChange: (value: string) => void
  mode: 'match' | 'value'
  placeholder?: string
  className?: string
}

function getMatchGhost(value: string): string {
  const trimmed = value.trimEnd()

  if (trimmed === '') return "TAG['key'] == 'value'"

  // After || or at the start of a new OR clause
  if (/\|\|\s*$/.test(trimmed)) return trimmed + " TAG['key'] == 'value'"

  // Typing DIM...
  if (/^DIM$/i.test(trimmed) || /\|\|\s*DIM$/i.test(trimmed)) {
    return trimmed.replace(/DIM$/i, "DIMENSION['key'] == 'value'")
  }
  if (/DIMENSION$/i.test(trimmed)) {
    return trimmed + "['key'] == 'value'"
  }

  // Typing TAG without bracket
  if (/TAG$/i.test(trimmed)) return trimmed + "['key'] == 'value'"

  // TAG[ started
  if (/TAG\[$/i.test(trimmed)) return trimmed + "'key'] == 'value'"
  if (/TAG\['$/i.test(trimmed)) return trimmed + "key'] == 'value'"

  // DIMENSION[ started
  if (/DIMENSION\[$/i.test(trimmed)) return trimmed + "'key'] == 'value'"
  if (/DIMENSION\['$/i.test(trimmed)) return trimmed + "key'] == 'value'"

  // After TAG['xxx'] or DIMENSION['xxx'] — suggest operator
  if (/(?:TAG|DIMENSION)\['[^']*'\]\s*$/i.test(trimmed)) {
    return trimmed + " == 'value'"
  }

  // After == — suggest value
  if (/==\s*$/i.test(trimmed)) return trimmed + "'value'"
  if (/==\s*'$/i.test(trimmed)) return trimmed + "value'"

  // After CO... — complete CONTAINS
  if (/\s+CO$/i.test(trimmed)) return trimmed.replace(/CO$/i, "CONTAINS 'value'")
  if (/\s+CON$/i.test(trimmed)) return trimmed.replace(/CON$/i, "CONTAINS 'value'")
  if (/\s+CONT$/i.test(trimmed)) return trimmed.replace(/CONT$/i, "CONTAINS 'value'")
  if (/\s+CONTA$/i.test(trimmed)) return trimmed.replace(/CONTA$/i, "CONTAINS 'value'")
  if (/\s+CONTAI$/i.test(trimmed)) return trimmed.replace(/CONTAI$/i, "CONTAINS 'value'")
  if (/\s+CONTAIN$/i.test(trimmed)) return trimmed.replace(/CONTAIN$/i, "CONTAINS 'value'")
  if (/\s+CONTAINS$/i.test(trimmed)) return trimmed + " 'value'"
  if (/CONTAINS\s+'$/i.test(trimmed)) return trimmed + "value'"

  // After | — suggest ||
  if (/\|\s*$/.test(trimmed) && !/\|\|/.test(trimmed.slice(-3))) {
    return trimmed + "| TAG['key'] == 'value'"
  }

  return ''
}

function getValueGhost(value: string): string {
  if (value === '') return "'Result Value'"
  if (value === "'") return "'Result Value'"
  return ''
}

export function ExpressionInput({ value, onChange, mode, placeholder, className }: ExpressionInputProps) {
  const [isFocused, setIsFocused] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const ghostRef = useRef<HTMLSpanElement>(null)

  const ghost = mode === 'match' ? getMatchGhost(value) : getValueGhost(value)
  // Only show the part of the ghost that extends beyond what user typed
  const ghostSuffix = ghost.startsWith(value) ? ghost.slice(value.length) : ''
  const showGhost = isFocused && ghostSuffix.length > 0

  const handleKeyDown = useCallback((e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Tab' && showGhost) {
      e.preventDefault()
      onChange(ghost)
      // Move cursor to end
      requestAnimationFrame(() => {
        if (inputRef.current) {
          inputRef.current.selectionStart = inputRef.current.selectionEnd = ghost.length
        }
      })
    }
  }, [showGhost, ghost, onChange])

  const handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    onChange(e.target.value)
  }

  const handleBlur = () => {
    setIsFocused(false)
    // Auto-wrap value expressions in quotes if missing
    if (mode === 'value' && value && !value.startsWith("'") && !value.startsWith('"')) {
      onChange(`'${value}'`)
    }
  }

  const accentColor = mode === 'match' ? 'indigo' : 'emerald'
  const borderClass = isFocused
    ? mode === 'match' ? 'border-l-4 border-l-indigo-500 bg-indigo-50/50' : 'border-l-4 border-l-emerald-500 bg-emerald-50/50'
    : ''

  return (
    <div className={cn('relative', className)}>
      <div className={cn(
        'relative flex items-center rounded-lg border border-gray-300 transition-all',
        borderClass,
        isFocused && `ring-2 ring-${accentColor}-500/20 ring-offset-1`
      )}>
        {/* Ghost text layer */}
        <div className="absolute inset-0 flex items-center px-3 pointer-events-none overflow-hidden">
          {/* Invisible text matching user input to position ghost */}
          <span className="text-xs font-mono whitespace-pre invisible">{value}</span>
          {/* Ghost suffix */}
          {showGhost && (
            <span ref={ghostRef} className="text-xs font-mono text-gray-300 whitespace-pre">
              {ghostSuffix}
            </span>
          )}
        </div>
        {/* Actual input */}
        <input
          ref={inputRef}
          type="text"
          value={value}
          onChange={handleChange}
          onFocus={() => setIsFocused(true)}
          onBlur={handleBlur}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          className="w-full h-10 px-3 py-2 text-xs font-mono bg-transparent rounded-lg focus:outline-none placeholder:text-gray-400"
        />
      </div>
      {/* Tab hint */}
      {showGhost && (
        <div className="absolute right-2 top-1/2 -translate-y-1/2 pointer-events-none">
          <span className="text-[10px] text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded font-sans">Tab</span>
        </div>
      )}
    </div>
  )
}
