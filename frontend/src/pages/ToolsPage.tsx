import { useState, useEffect, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Play, RefreshCw, Calendar, Square, Loader2 } from 'lucide-react'
import { api } from '@/lib/api'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

export function ToolsPage() {
  const queryClient = useQueryClient()
  const [syncMode, setSyncMode] = useState('week')
  const [syncDate, setSyncDate] = useState(() => {
    const d = new Date()
    return d.toISOString().split('T')[0]
  })
  const [syncYear, setSyncYear] = useState(new Date().getFullYear())

  // Compute ISO week number and date range from selected date
  const getISOWeekInfo = (dateStr: string) => {
    const d = new Date(dateStr + 'T00:00:00')
    // ISO week: Thursday determines the week's year
    const day = d.getDay() || 7 // Mon=1..Sun=7
    const thursday = new Date(d)
    thursday.setDate(d.getDate() + 4 - day)
    const yearStart = new Date(thursday.getFullYear(), 0, 1)
    const weekNumber = Math.ceil(((thursday.getTime() - yearStart.getTime()) / 86400000 + 1) / 7)
    const year = thursday.getFullYear()
    // Week start (Monday) and end (Sunday)
    const monday = new Date(d)
    monday.setDate(d.getDate() - day + 1)
    const sunday = new Date(monday)
    sunday.setDate(monday.getDate() + 6)
    const fmt = (dt: Date) => dt.toISOString().split('T')[0]
    return { weekNumber, year, start: fmt(monday), end: fmt(sunday) }
  }
  const weekInfo = getISOWeekInfo(syncDate)
  const weekNumber = weekInfo.weekNumber
  const [syncMonth, setSyncMonth] = useState(new Date().getMonth() + 1)
  const [fromYear, setFromYear] = useState(new Date().getFullYear())
  const [fromMonth, setFromMonth] = useState(1)
  const [toYear, setToYear] = useState(new Date().getFullYear())
  const [toMonth, setToMonth] = useState(new Date().getMonth() + 1)
  const [filterMode, setFilterMode] = useState('all')
  // startingSync: local flag, true from click until backend confirms running
  const [startingSync, setStartingSync] = useState(false)
  const [syncLabel, setSyncLabel] = useState('')
  const [selectedAccounts, setSelectedAccounts] = useState<string[]>([])
  const [accountDropdownOpen, setAccountDropdownOpen] = useState(false)
  const accountDropdownRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!accountDropdownOpen) return
    const handleClickOutside = (e: MouseEvent) => {
      if (accountDropdownRef.current && !accountDropdownRef.current.contains(e.target as Node)) {
        setAccountDropdownOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [accountDropdownOpen])

  const dimensionsQuery = useQuery({
    queryKey: ['dimensions'],
    queryFn: () => api.listDimensions(),
  })

  const accountsQuery = useQuery({
    queryKey: ['accounts'],
    queryFn: () => api.getAccounts(),
  })

  const syncHistoryQuery = useQuery({
    queryKey: ['sync-history'],
    queryFn: () => api.getSyncHistory(10),
  })

  // Always poll every 2s — simple, no conditional
  const syncProgressQuery = useQuery({
    queryKey: ['sync-progress'],
    queryFn: () => api.getSyncProgress(),
    refetchInterval: 2000,
  })

  const acctKeys = selectedAccounts.length > 0 ? selectedAccounts : undefined

  const [syncError, setSyncError] = useState('')

  const onSyncError = (err: Error) => {
    console.log(`[SYNC] mutation error: ${err.message}`)
    setStartingSync(false)
    setSyncError(err.message)
  }

  const onSyncSuccess = (label: string) => {
    console.log(`[SYNC] mutation success, label=${label}`)
    setSyncError('')
    setSyncLabel(label)
    // Force immediate refetch so UI picks up "running" status
    queryClient.invalidateQueries({ queryKey: ['sync-progress'] })
  }

  const weekSyncMutation = useMutation({
    mutationFn: () => api.startWeekSync(weekNumber, weekInfo.year, filterMode, undefined, acctKeys),
    onSuccess: () => {
      const acctLabel = selectedAccounts.length > 0 ? ` (${selectedAccounts.length} accounts)` : ' (all accounts)'
      onSyncSuccess(`W${weekInfo.weekNumber} / ${weekInfo.year}  (${weekInfo.start} \u2192 ${weekInfo.end})${acctLabel}`)
    },
    onError: onSyncError,
  })

  const monthSyncMutation = useMutation({
    mutationFn: () => api.startMonthSync(syncYear, syncMonth, filterMode, undefined, acctKeys),
    onSuccess: () => {
      const acctLabel = selectedAccounts.length > 0 ? ` (${selectedAccounts.length} accounts)` : ' (all accounts)'
      onSyncSuccess(`${syncYear}-${String(syncMonth).padStart(2, '0')}${acctLabel}`)
    },
    onError: onSyncError,
  })

  const rangeSyncMutation = useMutation({
    mutationFn: () => api.startRangeSync(fromYear, fromMonth, toYear, toMonth, filterMode, undefined, acctKeys),
    onSuccess: () => {
      const acctLabel = selectedAccounts.length > 0 ? ` (${selectedAccounts.length} accounts)` : ' (all accounts)'
      onSyncSuccess(`${fromYear}-${String(fromMonth).padStart(2, '0')} \u2192 ${toYear}-${String(toMonth).padStart(2, '0')}${acctLabel}`)
    },
    onError: onSyncError,
  })

  const cancelMutation = useMutation({
    mutationFn: () => api.cancelSync(),
    onSuccess: () => {
      setStartingSync(false)
      queryClient.invalidateQueries({ queryKey: ['sync-progress'] })
      queryClient.invalidateQueries({ queryKey: ['sync-history'] })
    },
  })

  const resetMutation = useMutation({
    mutationFn: () => api.forceReset(),
    onSuccess: () => {
      setStartingSync(false)
      queryClient.invalidateQueries({ queryKey: ['sync-progress'] })
    },
  })

  const handleSync = () => {
    console.log(`[SYNC] handleSync mode=${syncMode}`)
    setStartingSync(true)
    setSyncError('')
    switch (syncMode) {
      case 'week': weekSyncMutation.mutate(); break
      case 'month': monthSyncMutation.mutate(); break
      case 'range': rangeSyncMutation.mutate(); break
    }
  }

  const isMutating = weekSyncMutation.isPending || monthSyncMutation.isPending || rangeSyncMutation.isPending

  // Build dimension names for the filter label
  const dimNames = dimensionsQuery.data?.map(d => d.vtag_name) || []

  const progress = syncProgressQuery.data
  // isRunning is derived purely from backend status — single source of truth
  const isRunning = progress?.status === 'running' || progress?.status === 'cancelling'

  // Clear startingSync once backend confirms running (or if it errors)
  useEffect(() => {
    if (startingSync && (isRunning || syncError)) {
      setStartingSync(false)
    }
  }, [startingSync, isRunning, syncError])

  const accounts = accountsQuery.data?.accounts || []
  const cloudIcon = (type: string) => {
    switch (type) {
      case 'AWS': return '🅰'
      case 'GCP': return '🇬'
      case 'Azure': return '🅼'
      default: return '☁'
    }
  }

  const toggleAccount = (key: string) => {
    setSelectedAccounts(prev =>
      prev.includes(key) ? prev.filter(k => k !== key) : [...prev, key]
    )
  }

  const accountSelect = (
    <div className="relative" ref={accountDropdownRef}>
      <label className="text-xs font-medium text-gray-500">Accounts</label>
      <button
        type="button"
        className="flex h-10 min-w-[180px] items-center justify-between rounded-lg border border-gray-300 bg-background px-3 py-2 text-sm"
        onClick={() => setAccountDropdownOpen(!accountDropdownOpen)}
      >
        <span className={selectedAccounts.length === 0 ? 'text-gray-500' : ''}>
          {selectedAccounts.length === 0
            ? 'All Accounts'
            : `${selectedAccounts.length} selected`}
        </span>
        <span className="ml-2 text-gray-400">▾</span>
      </button>
      {accountDropdownOpen && (
        <div className="absolute z-50 mt-1 max-h-64 w-80 overflow-auto rounded-lg border bg-white shadow-lg">
          <div className="flex items-center justify-between border-b px-3 py-2">
            <button
              className="text-xs text-blue-600 hover:underline"
              onClick={() => setSelectedAccounts(accounts.map(a => String(a.accountKey)))}
            >Select All</button>
            <button
              className="text-xs text-gray-500 hover:underline"
              onClick={() => setSelectedAccounts([])}
            >Clear</button>
          </div>
          {accounts.map(acc => (
            <label
              key={acc.accountKey}
              className="flex cursor-pointer items-center gap-2 px-3 py-1.5 hover:bg-gray-50"
            >
              <input
                type="checkbox"
                checked={selectedAccounts.includes(String(acc.accountKey))}
                onChange={() => toggleAccount(String(acc.accountKey))}
                className="rounded"
              />
              <span className="text-xs">{cloudIcon(acc.cloudType)}</span>
              <span className="text-sm truncate">{acc.accountId}</span>
              <span className="text-xs text-gray-400 truncate">{acc.accountName}</span>
            </label>
          ))}
        </div>
      )}
    </div>
  )

  const filterSelect = (
    <div>
      <label className="text-xs font-medium text-gray-500">Assets</label>
      <select
        className="flex h-10 rounded-lg border border-gray-300 bg-background px-3 py-2 text-sm"
        value={filterMode}
        onChange={(e) => setFilterMode(e.target.value)}
      >
        <option value="all">All Assets</option>
        <option value="not_vtagged">
          Without VTags{dimNames.length > 0 ? ` (${dimNames.join(', ')})` : ''}
        </option>
      </select>
    </div>
  )

  return (
    <div className="space-y-6">
      {/* Sync */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Calendar className="h-4 w-4" /> Sync
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Tabs value={syncMode} onValueChange={setSyncMode}>
            <TabsList>
              <TabsTrigger value="week">Week</TabsTrigger>
              <TabsTrigger value="month">Month</TabsTrigger>
              <TabsTrigger value="range">Range</TabsTrigger>
            </TabsList>

            <TabsContent value="week" className="mt-4">
              <div className="flex items-end gap-4 flex-wrap">
                <div>
                  <label className="text-xs font-medium text-gray-500">Pick a date</label>
                  <Input type="date" value={syncDate} onChange={(e) => setSyncDate(e.target.value)} className="w-40" />
                </div>
                <div className="pb-2">
                  <span className="text-sm font-medium text-gray-700">W{weekInfo.weekNumber} / {weekInfo.year}</span>
                  <span className="text-xs text-gray-400 ml-2">{weekInfo.start} &rarr; {weekInfo.end}</span>
                </div>
                {accountSelect}
                {filterSelect}
                <Button onClick={handleSync} disabled={isMutating || isRunning || startingSync}>
                  <Play className="h-4 w-4 mr-1" /> {isMutating || startingSync ? 'Starting...' : 'Start'}
                </Button>
              </div>
            </TabsContent>

            <TabsContent value="month" className="mt-4">
              <div className="flex items-end gap-4 flex-wrap">
                <div>
                  <label className="text-xs font-medium text-gray-500">Year</label>
                  <Input type="number" value={syncYear} onChange={(e) => setSyncYear(Number(e.target.value))} className="w-24" />
                </div>
                <div>
                  <label className="text-xs font-medium text-gray-500">Month</label>
                  <Input type="number" min={1} max={12} value={syncMonth} onChange={(e) => setSyncMonth(Number(e.target.value))} className="w-24" />
                </div>
                {accountSelect}
                {filterSelect}
                <Button onClick={handleSync} disabled={isMutating || isRunning || startingSync}>
                  <Play className="h-4 w-4 mr-1" /> {isMutating || startingSync ? 'Starting...' : 'Start'}
                </Button>
              </div>
            </TabsContent>

            <TabsContent value="range" className="mt-4">
              <div className="flex items-end gap-4 flex-wrap">
                <div>
                  <label className="text-xs font-medium text-gray-500">From Year</label>
                  <Input type="number" value={fromYear} onChange={(e) => setFromYear(Number(e.target.value))} className="w-24" />
                </div>
                <div>
                  <label className="text-xs font-medium text-gray-500">From Month</label>
                  <Input type="number" min={1} max={12} value={fromMonth} onChange={(e) => setFromMonth(Number(e.target.value))} className="w-24" />
                </div>
                <div>
                  <label className="text-xs font-medium text-gray-500">To Year</label>
                  <Input type="number" value={toYear} onChange={(e) => setToYear(Number(e.target.value))} className="w-24" />
                </div>
                <div>
                  <label className="text-xs font-medium text-gray-500">To Month</label>
                  <Input type="number" min={1} max={12} value={toMonth} onChange={(e) => setToMonth(Number(e.target.value))} className="w-24" />
                </div>
                {accountSelect}
                {filterSelect}
                <Button onClick={handleSync} disabled={isMutating || isRunning || startingSync}>
                  <Play className="h-4 w-4 mr-1" /> {isMutating || startingSync ? 'Starting...' : 'Start'}
                </Button>
              </div>
            </TabsContent>
          </Tabs>

          {/* Sync error */}
          {syncError && (
            <div className="mt-4 rounded-lg border border-red-200 bg-red-50 p-4">
              <div className="flex items-center gap-2">
                <span className="text-sm font-medium text-red-800">Sync failed: {syncError}</span>
              </div>
            </div>
          )}

          {/* Starting indicator (before backend confirms running) */}
          {startingSync && !isRunning && (
            <div className="mt-4 rounded-lg border border-blue-200 bg-blue-50 p-4">
              <div className="flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin text-blue-600" />
                <span className="text-sm font-medium text-blue-800">Starting sync...</span>
              </div>
            </div>
          )}

          {/* Live Sync Progress */}
          {isRunning && progress && (
            <div className="mt-4 rounded-lg border border-blue-200 bg-blue-50 p-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin text-blue-600" />
                  <span className="text-sm font-medium text-blue-800">
                    {progress.status === 'cancelling' ? 'Cancelling...' : 'Sync Running'}
                  </span>
                  {syncLabel && (
                    <span className="text-xs font-medium text-blue-700">{syncLabel}</span>
                  )}
                  {progress.phase && (
                    <span className="text-xs text-blue-600">({progress.phase})</span>
                  )}
                </div>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => cancelMutation.mutate()}
                  disabled={cancelMutation.isPending || progress.status === 'cancelling'}
                >
                  <Square className="h-3 w-3 mr-1" /> Cancel
                </Button>
              </div>
              <div className="grid grid-cols-4 gap-4 text-sm">
                <div>
                  <span className="text-gray-500">Assets</span>
                  <p className="font-mono font-medium">{progress.processed_assets?.toLocaleString()}</p>
                </div>
                <div>
                  <span className="text-gray-500">Matched</span>
                  <p className="font-mono font-medium text-green-600">{progress.matched_assets?.toLocaleString()}</p>
                </div>
                <div>
                  <span className="text-gray-500">Unmatched</span>
                  <p className="font-mono font-medium text-gray-500">{progress.unmatched_assets?.toLocaleString()}</p>
                </div>
                <div>
                  <span className="text-gray-500">Elapsed</span>
                  <p className="font-mono font-medium">{Math.round(progress.elapsed_seconds || 0)}s</p>
                </div>
              </div>
            </div>
          )}

          {/* Sync completed message */}
          {!startingSync && !isRunning && progress?.status === 'completed' && progress.processed_assets > 0 && (
            <div className={`mt-4 rounded-lg border p-4 ${
              progress.matched_assets > 0
                ? 'border-green-200 bg-green-50'
                : 'border-amber-200 bg-amber-50'
            }`}>
              <div className="flex items-center gap-2 mb-2">
                <Badge variant={progress.matched_assets > 0 ? 'success' : 'warning'}>
                  {progress.matched_assets > 0 ? 'Completed' : 'No Matches'}
                </Badge>
                {progress.matched_assets === 0 && (
                  <span className="text-sm text-amber-700">No assets matched any dimension rules. Nothing was uploaded.</span>
                )}
              </div>
              <div className="grid grid-cols-4 gap-4 text-sm">
                <div>
                  <span className="text-gray-500">Assets</span>
                  <p className="font-mono font-medium">{progress.processed_assets?.toLocaleString()}</p>
                </div>
                <div>
                  <span className="text-gray-500">Matched</span>
                  <p className="font-mono font-medium text-green-600">{progress.matched_assets?.toLocaleString()}</p>
                </div>
                <div>
                  <span className="text-gray-500">Unmatched</span>
                  <p className="font-mono font-medium text-gray-500">{progress.unmatched_assets?.toLocaleString()}</p>
                </div>
                <div>
                  <span className="text-gray-500">Elapsed</span>
                  <p className="font-mono font-medium">{Math.round(progress.elapsed_seconds || 0)}s</p>
                </div>
              </div>
            </div>
          )}

          {/* Sync History */}
          {syncHistoryQuery.data && syncHistoryQuery.data.length > 0 && (
            <div className="mt-6 border-t pt-4">
              <h4 className="text-sm font-medium mb-2">Recent Syncs</h4>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>ID</TableHead>
                    <TableHead>Period</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Started</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {syncHistoryQuery.data.map((sync) => (
                    <TableRow key={sync.sync_id}>
                      <TableCell className="font-mono text-xs">{sync.sync_id.slice(0, 8)}</TableCell>
                      <TableCell className="text-sm">{sync.year}-{String(sync.month).padStart(2, '0')}</TableCell>
                      <TableCell>
                        <Badge variant={sync.status === 'completed' ? 'success' : sync.status === 'error' ? 'error' : 'secondary'}>
                          {sync.status}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-xs text-gray-400">{new Date(sync.started_at).toLocaleString()}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Force Reset */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <RefreshCw className="h-4 w-4" /> Force Reset
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-gray-500 mb-3">
            Reset the agent state if it gets stuck. This does not delete any data.
          </p>
          <Button variant="destructive" size="sm" onClick={() => resetMutation.mutate()} disabled={resetMutation.isPending}>
            {resetMutation.isPending ? 'Resetting...' : 'Force Reset'}
          </Button>
          {resetMutation.data && (
            <p className="text-sm text-green-600 mt-2">{resetMutation.data.message}</p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
