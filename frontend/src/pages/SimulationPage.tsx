import { useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { Play } from 'lucide-react'
import { api } from '@/lib/api'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { MetricCard } from '@/components/common/MetricCard'
import type { SimulationResults } from '@/types'

export function SimulationPage() {
  const [weekNumber, setWeekNumber] = useState(() => {
    const now = new Date()
    const start = new Date(now.getFullYear(), 0, 1)
    const diff = now.getTime() - start.getTime()
    return Math.ceil(diff / (7 * 24 * 60 * 60 * 1000))
  })
  const [year, setYear] = useState(new Date().getFullYear())
  const [filterMode, setFilterMode] = useState('all')
  const [maxRecords, setMaxRecords] = useState(1000)

  const dimensionsQuery = useQuery({
    queryKey: ['dimensions'],
    queryFn: () => api.listDimensions(),
  })

  const resultsQuery = useQuery({
    queryKey: ['simulation-results'],
    queryFn: () => api.getSimulationResults(),
    refetchInterval: (query) => {
      const data = query.state.data
      return data?.status === 'running' ? 2000 : 5000
    },
  })

  const simulationMutation = useMutation({
    mutationFn: () => api.runSimulation(weekNumber, year, filterMode, maxRecords),
  })

  const results: (SimulationResults & { phase?: string }) | null = resultsQuery.data ?? null
  const isCompleted = results?.status === 'completed'
  const isRunning = results?.status === 'running'

  return (
    <div className="space-y-6">
      {/* Controls */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Run Simulation</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-end gap-4">
            <div>
              <label className="text-xs font-medium text-gray-500">Week</label>
              <Input type="number" value={weekNumber} onChange={(e) => setWeekNumber(Number(e.target.value))} className="w-24" />
            </div>
            <div>
              <label className="text-xs font-medium text-gray-500">Year</label>
              <Input type="number" value={year} onChange={(e) => setYear(Number(e.target.value))} className="w-24" />
            </div>
            <div>
              <label className="text-xs font-medium text-gray-500">Filter</label>
              <select
                className="flex h-10 rounded-lg border border-gray-300 bg-background px-3 py-2 text-sm"
                value={filterMode}
                onChange={(e) => setFilterMode(e.target.value)}
              >
                <option value="all">All</option>
                <option value="not_vtagged">Not Tagged</option>
              </select>
            </div>
            <div>
              <label className="text-xs font-medium text-gray-500">Max Records</label>
              <Input type="number" value={maxRecords} onChange={(e) => setMaxRecords(Number(e.target.value))} className="w-28" />
            </div>
            <Button onClick={() => simulationMutation.mutate()} disabled={simulationMutation.isPending || isRunning}>
              <Play className="h-4 w-4 mr-1" />
              {simulationMutation.isPending || isRunning ? 'Running...' : 'Run'}
            </Button>
          </div>
          {dimensionsQuery.data && dimensionsQuery.data.length > 0 && (
            <div className="mt-3 flex flex-wrap gap-1">
              <span className="text-xs text-gray-400">Loaded dimensions:</span>
              {dimensionsQuery.data.map((d) => (
                <Badge key={d.vtag_name} variant="secondary" className="text-xs">{d.vtag_name}</Badge>
              ))}
            </div>
          )}
          {results?.error_message && (
            <div className="mt-3 text-sm text-red-600">
              Error: {results.error_message}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Live progress while running */}
      {results && isRunning && (
        <Card>
          <CardContent className="py-4">
            <div className="flex items-center gap-4">
              <div className="h-2 flex-1 bg-gray-100 rounded-full overflow-hidden">
                <div className="h-full bg-primary-500 rounded-full animate-pulse" style={{ width: '100%' }} />
              </div>
            </div>
            <div className="mt-3 grid grid-cols-4 gap-4 text-center">
              <div>
                <div className="text-lg font-semibold">{(results.total_assets || 0).toLocaleString()}</div>
                <div className="text-xs text-gray-500">Assets Processed</div>
              </div>
              <div>
                <div className="text-lg font-semibold">{(results.matched_assets || 0).toLocaleString()}</div>
                <div className="text-xs text-gray-500">Matched</div>
              </div>
              <div>
                <div className="text-lg font-semibold">{(results.unmatched_assets || 0).toLocaleString()}</div>
                <div className="text-xs text-gray-500">Unmatched</div>
              </div>
              <div>
                <div className="text-lg font-semibold">{Math.round(results.elapsed_seconds || 0)}s</div>
                <div className="text-xs text-gray-500">Elapsed</div>
              </div>
            </div>
            {results.phase && (
              <div className="mt-2 text-xs text-gray-400 text-center">{results.phase}</div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Completed Results */}
      {results && isCompleted && (
        <>
          <div className="grid grid-cols-4 gap-4">
            <MetricCard title="Total Assets" value={results.total_assets.toLocaleString()} />
            <MetricCard title="Matched" value={results.matched_assets.toLocaleString()} />
            <MetricCard title="Unmatched" value={results.unmatched_assets.toLocaleString()} />
            <MetricCard
              title="Match Rate"
              value={`${results.match_rate.toFixed(1)}%`}
              subtitle={`${results.elapsed_seconds}s`}
            />
          </div>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base">Sample Results ({results.sample_records.length})</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-auto max-h-96">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Status</TableHead>
                      <TableHead>Resource ID</TableHead>
                      <TableHead>Account</TableHead>
                      {(results.tag_keys || []).map((key) => (
                        <TableHead key={`tag-${key}`} className="text-blue-600">Tag: {key}</TableHead>
                      ))}
                      {(results.vtag_names || []).map((name) => (
                        <TableHead key={name} className="text-emerald-600">{name}</TableHead>
                      ))}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {results.sample_records.map((sample, i) => {
                      const hasMatch = Object.values(sample.dimensions || {}).some(v => v !== 'Unallocated')
                      return (
                        <TableRow key={i}>
                          <TableCell>
                            <Badge variant={hasMatch ? 'success' : 'warning'} className="text-[10px]">
                              {hasMatch ? 'Matched' : 'Unallocated'}
                            </Badge>
                          </TableCell>
                          <TableCell className="font-mono text-xs max-w-[200px] truncate">{sample.resourceid}</TableCell>
                          <TableCell className="text-xs">{sample.linkedaccid}</TableCell>
                          {(results.tag_keys || []).map((key) => (
                            <TableCell key={`tag-${key}`} className="text-xs text-blue-700 font-mono">
                              {sample.tags?.[key] || <span className="text-gray-300">-</span>}
                            </TableCell>
                          ))}
                          {(results.vtag_names || []).map((name) => (
                            <TableCell key={name} className="text-xs">
                              <span className={sample.dimensions?.[name] === 'Unallocated' ? 'text-gray-400' : 'font-medium text-emerald-700'}>
                                {sample.dimensions?.[name] || '-'}
                              </span>
                            </TableCell>
                          ))}
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        </>
      )}

    </div>
  )
}
