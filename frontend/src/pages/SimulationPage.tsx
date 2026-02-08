import { useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { Play, Search } from 'lucide-react'
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
  const [resolveTags, setResolveTags] = useState('')

  const dimensionsQuery = useQuery({
    queryKey: ['dimensions'],
    queryFn: () => api.listDimensions(),
  })

  const resultsQuery = useQuery({
    queryKey: ['simulation-results'],
    queryFn: () => api.getSimulationResults(),
    refetchInterval: 5000,
  })

  const simulationMutation = useMutation({
    mutationFn: () => api.runSimulation(weekNumber, year, filterMode, maxRecords),
  })

  const resolveMutation = useMutation({
    mutationFn: (tags: Record<string, string>) => api.resolveTags(tags),
  })

  const results: SimulationResults | null = resultsQuery.data ?? null

  const handleResolve = () => {
    try {
      const tags = JSON.parse(resolveTags)
      resolveMutation.mutate(tags)
    } catch {
      // Try key=value format
      const tags: Record<string, string> = {}
      resolveTags.split('\n').forEach(line => {
        const [key, ...rest] = line.split('=')
        if (key && rest.length) tags[key.trim()] = rest.join('=').trim()
      })
      if (Object.keys(tags).length > 0) resolveMutation.mutate(tags)
    }
  }

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
            <Button onClick={() => simulationMutation.mutate()} disabled={simulationMutation.isPending}>
              <Play className="h-4 w-4 mr-1" />
              {simulationMutation.isPending ? 'Running...' : 'Run'}
            </Button>
          </div>
          {dimensionsQuery.data && (dimensionsQuery.data as Array<{vtag_name: string}>).length > 0 && (
            <div className="mt-3 flex flex-wrap gap-1">
              <span className="text-xs text-gray-400">Loaded dimensions:</span>
              {(dimensionsQuery.data as Array<{vtag_name: string}>).map((d) => (
                <Badge key={d.vtag_name} variant="secondary" className="text-xs">{d.vtag_name}</Badge>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Results */}
      {results && results.completed && (
        <>
          <div className="grid grid-cols-4 gap-4">
            <MetricCard title="Total Processed" value={results.total_processed.toLocaleString()} />
            <MetricCard title="Dimension Matches" value={results.dimension_matches.toLocaleString()} />
            <MetricCard title="Unallocated" value={results.unallocated.toLocaleString()} />
            <MetricCard
              title="Match Rate"
              value={results.total_processed > 0
                ? `${((results.dimension_matches / results.total_processed) * 100).toFixed(1)}%`
                : '0%'
              }
              subtitle={`${results.duration_seconds.toFixed(1)}s`}
            />
          </div>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base">Sample Results ({results.samples.length})</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-auto max-h-96">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Status</TableHead>
                      <TableHead>Resource ID</TableHead>
                      <TableHead>Account</TableHead>
                      {(results.vtag_names || []).map((name) => (
                        <TableHead key={name}>{name}</TableHead>
                      ))}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {results.samples.map((sample, i) => {
                      const hasMatch = Object.values(sample.dimensions || {}).some(v => v !== 'Unallocated')
                      return (
                        <TableRow key={i}>
                          <TableCell>
                            <Badge variant={hasMatch ? 'success' : 'warning'} className="text-[10px]">
                              {hasMatch ? 'Matched' : 'Unallocated'}
                            </Badge>
                          </TableCell>
                          <TableCell className="font-mono text-xs max-w-[200px] truncate">{sample.resource_id}</TableCell>
                          <TableCell className="text-xs">{sample.account_id}</TableCell>
                          {(results.vtag_names || []).map((name) => (
                            <TableCell key={name} className="text-xs">
                              <span className={sample.dimensions?.[name] === 'Unallocated' ? 'text-gray-400' : 'font-medium'}>
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

      {/* Tag Resolution */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Resolve Tags</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <textarea
            className="w-full h-24 p-3 font-mono text-xs border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500"
            value={resolveTags}
            onChange={(e) => setResolveTags(e.target.value)}
            placeholder='{"environment": "production", "team": "platform"}'
          />
          <div className="flex gap-2">
            <Button size="sm" onClick={handleResolve} disabled={resolveMutation.isPending}>
              <Search className="h-4 w-4 mr-1" /> Resolve
            </Button>
          </div>
          {resolveMutation.data && (
            <div className="p-3 bg-gray-50 rounded-lg">
              <h4 className="text-sm font-medium mb-2">Results</h4>
              {Object.entries(resolveMutation.data.dimensions || {}).map(([key, value]) => (
                <div key={key} className="flex items-center gap-2 text-sm">
                  <span className="font-medium">{key}:</span>
                  <span className={value === 'Unallocated' ? 'text-gray-400' : ''}>{value}</span>
                  <Badge variant="secondary" className="text-[10px]">
                    {resolveMutation.data?.dimension_sources?.[key] || 'default'}
                  </Badge>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
