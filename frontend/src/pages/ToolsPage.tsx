import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Play, Upload, RefreshCw, Calendar } from 'lucide-react'
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
  const [weekNumber, setWeekNumber] = useState(1)
  const [syncYear, setSyncYear] = useState(new Date().getFullYear())
  const [syncMonth, setSyncMonth] = useState(new Date().getMonth() + 1)
  const [fromYear, setFromYear] = useState(new Date().getFullYear())
  const [fromMonth, setFromMonth] = useState(1)
  const [toYear, setToYear] = useState(new Date().getFullYear())
  const [toMonth, setToMonth] = useState(new Date().getMonth() + 1)
  const [forceAll, setForceAll] = useState(false)

  const filesQuery = useQuery({
    queryKey: ['output-files'],
    queryFn: () => api.listSimulationFiles(),
  })

  const syncHistoryQuery = useQuery({
    queryKey: ['sync-history'],
    queryFn: () => api.getSyncHistory(10),
  })

  const weekSyncMutation = useMutation({
    mutationFn: () => api.startWeekSync(weekNumber, syncYear, forceAll),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['sync-history'] }),
  })

  const monthSyncMutation = useMutation({
    mutationFn: () => api.startMonthSync(syncYear, syncMonth, forceAll),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['sync-history'] }),
  })

  const rangeSyncMutation = useMutation({
    mutationFn: () => api.startRangeSync(fromYear, fromMonth, toYear, toMonth, forceAll),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['sync-history'] }),
  })

  const resetMutation = useMutation({
    mutationFn: () => api.forceReset(),
  })

  const uploadMutation = useMutation({
    mutationFn: (file: string) => api.startVTagUpload(file),
  })

  const handleSync = () => {
    switch (syncMode) {
      case 'week': weekSyncMutation.mutate(); break
      case 'month': monthSyncMutation.mutate(); break
      case 'range': rangeSyncMutation.mutate(); break
    }
  }

  const isSyncing = weekSyncMutation.isPending || monthSyncMutation.isPending || rangeSyncMutation.isPending

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
              <div className="flex items-end gap-4">
                <div>
                  <label className="text-xs font-medium text-gray-500">Week</label>
                  <Input type="number" value={weekNumber} onChange={(e) => setWeekNumber(Number(e.target.value))} className="w-24" />
                </div>
                <div>
                  <label className="text-xs font-medium text-gray-500">Year</label>
                  <Input type="number" value={syncYear} onChange={(e) => setSyncYear(Number(e.target.value))} className="w-24" />
                </div>
                <label className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={forceAll} onChange={(e) => setForceAll(e.target.checked)} />
                  Force all
                </label>
                <Button onClick={handleSync} disabled={isSyncing}>
                  <Play className="h-4 w-4 mr-1" /> {isSyncing ? 'Syncing...' : 'Start'}
                </Button>
              </div>
            </TabsContent>

            <TabsContent value="month" className="mt-4">
              <div className="flex items-end gap-4">
                <div>
                  <label className="text-xs font-medium text-gray-500">Year</label>
                  <Input type="number" value={syncYear} onChange={(e) => setSyncYear(Number(e.target.value))} className="w-24" />
                </div>
                <div>
                  <label className="text-xs font-medium text-gray-500">Month</label>
                  <Input type="number" min={1} max={12} value={syncMonth} onChange={(e) => setSyncMonth(Number(e.target.value))} className="w-24" />
                </div>
                <label className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={forceAll} onChange={(e) => setForceAll(e.target.checked)} />
                  Force all
                </label>
                <Button onClick={handleSync} disabled={isSyncing}>
                  <Play className="h-4 w-4 mr-1" /> {isSyncing ? 'Syncing...' : 'Start'}
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
                <label className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={forceAll} onChange={(e) => setForceAll(e.target.checked)} />
                  Force all
                </label>
                <Button onClick={handleSync} disabled={isSyncing}>
                  <Play className="h-4 w-4 mr-1" /> {isSyncing ? 'Syncing...' : 'Start'}
                </Button>
              </div>
            </TabsContent>
          </Tabs>

          {/* Sync History */}
          {syncHistoryQuery.data && (syncHistoryQuery.data as Array<{sync_id: string; year: number; month: number; status: string; started_at: string}>).length > 0 && (
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
                  {(syncHistoryQuery.data as Array<{sync_id: string; year: number; month: number; status: string; started_at: string}>).map((sync) => (
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

      {/* Upload Virtual Tags */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Upload className="h-4 w-4" /> Upload Virtual Tags
          </CardTitle>
        </CardHeader>
        <CardContent>
          {filesQuery.data?.files && filesQuery.data.files.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>File</TableHead>
                  <TableHead>Size</TableHead>
                  <TableHead>Modified</TableHead>
                  <TableHead></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filesQuery.data.files.map((file) => (
                  <TableRow key={file.name}>
                    <TableCell className="font-mono text-xs">{file.name}</TableCell>
                    <TableCell className="text-sm">{file.size_mb} MB</TableCell>
                    <TableCell className="text-xs text-gray-400">{file.modified_at}</TableCell>
                    <TableCell>
                      <Button size="sm" variant="outline" onClick={() => uploadMutation.mutate(file.path)} disabled={uploadMutation.isPending}>
                        Upload
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <p className="text-sm text-gray-400">No output files available. Run a simulation first.</p>
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
