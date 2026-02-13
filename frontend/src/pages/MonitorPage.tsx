import { useState, useEffect } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Activity, Upload, CheckCircle, XCircle, Loader2, RefreshCw } from 'lucide-react'
import { api } from '@/lib/api'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Progress } from '@/components/ui/progress'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

export function MonitorPage() {
  const queryClient = useQueryClient()
  const [importPolling, setImportPolling] = useState(true)

  // Always poll sync progress — same source of truth as Tools page
  const syncProgressQuery = useQuery({
    queryKey: ['sync-progress'],
    queryFn: () => api.getSyncProgress(),
    refetchInterval: 2000,
  })

  // Import status - always fetch fresh on mount, poll faster when active
  const importStatusQuery = useQuery({
    queryKey: ['import-status'],
    queryFn: () => api.getImportStatus(),
    refetchInterval: importPolling ? 5000 : 15000,
    refetchOnMount: 'always',
    staleTime: 0,
  })

  // Auto-start/stop polling based on import statuses
  useEffect(() => {
    const statuses = importStatusQuery.data?.import_statuses
    if (statuses && statuses.length > 0) {
      const allDone = statuses.every(
        s => s.phase === 'completed' || s.phase === 'failed' || s.phase === 'error'
      )
      if (allDone) {
        setImportPolling(false)
      } else if (!importPolling) {
        setImportPolling(true)
      }
    }
  }, [importStatusQuery.data])

  const progress = syncProgressQuery.data
  const isRunning = progress?.status === 'running' || progress?.status === 'cancelling'
  const isCompleted = progress?.status === 'completed'
  const isError = progress?.status === 'error'
  const isCancelled = progress?.status === 'cancelled'
  const lastSync = progress?.last_sync

  const stateColor = () => {
    if (isRunning) return 'default'
    if (isCompleted) return 'success'
    if (isError) return 'error'
    if (isCancelled) return 'warning'
    return 'secondary'
  }

  const stateLabel = () => {
    if (!progress) return 'No data'
    if (isRunning) {
      const phase = progress.phase || 'running'
      switch (phase) {
        case 'authenticating': return 'Authenticating'
        case 'starting': return 'Starting'
        default: return 'Running'
      }
    }
    if (isCompleted) return 'Completed'
    if (isError) return 'Error'
    if (isCancelled) return 'Cancelled'
    if (lastSync) return 'Idle (last sync available)'
    return 'Idle'
  }

  // Pick stats to display: live progress or last sync summary
  const displayStats = isRunning && progress ? {
    processed_assets: progress.processed_assets || 0,
    matched_assets: progress.matched_assets || 0,
    unmatched_assets: progress.unmatched_assets || 0,
  } : lastSync ? {
    total_assets: lastSync.total_assets || 0,
    matched_assets: lastSync.matched_assets || 0,
    unmatched_assets: lastSync.unmatched_assets || 0,
    uploaded: lastSync.uploaded_count || 0,
  } : null

  const elapsed = isRunning
    ? progress?.elapsed_seconds || 0
    : lastSync?.elapsed_seconds || 0

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2">
              <Activity className="h-4 w-4" />
              Agent Status
              {isRunning && <Loader2 className="h-4 w-4 animate-spin text-blue-500" />}
            </CardTitle>
            <Badge variant={stateColor() as 'default' | 'secondary' | 'destructive'}>
              {stateLabel()}
            </Badge>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {isRunning && progress && (
            <>
              <div>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm text-gray-600">
                    {progress.phase || 'Sync running'}
                  </span>
                </div>
                {progress.progress_pct > 0 && <Progress value={progress.progress_pct} />}
              </div>
            </>
          )}

          {!isRunning && lastSync && (
            <p className="text-sm text-gray-600">
              Last {lastSync.sync_type || 'sync'}: {lastSync.start_date} to {lastSync.end_date}
            </p>
          )}

          {!isRunning && isError && progress?.error_message && (
            <p className="text-sm text-red-600">{progress.error_message}</p>
          )}

          {elapsed > 0 && (
            <p className="text-xs text-gray-400">
              Elapsed: {Math.round(elapsed)}s
            </p>
          )}

          {displayStats && Object.keys(displayStats).length > 0 && (
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mt-2">
              {Object.entries(displayStats).map(([key, value]) => (
                <div key={key} className="p-3 bg-gray-50 rounded-lg">
                  <p className="text-xs text-gray-500">{key.replace(/_/g, ' ')}</p>
                  <p className="text-lg font-bold">{typeof value === 'number' ? value.toLocaleString() : String(value)}</p>
                </div>
              ))}
            </div>
          )}

          {!progress && (
            <p className="text-sm text-gray-400">No status data available</p>
          )}
        </CardContent>
      </Card>

      {/* Umbrella Import Status */}
      <Card className="border-blue-200">
        <CardHeader className="flex flex-row items-center justify-between">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Upload className="h-5 w-5" />
              Virtual Tag Uploads
              {(importStatusQuery.isLoading || importStatusQuery.isFetching) && <Loader2 className="h-4 w-4 animate-spin text-blue-500" />}
            </CardTitle>
            <CardDescription>
              {importStatusQuery.data?.last_result
                ? `${String(importStatusQuery.data.last_result.upload_count ?? 0)} payer account(s) uploaded in ${Math.round(Number(importStatusQuery.data.last_result.elapsed_seconds) || 0)}s`
                : 'Import processing status from Umbrella'}
            </CardDescription>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              setImportPolling(true)
              queryClient.invalidateQueries({ queryKey: ['import-status'] })
            }}
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${importStatusQuery.isFetching ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        </CardHeader>
        <CardContent>
          {importStatusQuery.isLoading ? (
            <div className="flex items-center justify-center gap-2 text-gray-400 py-6">
              <Loader2 className="h-4 w-4 animate-spin" />
              <span className="text-sm">Fetching upload statuses from Umbrella...</span>
            </div>
          ) : importStatusQuery.data?.import_statuses && importStatusQuery.data.import_statuses.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-8"></TableHead>
                  <TableHead>Time</TableHead>
                  <TableHead>Sync</TableHead>
                  <TableHead>Payer Account</TableHead>
                  <TableHead>Records</TableHead>
                  <TableHead>Operations</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {[...importStatusQuery.data.import_statuses]
                  .sort((a, b) => {
                    const ta = a.timestamp ? new Date(a.timestamp).getTime() : 0
                    const tb = b.timestamp ? new Date(b.timestamp).getTime() : 0
                    return tb - ta
                  })
                  .map((imp) => (
                  <TableRow key={imp.upload_id}>
                    <TableCell>
                      {imp.phase === 'completed' ? (
                        <CheckCircle className="h-4 w-4 text-green-600" />
                      ) : imp.phase === 'failed' || imp.phase === 'error' ? (
                        <XCircle className="h-4 w-4 text-red-600" />
                      ) : (
                        <Loader2 className="h-4 w-4 text-blue-600 animate-spin" />
                      )}
                    </TableCell>
                    <TableCell className="text-xs text-gray-500">
                      {imp.timestamp ? new Date(imp.timestamp).toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'medium' }) : '-'}
                    </TableCell>
                    <TableCell className="text-xs">
                      {imp.sync_type ? (
                        <div>
                          <Badge variant="outline" className="text-[10px] px-1.5 py-0">
                            {imp.sync_type}
                          </Badge>
                          {imp.start_date && (
                            <span className="text-gray-400 block mt-0.5">
                              {imp.start_date} ~ {imp.end_date}
                            </span>
                          )}
                        </div>
                      ) : '-'}
                    </TableCell>
                    <TableCell className="font-mono text-xs">{imp.account_id || '-'}</TableCell>
                    <TableCell className="text-sm">
                      {(imp.processed_rows ?? 0).toLocaleString()} / {(imp.total_rows ?? 0).toLocaleString()}
                      {imp.errors != null && Number(imp.errors) > 0 && (
                        <span className="text-red-600 ml-1">({imp.errors} err)</span>
                      )}
                    </TableCell>
                    <TableCell className="text-xs">
                      <div className="space-y-0.5">
                        <span className={`block ${Number(imp.updated) > 0 ? 'text-blue-600' : 'text-gray-400'}`}>{Number(imp.updated || 0).toLocaleString()} updated</span>
                        <span className={`block ${Number(imp.inserted) > 0 ? 'text-green-600' : 'text-gray-400'}`}>{Number(imp.inserted || 0).toLocaleString()} inserted</span>
                        <span className={`block ${Number(imp.deleted) > 0 ? 'text-red-600' : 'text-gray-400'}`}>{Number(imp.deleted || 0).toLocaleString()} deleted</span>
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant={
                          imp.status === 'success' || imp.phase === 'completed' ? 'success' :
                          imp.status === 'failed' || imp.phase === 'failed' || imp.phase === 'error' ? 'error' :
                          'default'
                        }
                      >
                        {imp.status || imp.phase}
                      </Badge>
                      {imp.error && (
                        <span className="text-red-500 text-xs block mt-1">{imp.error}</span>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <p className="text-sm text-gray-400 py-4 text-center">No uploads yet. Run a sync to see upload statuses here.</p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
