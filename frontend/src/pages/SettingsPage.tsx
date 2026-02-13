import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Key, Copy, Check, Database, HardDrive } from 'lucide-react'
import { api } from '@/lib/api'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog'


export function SettingsPage() {
  const queryClient = useQueryClient()
  const [retentionDays, setRetentionDays] = useState(30)
  const [confirmCleanup, setConfirmCleanup] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)

  const dimensionsQuery = useQuery({
    queryKey: ['dimensions'],
    queryFn: () => api.listDimensions(),
  })

  const statusQuery = useQuery({
    queryKey: ['system-status'],
    queryFn: () => api.getStatus(),
  })

  const cleanupPreviewQuery = useQuery({
    queryKey: ['cleanup-preview', retentionDays],
    queryFn: () => api.getCleanupPreview(retentionDays),
  })

  const createKeyMutation = useMutation({
    mutationFn: (name: string) => api.createKey(name),
  })

  const cleanupMutation = useMutation({
    mutationFn: (type: string) => {
      if (type === 'soft') return api.performCleanup({ soft: true, retentionDays })
      return api.performCleanup({ deleteFiles: true, cleanDatabase: true })
    },
    onSuccess: () => {
      setConfirmCleanup(null)
      queryClient.invalidateQueries({ queryKey: ['cleanup-preview'] })
    },
  })

  const handleCopyKey = (key: string) => {
    navigator.clipboard.writeText(key)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div className="space-y-6">
      {/* API Keys */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="text-base flex items-center gap-2">
                <Key className="h-4 w-4" /> API Keys
              </CardTitle>
              <CardDescription>Manage authentication keys</CardDescription>
            </div>
            <Button size="sm" onClick={() => createKeyMutation.mutate('cli-key')}>
              Generate New Key
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {createKeyMutation.data && (
            <div className="mb-4 p-3 bg-primary-50 border border-primary-200 rounded-lg">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-primary-800">New Key Generated</p>
                  <code className="text-xs font-mono text-primary-700">{createKeyMutation.data.key}</code>
                </div>
                <Button size="sm" variant="ghost" onClick={() => handleCopyKey(createKeyMutation.data!.key)}>
                  {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
                </Button>
              </div>
            </div>
          )}
          <p className="text-sm text-gray-500">
            Use your API key with the CLI: <code className="bg-gray-100 px-1 rounded">vtagger --api-key &lt;key&gt; ...</code>
          </p>
        </CardContent>
      </Card>

      {/* Cleanup */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <HardDrive className="h-4 w-4" /> Cleanup
          </CardTitle>
          <CardDescription>Remove old files and database records</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center gap-4">
            <div>
              <label className="text-xs font-medium text-gray-500">Retention Days</label>
              <Input type="number" value={retentionDays} onChange={(e) => setRetentionDays(Number(e.target.value))} className="w-24" />
            </div>
            <Button variant="outline" size="sm" onClick={() => setConfirmCleanup('soft')}>
              Soft Cleanup
            </Button>
            <Button variant="destructive" size="sm" onClick={() => setConfirmCleanup('hard')}>
              Hard Cleanup
            </Button>
          </div>

          {cleanupPreviewQuery.data && (
            <div className="p-3 bg-gray-50 rounded-lg space-y-2">
              <p className="text-sm font-medium">Current Data</p>
              {cleanupPreviewQuery.data.output_dir && (
                <p className="text-xs text-gray-500">
                  Files: {cleanupPreviewQuery.data.output_dir.file_count} ({cleanupPreviewQuery.data.output_dir.total_size_mb?.toFixed(1) ?? 0} MB)
                </p>
              )}
              {cleanupPreviewQuery.data.tables && Object.entries(cleanupPreviewQuery.data.tables).map(([table, count]) => (
                <p key={table} className="text-xs text-gray-500">
                  {table}: {count} records
                </p>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* System Info */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Database className="h-4 w-4" /> System Info
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-3 gap-4">
            <div className="p-3 bg-gray-50 rounded-lg">
              <p className="text-xs text-gray-500">Status</p>
              <p className="text-sm font-medium">
                <Badge variant={statusQuery.data?.state === 'idle' ? 'success' : 'secondary'}>
                  {statusQuery.data?.state || 'Unknown'}
                </Badge>
              </p>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <p className="text-xs text-gray-500">Dimensions Loaded</p>
              <p className="text-sm font-medium">{dimensionsQuery.data?.length ?? 0}</p>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <p className="text-xs text-gray-500">Version</p>
              <p className="text-sm font-medium">VTagger v1.0.0</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Cleanup Confirmation Dialog */}
      <Dialog open={!!confirmCleanup} onOpenChange={() => setConfirmCleanup(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{confirmCleanup === 'hard' ? 'Hard Cleanup' : 'Soft Cleanup'}</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-gray-600">
            {confirmCleanup === 'hard'
              ? 'This will delete ALL files and database records except dimensions and API keys. This cannot be undone.'
              : `This will delete files and records older than ${retentionDays} days.`
            }
          </p>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmCleanup(null)}>Cancel</Button>
            <Button
              variant="destructive"
              onClick={() => confirmCleanup && cleanupMutation.mutate(confirmCleanup)}
              disabled={cleanupMutation.isPending}
            >
              {cleanupMutation.isPending ? 'Cleaning...' : 'Confirm'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
