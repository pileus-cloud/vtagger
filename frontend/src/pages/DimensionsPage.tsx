import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Trash2, Search, ChevronRight, FileJson, List, Save, Check, AlertCircle, Tag } from 'lucide-react'
import { api } from '@/lib/api'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import type { DimensionDef, DimensionStatement } from '@/types'

export function DimensionsPage() {
  const queryClient = useQueryClient()
  const [selectedDim, setSelectedDim] = useState<string | null>(null)
  const [editorMode, setEditorMode] = useState<'form' | 'json'>('form')
  const [isCreating, setIsCreating] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null)
  const [searchQuery, setSearchQuery] = useState('')

  // New dimension form state
  const [newName, setNewName] = useState('')
  const [newIndex, setNewIndex] = useState(0)
  const [newDefault, setNewDefault] = useState('Unallocated')
  const [newStatements, setNewStatements] = useState<DimensionStatement[]>([])

  // JSON editor state
  const [jsonContent, setJsonContent] = useState('')
  const [validationErrors, setValidationErrors] = useState<string[]>([])

  const dimensionsQuery = useQuery({
    queryKey: ['dimensions'],
    queryFn: () => api.listDimensions(),
  })

  const contentQuery = useQuery({
    queryKey: ['dimension', selectedDim],
    queryFn: () => api.getDimension(selectedDim!, 1, 500),
    enabled: !!selectedDim,
  })

  const discoveredTagsQuery = useQuery({
    queryKey: ['discovered-tags'],
    queryFn: () => api.getDiscoveredTags(),
  })

  const historyQuery = useQuery({
    queryKey: ['dimension-history', selectedDim],
    queryFn: () => api.getDimensionHistory(selectedDim!, 20),
    enabled: !!selectedDim,
  })

  const searchMutation = useMutation({
    mutationFn: ({ name, query }: { name: string; query: string }) =>
      api.searchDimension(name, query),
  })

  const createMutation = useMutation({
    mutationFn: (data: {
      vtag_name: string; index: number; defaultValue: string;
      statements: DimensionStatement[]
    }) => api.createDimension(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dimensions'] })
      setIsCreating(false)
      resetForm()
    },
  })

  const updateMutation = useMutation({
    mutationFn: ({ name, data }: { name: string; data: Record<string, unknown> }) =>
      api.updateDimension(name, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dimensions'] })
      queryClient.invalidateQueries({ queryKey: ['dimension', selectedDim] })
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (name: string) => api.deleteDimension(name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dimensions'] })
      if (selectedDim === deleteTarget) setSelectedDim(null)
      setDeleteTarget(null)
    },
  })

  const validateMutation = useMutation({
    mutationFn: (content: Record<string, unknown>) => api.validateDimension(content),
  })

  const resetForm = () => {
    setNewName('')
    setNewIndex(0)
    setNewDefault('Unallocated')
    setNewStatements([])
    setJsonContent('')
    setValidationErrors([])
  }

  const handleCreate = () => {
    if (editorMode === 'json') {
      try {
        const parsed = JSON.parse(jsonContent)
        createMutation.mutate({
          vtag_name: parsed.vtag_name || parsed.name || newName,
          index: parsed.index ?? newIndex,
          defaultValue: parsed.defaultValue || newDefault,
          statements: parsed.statements || [],
        })
      } catch {
        setValidationErrors(['Invalid JSON'])
      }
    } else {
      createMutation.mutate({
        vtag_name: newName,
        index: newIndex,
        defaultValue: newDefault,
        statements: newStatements,
      })
    }
  }

  const handleSave = () => {
    if (!selectedDim || !contentQuery.data) return
    if (editorMode === 'json') {
      try {
        const parsed = JSON.parse(jsonContent)
        updateMutation.mutate({
          name: selectedDim,
          data: {
            index: parsed.index,
            defaultValue: parsed.defaultValue,
            statements: parsed.statements,
          },
        })
      } catch {
        setValidationErrors(['Invalid JSON'])
      }
    } else {
      updateMutation.mutate({
        name: selectedDim,
        data: { statements: newStatements },
      })
    }
  }

  const handleValidate = () => {
    try {
      const content = editorMode === 'json'
        ? JSON.parse(jsonContent)
        : { vtag_name: newName || selectedDim, statements: newStatements }
      validateMutation.mutate(content, {
        onSuccess: (result) => {
          setValidationErrors(result.errors || [])
        },
      })
    } catch {
      setValidationErrors(['Invalid JSON syntax'])
    }
  }

  const handleSearch = () => {
    if (selectedDim && searchQuery) {
      searchMutation.mutate({ name: selectedDim, query: searchQuery })
    }
  }

  const handleSelectDimension = (dim: DimensionDef) => {
    setSelectedDim(dim.vtag_name)
    setIsCreating(false)
    setValidationErrors([])
    setSearchQuery('')
  }

  // When content loads, populate editor
  const content = contentQuery.data
  if (content && selectedDim && editorMode === 'form' && newStatements.length === 0) {
    // Only set on first load
  }

  const addStatement = () => {
    setNewStatements([...newStatements, { matchExpression: '', valueExpression: '' }])
  }

  const removeStatement = (index: number) => {
    setNewStatements(newStatements.filter((_, i) => i !== index))
  }

  const updateStatement = (index: number, field: keyof DimensionStatement, value: string) => {
    const updated = [...newStatements]
    updated[index] = { ...updated[index], [field]: value }
    setNewStatements(updated)
  }

  const dimensions = dimensionsQuery.data || []
  const discoveredTags = discoveredTagsQuery.data?.tags || []

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-12 gap-6">
        {/* Left panel: Dimension list */}
        <div className="col-span-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-3">
              <CardTitle className="text-base">Dimensions</CardTitle>
              <Button size="sm" onClick={() => { setIsCreating(true); setSelectedDim(null); resetForm() }}>
                <Plus className="h-4 w-4 mr-1" /> New
              </Button>
            </CardHeader>
            <CardContent className="p-0">
              {dimensions.length === 0 ? (
                <div className="p-6 text-center text-sm text-gray-500">
                  No dimensions defined yet. Create one to get started.
                </div>
              ) : (
                <div className="divide-y">
                  {(dimensions as DimensionDef[]).sort((a: DimensionDef, b: DimensionDef) => a.index_number - b.index_number).map((dim: DimensionDef) => (
                    <button
                      key={dim.vtag_name}
                      onClick={() => handleSelectDimension(dim)}
                      className={`w-full flex items-center justify-between px-4 py-3 text-left hover:bg-gray-50 transition-colors ${
                        selectedDim === dim.vtag_name ? 'bg-primary-50 border-l-2 border-primary-500' : ''
                      }`}
                    >
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <Badge variant="secondary" className="text-xs">{dim.index_number}</Badge>
                          <span className="font-medium text-sm truncate">{dim.vtag_name}</span>
                        </div>
                        <div className="text-xs text-gray-400 mt-0.5">
                          {dim.statement_count} statements
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <button
                          onClick={(e) => { e.stopPropagation(); setDeleteTarget(dim.vtag_name) }}
                          className="text-gray-400 hover:text-red-500 transition-colors"
                        >
                          <Trash2 className="h-4 w-4" />
                        </button>
                        <ChevronRight className="h-4 w-4 text-gray-300" />
                      </div>
                    </button>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Right panel: Editor */}
        <div className="col-span-8">
          {isCreating ? (
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-3">
                <CardTitle className="text-base">Create Dimension</CardTitle>
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    variant={editorMode === 'form' ? 'default' : 'outline'}
                    onClick={() => setEditorMode('form')}
                  >
                    <List className="h-4 w-4 mr-1" /> Form
                  </Button>
                  <Button
                    size="sm"
                    variant={editorMode === 'json' ? 'default' : 'outline'}
                    onClick={() => setEditorMode('json')}
                  >
                    <FileJson className="h-4 w-4 mr-1" /> JSON
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                {editorMode === 'form' ? (
                  <>
                    <div className="grid grid-cols-3 gap-4">
                      <div>
                        <label className="text-sm font-medium">Name</label>
                        <Input value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="vtag_name" />
                      </div>
                      <div>
                        <label className="text-sm font-medium">Index</label>
                        <Input type="number" value={newIndex} onChange={(e) => setNewIndex(Number(e.target.value))} />
                      </div>
                      <div>
                        <label className="text-sm font-medium">Default Value</label>
                        <Input value={newDefault} onChange={(e) => setNewDefault(e.target.value)} />
                      </div>
                    </div>
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <label className="text-sm font-medium">Statements</label>
                        <Button size="sm" variant="outline" onClick={addStatement}>
                          <Plus className="h-3 w-3 mr-1" /> Add
                        </Button>
                      </div>
                      {newStatements.map((stmt, i) => (
                        <div key={i} className="flex gap-2 items-start">
                          <div className="flex-1">
                            <Input
                              placeholder="TAG['key'] == 'value'"
                              value={stmt.matchExpression}
                              onChange={(e) => updateStatement(i, 'matchExpression', e.target.value)}
                              className="text-xs font-mono"
                            />
                          </div>
                          <div className="flex-1">
                            <Input
                              placeholder="'Result Value'"
                              value={stmt.valueExpression}
                              onChange={(e) => updateStatement(i, 'valueExpression', e.target.value)}
                              className="text-xs font-mono"
                            />
                          </div>
                          <Button size="icon" variant="ghost" onClick={() => removeStatement(i)} className="text-gray-400 hover:text-red-500">
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </div>
                      ))}
                    </div>
                  </>
                ) : (
                  <textarea
                    className="w-full h-64 p-3 font-mono text-xs border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500"
                    value={jsonContent}
                    onChange={(e) => setJsonContent(e.target.value)}
                    placeholder={`{\n  "vtag_name": "my_dimension",\n  "index": 0,\n  "defaultValue": "Unallocated",\n  "statements": [\n    {\n      "matchExpression": "TAG['key'] == 'value'",\n      "valueExpression": "'Result'"\n    }\n  ]\n}`}
                  />
                )}

                {validationErrors.length > 0 && (
                  <div className="p-3 bg-red-50 rounded-lg space-y-1">
                    {validationErrors.map((err, i) => (
                      <div key={i} className="flex items-center gap-2 text-sm text-red-700">
                        <AlertCircle className="h-4 w-4" /> {err}
                      </div>
                    ))}
                  </div>
                )}

                <div className="flex gap-2 justify-end">
                  <Button variant="outline" onClick={handleValidate}>
                    <Check className="h-4 w-4 mr-1" /> Validate
                  </Button>
                  <Button onClick={handleCreate} disabled={createMutation.isPending}>
                    <Save className="h-4 w-4 mr-1" />
                    {createMutation.isPending ? 'Creating...' : 'Create'}
                  </Button>
                </div>
              </CardContent>
            </Card>
          ) : selectedDim && content ? (
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-3">
                <div>
                  <CardTitle className="text-base">{content.vtag_name}</CardTitle>
                  <p className="text-xs text-gray-400 mt-1">
                    Index: {content.index} | Default: {content.defaultValue} | {content.statement_count} statements
                  </p>
                </div>
                <div className="flex gap-2">
                  <div className="flex items-center gap-1">
                    <Input
                      placeholder="Search statements..."
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      className="w-48 h-8 text-xs"
                      onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
                    />
                    <Button size="sm" variant="outline" onClick={handleSearch}>
                      <Search className="h-3 w-3" />
                    </Button>
                  </div>
                  <Button size="sm" variant="outline" onClick={handleSave} disabled={updateMutation.isPending}>
                    <Save className="h-4 w-4 mr-1" /> Save
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                <div className="max-h-96 overflow-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-12">#</TableHead>
                        <TableHead>Match Expression</TableHead>
                        <TableHead>Value Expression</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {(searchMutation.data?.statements || content.statements).map((stmt, i) => (
                        <TableRow key={i}>
                          <TableCell className="text-gray-400 text-xs">{i + 1}</TableCell>
                          <TableCell className="font-mono text-xs">{stmt.matchExpression}</TableCell>
                          <TableCell className="font-mono text-xs">{stmt.valueExpression}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
                {content.pagination.total_pages > 1 && (
                  <div className="mt-3 text-xs text-gray-400 text-center">
                    Page {content.pagination.page} of {content.pagination.total_pages} ({content.pagination.total_statements} total)
                  </div>
                )}

                {/* History */}
                {historyQuery.data && historyQuery.data.history.length > 0 && (
                  <div className="mt-6 border-t pt-4">
                    <h4 className="text-sm font-medium mb-2">Recent Changes</h4>
                    <div className="space-y-2 max-h-32 overflow-auto">
                      {historyQuery.data.history.slice(0, 5).map((entry) => (
                        <div key={entry.id} className="flex items-center gap-2 text-xs text-gray-500">
                          <Badge variant={entry.action === 'created' ? 'success' : entry.action === 'deleted' ? 'error' : 'secondary'} className="text-[10px]">
                            {entry.action}
                          </Badge>
                          <span>{new Date(entry.created_at).toLocaleString()}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardContent className="flex items-center justify-center h-64 text-gray-400">
                Select a dimension or create a new one
              </CardContent>
            </Card>
          )}
        </div>
      </div>

      {/* Discovered Tags Panel */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2">
              <Tag className="h-4 w-4" /> Discovered Tags
            </CardTitle>
            <Button size="sm" variant="outline" onClick={() => discoveredTagsQuery.refetch()}>
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {discoveredTags.length === 0 ? (
            <p className="text-sm text-gray-500">
              No tags discovered yet. Run a simulation to discover available tag keys.
            </p>
          ) : (
            <div className="max-h-48 overflow-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Tag Key</TableHead>
                    <TableHead>Sample Values</TableHead>
                    <TableHead>Occurrences</TableHead>
                    <TableHead>Last Seen</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {discoveredTags.map((tag) => (
                    <TableRow key={tag.tag_key}>
                      <TableCell className="font-mono text-xs font-medium">{tag.tag_key}</TableCell>
                      <TableCell className="text-xs text-gray-500">
                        {(tag.sample_values || []).slice(0, 3).join(', ')}
                      </TableCell>
                      <TableCell className="text-xs">{tag.occurrence_count}</TableCell>
                      <TableCell className="text-xs text-gray-400">
                        {new Date(tag.last_seen_at).toLocaleDateString()}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Delete confirmation dialog */}
      <Dialog open={!!deleteTarget} onOpenChange={() => setDeleteTarget(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Dimension</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-gray-600">
            Are you sure you want to delete <strong>{deleteTarget}</strong>? This action cannot be undone.
          </p>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteTarget(null)}>Cancel</Button>
            <Button variant="destructive" onClick={() => deleteTarget && deleteMutation.mutate(deleteTarget)} disabled={deleteMutation.isPending}>
              {deleteMutation.isPending ? 'Deleting...' : 'Delete'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
