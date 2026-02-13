import { useState, useRef, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Trash2, ChevronRight, ChevronDown, FileJson, List, Save, Check, AlertCircle, Download, Upload } from 'lucide-react'
import { api } from '@/lib/api'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog'
import { ExpressionInput } from '@/components/dimensions/ExpressionInput'
import type { DimensionDef, DimensionStatement } from '@/types'

// --- Operator Builder types ---
interface BuilderCondition {
  key: string
  operator: '==' | 'CONTAINS'
  value: string
}

interface BuilderStatement {
  conditions: BuilderCondition[]
  valueExpression: string
  useRawMode: boolean
  rawMatchExpression: string
}

function conditionsToExpression(conditions: BuilderCondition[]): string {
  return conditions
    .filter(c => c.key && c.value)
    .map(c => `TAG['${c.key}'] ${c.operator} '${c.value}'`)
    .join(' || ')
}

function newCondition(): BuilderCondition {
  return { key: '', operator: '==', value: '' }
}

function newBuilderStatement(): BuilderStatement {
  return {
    conditions: [newCondition()],
    valueExpression: '',
    useRawMode: false,
    rawMatchExpression: '',
  }
}

function builderToStatement(bs: BuilderStatement): DimensionStatement {
  const matchExpression = bs.useRawMode ? bs.rawMatchExpression : conditionsToExpression(bs.conditions)
  return { matchExpression, valueExpression: bs.valueExpression }
}

// Parse a match expression string back into builder conditions.
// Falls back to raw mode if expression can't be parsed into structured conditions.
const TAG_CONDITION_RE = /TAG\['([^']+)'\]\s*(==|CONTAINS)\s*'([^']*)'/gi

function parseMatchExpression(expr: string): { conditions: BuilderCondition[]; canParse: boolean } {
  const conditions: BuilderCondition[] = []
  let match: RegExpExecArray | null
  const re = new RegExp(TAG_CONDITION_RE.source, 'gi')
  while ((match = re.exec(expr)) !== null) {
    conditions.push({ key: match[1], operator: match[2] as '==' | 'CONTAINS', value: match[3] })
  }
  // Verify we captured the full expression by rebuilding and comparing (ignoring whitespace)
  if (conditions.length > 0) {
    const rebuilt = conditions.map(c => `TAG['${c.key}'] ${c.operator} '${c.value}'`).join(' || ')
    const norm = (s: string) => s.replace(/\s+/g, '')
    if (norm(rebuilt) === norm(expr)) {
      return { conditions, canParse: true }
    }
  }
  return { conditions: [newCondition()], canParse: false }
}

function statementToBuilder(stmt: DimensionStatement): BuilderStatement {
  const { conditions, canParse } = parseMatchExpression(stmt.matchExpression)
  return {
    conditions,
    valueExpression: stmt.valueExpression,
    useRawMode: !canParse,
    rawMatchExpression: canParse ? '' : stmt.matchExpression,
  }
}

export function DimensionsPage() {
  const queryClient = useQueryClient()
  const [selectedDim, setSelectedDim] = useState<string | null>(null)
  const [editorMode, setEditorMode] = useState<'form' | 'json'>('form')
  const [isCreating, setIsCreating] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null)
  const [searchQuery, setSearchQuery] = useState('')
  const fileInputRef = useRef<HTMLInputElement>(null)

  // New dimension form state
  const [newName, setNewName] = useState('')
  const [newDefault, setNewDefault] = useState('Unallocated')
  const [builderStatements, setBuilderStatements] = useState<BuilderStatement[]>([])

  // JSON editor state
  const [jsonContent, setJsonContent] = useState('')
  const [validationErrors, setValidationErrors] = useState<string[]>([])
  const [importErrors, setImportErrors] = useState<string[]>([])
  const [expandedStatements, setExpandedStatements] = useState<Set<number>>(new Set())

  const dimensionsQuery = useQuery({
    queryKey: ['dimensions'],
    queryFn: () => api.listDimensions(),
  })

  const contentQuery = useQuery({
    queryKey: ['dimension', selectedDim],
    queryFn: () => api.getDimension(selectedDim!, 1, 500),
    enabled: !!selectedDim,
  })


  const historyQuery = useQuery({
    queryKey: ['dimension-history', selectedDim],
    queryFn: () => api.getDimensionHistory(selectedDim!, 20),
    enabled: !!selectedDim,
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
    setNewDefault('Unallocated')
    setBuilderStatements([])
    setJsonContent('')
    setValidationErrors([])
    setImportErrors([])
  }

  // --- Import/Export ---
  const handleExport = () => {
    if (!contentQuery.data) return
    const data = {
      vtag_name: contentQuery.data.vtag_name,
      defaultValue: contentQuery.data.defaultValue,
      statements: contentQuery.data.statements,
    }
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${data.vtag_name}.json`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }

  const handleImport = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return
    setImportErrors([])
    const reader = new FileReader()
    reader.onload = (e) => {
      try {
        const parsed = JSON.parse(e.target?.result as string)
        if (!parsed.vtag_name && !parsed.name) {
          setImportErrors(['JSON must include "vtag_name" or "name" field'])
          return
        }
        if (!Array.isArray(parsed.statements)) {
          setImportErrors(['JSON must include a "statements" array'])
          return
        }
        const dims = dimensionsQuery.data || []
        createMutation.mutate({
          vtag_name: parsed.vtag_name || parsed.name,
          index: dims.length,
          defaultValue: parsed.defaultValue || 'Unallocated',
          statements: parsed.statements,
        })
      } catch {
        setImportErrors(['Invalid JSON file'])
      }
    }
    reader.readAsText(file)
    // Reset file input so the same file can be re-imported
    event.target.value = ''
  }

  // --- Builder helpers ---
  const addBuilderStatement = () => {
    setBuilderStatements([...builderStatements, newBuilderStatement()])
  }

  const removeBuilderStatement = (index: number) => {
    setBuilderStatements(builderStatements.filter((_, i) => i !== index))
  }

  const updateBuilderCondition = (stmtIdx: number, condIdx: number, field: 'key' | 'operator' | 'value', value: string) => {
    const updated = [...builderStatements]
    const conds = [...updated[stmtIdx].conditions]
    conds[condIdx] = { ...conds[condIdx], [field]: value }
    updated[stmtIdx] = { ...updated[stmtIdx], conditions: conds }
    setBuilderStatements(updated)
  }

  const addCondition = (stmtIdx: number) => {
    const updated = [...builderStatements]
    updated[stmtIdx] = {
      ...updated[stmtIdx],
      conditions: [...updated[stmtIdx].conditions, newCondition()],
    }
    setBuilderStatements(updated)
  }

  const removeCondition = (stmtIdx: number, condIdx: number) => {
    const updated = [...builderStatements]
    updated[stmtIdx] = {
      ...updated[stmtIdx],
      conditions: updated[stmtIdx].conditions.filter((_, i) => i !== condIdx),
    }
    setBuilderStatements(updated)
  }

  const toggleRawMode = (stmtIdx: number) => {
    const updated = [...builderStatements]
    const stmt = updated[stmtIdx]
    if (stmt.useRawMode) {
      // Switching to builder — keep raw text
      updated[stmtIdx] = { ...stmt, useRawMode: false }
    } else {
      // Switching to raw — populate with compiled expression
      updated[stmtIdx] = {
        ...stmt,
        useRawMode: true,
        rawMatchExpression: conditionsToExpression(stmt.conditions),
      }
    }
    setBuilderStatements(updated)
  }

  const updateRawMatch = (stmtIdx: number, value: string) => {
    const updated = [...builderStatements]
    updated[stmtIdx] = { ...updated[stmtIdx], rawMatchExpression: value }
    setBuilderStatements(updated)
  }

  const updateValueExpression = (stmtIdx: number, value: string) => {
    const updated = [...builderStatements]
    updated[stmtIdx] = { ...updated[stmtIdx], valueExpression: value }
    setBuilderStatements(updated)
  }

  // --- Handlers ---
  const handleCreate = () => {
    const dims = dimensionsQuery.data || []
    const nextIndex = dims.length
    if (editorMode === 'json') {
      try {
        const parsed = JSON.parse(jsonContent)
        createMutation.mutate({
          vtag_name: parsed.vtag_name || parsed.name || newName,
          index: nextIndex,
          defaultValue: parsed.defaultValue || newDefault,
          statements: parsed.statements || [],
        })
      } catch {
        setValidationErrors(['Invalid JSON'])
      }
    } else {
      createMutation.mutate({
        vtag_name: newName,
        index: nextIndex,
        defaultValue: newDefault,
        statements: builderStatements.map(builderToStatement),
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
        data: {
          defaultValue: newDefault,
          statements: builderStatements.map(builderToStatement),
        },
      })
    }
  }

  const handleValidate = () => {
    const statements = builderStatements.map(builderToStatement)
    try {
      const content = editorMode === 'json'
        ? JSON.parse(jsonContent)
        : { vtag_name: newName || selectedDim, index: 0, defaultValue: newDefault, statements }
      validateMutation.mutate(content, {
        onSuccess: (result) => {
          setValidationErrors(result.errors || [])
        },
      })
    } catch {
      setValidationErrors(['Invalid JSON syntax'])
    }
  }


  const handleSelectDimension = (dim: DimensionDef) => {
    setSelectedDim(dim.vtag_name)
    setIsCreating(false)
    setValidationErrors([])
    setSearchQuery('')
    setBuilderStatements([]) // reset so useEffect repopulates
  }

  const content = contentQuery.data

  // Populate builder from loaded content
  useEffect(() => {
    if (content && selectedDim && !isCreating) {
      setBuilderStatements(content.statements.map(statementToBuilder))
      setNewDefault(content.defaultValue)
    }
  }, [content, selectedDim, isCreating])

  const dimensions = dimensionsQuery.data || []

  const toggleExpanded = (idx: number) => {
    setExpandedStatements(prev => {
      const next = new Set(prev)
      if (next.has(idx)) next.delete(idx)
      else next.add(idx)
      return next
    })
  }

  // Summary of match expression for collapsed view
  const matchSummary = (stmt: BuilderStatement): string => {
    if (stmt.useRawMode) return stmt.rawMatchExpression || '(empty)'
    const expr = conditionsToExpression(stmt.conditions)
    return expr || '(empty)'
  }

  // --- Render helpers ---
  const filteredIndices = searchQuery
    ? builderStatements.reduce<number[]>((acc, stmt, idx) => {
        const q = searchQuery.toLowerCase()
        const matchExpr = stmt.useRawMode ? stmt.rawMatchExpression : conditionsToExpression(stmt.conditions)
        if (
          stmt.valueExpression.toLowerCase().includes(q) ||
          matchExpr.toLowerCase().includes(q)
        ) acc.push(idx)
        return acc
      }, [])
    : builderStatements.map((_, i) => i)

  const renderStatementEditor = () => (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <label className="text-sm font-medium">
          Statements
          {searchQuery && <span className="text-gray-400 font-normal"> ({filteredIndices.length} of {builderStatements.length})</span>}
        </label>
        <Button size="sm" variant="outline" onClick={() => {
          addBuilderStatement()
          setExpandedStatements(prev => new Set(prev).add(builderStatements.length))
        }}>
          <Plus className="h-3 w-3 mr-1" /> Add
        </Button>
      </div>
      {filteredIndices.map((stmtIdx) => {
        const stmt = builderStatements[stmtIdx]
        const isExpanded = expandedStatements.has(stmtIdx)
        return (
          <div key={stmtIdx} className="border rounded-lg bg-gray-50/50 overflow-hidden">
            {/* Value row — always visible */}
            <div className="flex items-center gap-2 px-3 py-2">
              <span className="text-xs text-gray-400 shrink-0 w-5">{stmtIdx + 1}.</span>
              <ExpressionInput
                value={stmt.valueExpression}
                onChange={(v) => updateValueExpression(stmtIdx, v)}
                mode="value"
                placeholder="'Result Value'"
                className="flex-1"
              />
              <Button size="icon" variant="ghost" onClick={() => removeBuilderStatement(stmtIdx)} className="h-7 w-7 shrink-0 text-gray-400 hover:text-red-500">
                <Trash2 className="h-3.5 w-3.5" />
              </Button>
            </div>

            {/* Expand toggle for match expression */}
            <button
              onClick={() => toggleExpanded(stmtIdx)}
              className="w-full flex items-center gap-2 px-3 py-1.5 text-left bg-gray-100/80 hover:bg-gray-100 transition-colors border-t border-gray-200/60"
            >
              {isExpanded
                ? <ChevronDown className="h-3 w-3 text-gray-400" />
                : <ChevronRight className="h-3 w-3 text-gray-400" />
              }
              <span className="text-[11px] font-medium text-gray-500">Match</span>
              {!isExpanded && (
                <span className="text-[10px] font-mono text-gray-400 truncate">{matchSummary(stmt)}</span>
              )}
            </button>

            {/* Match expression — expanded */}
            {isExpanded && (
              <div className="px-3 py-3 border-t border-gray-200/60 space-y-2">
                <div className="flex items-center justify-end">
                  <button
                    onClick={() => toggleRawMode(stmtIdx)}
                    className="text-[10px] px-2 py-0.5 rounded bg-gray-200 hover:bg-gray-300 text-gray-600 transition-colors"
                  >
                    {stmt.useRawMode ? 'Builder' : 'Raw'}
                  </button>
                </div>
                {stmt.useRawMode ? (
                  <ExpressionInput
                    value={stmt.rawMatchExpression}
                    onChange={(v) => updateRawMatch(stmtIdx, v)}
                    mode="match"
                    placeholder="TAG['key'] == 'value'"
                  />
                ) : (
                  <div className="space-y-2">
                    {stmt.conditions.map((cond, condIdx) => (
                      <div key={condIdx} className="flex gap-1.5 items-center">
                        {condIdx > 0 && (
                          <span className="text-[10px] font-mono text-indigo-500 font-bold px-1">||</span>
                        )}
                        <span className="text-xs font-mono text-gray-500 shrink-0">TAG[</span>
                        <Input
                          value={cond.key}
                          onChange={(e) => updateBuilderCondition(stmtIdx, condIdx, 'key', e.target.value)}
                          placeholder="key"
                          className="flex-1 h-8 text-xs font-mono border-indigo-200 focus-visible:ring-indigo-500/20"
                        />
                        <span className="text-xs font-mono text-gray-500 shrink-0">]</span>
                        <select
                          value={cond.operator}
                          onChange={(e) => updateBuilderCondition(stmtIdx, condIdx, 'operator', e.target.value)}
                          className="h-8 px-2 text-xs font-mono border border-gray-300 rounded-md bg-white focus:ring-2 focus:ring-indigo-500/20 focus:border-indigo-400"
                        >
                          <option value="==">==</option>
                          <option value="CONTAINS">CONTAINS</option>
                        </select>
                        <Input
                          value={cond.value}
                          onChange={(e) => updateBuilderCondition(stmtIdx, condIdx, 'value', e.target.value)}
                          placeholder="value"
                          className="flex-1 h-8 text-xs font-mono border-indigo-200 focus-visible:ring-indigo-500/20"
                        />
                        {stmt.conditions.length > 1 && (
                          <button
                            onClick={() => removeCondition(stmtIdx, condIdx)}
                            className="text-gray-400 hover:text-red-500 transition-colors p-1"
                          >
                            <Trash2 className="h-3 w-3" />
                          </button>
                        )}
                      </div>
                    ))}
                    <button
                      onClick={() => addCondition(stmtIdx)}
                      className="text-xs text-indigo-500 hover:text-indigo-700 flex items-center gap-1 transition-colors"
                    >
                      <Plus className="h-3 w-3" /> Add OR condition
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        )
      })}
      {builderStatements.length === 0 && (
        <div className="text-center py-4 text-sm text-gray-400 border-2 border-dashed rounded-lg">
          No statements yet. Click "Add" to create one.
        </div>
      )}
    </div>
  )

  return (
    <div className="space-y-6">
      {/* Hidden file input for import */}
      <input
        ref={fileInputRef}
        type="file"
        accept=".json"
        onChange={handleImport}
        className="hidden"
      />

      {/* Import error toast */}
      {importErrors.length > 0 && (
        <div className="p-3 bg-red-50 rounded-lg space-y-1">
          {importErrors.map((err, i) => (
            <div key={i} className="flex items-center gap-2 text-sm text-red-700">
              <AlertCircle className="h-4 w-4" /> {err}
            </div>
          ))}
          <button onClick={() => setImportErrors([])} className="text-xs text-red-500 underline">Dismiss</button>
        </div>
      )}

      <div className="grid grid-cols-12 gap-6">
        {/* Left panel: Dimension list */}
        <div className="col-span-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-3">
              <CardTitle className="text-base">Dimensions</CardTitle>
              <div className="flex gap-1.5">
                <Button size="sm" variant="outline" onClick={() => fileInputRef.current?.click()} title="Import JSON">
                  <Upload className="h-4 w-4" />
                </Button>
                <Button size="sm" onClick={() => { setIsCreating(true); setSelectedDim(null); resetForm() }}>
                  <Plus className="h-4 w-4 mr-1" /> New
                </Button>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              {dimensions.length === 0 ? (
                <div className="p-6 text-center text-sm text-gray-500">
                  No dimensions defined yet. Create one to get started.
                </div>
              ) : (
                <div className="divide-y">
                  {(dimensions as DimensionDef[]).sort((a: DimensionDef, b: DimensionDef) => a.index - b.index).map((dim: DimensionDef) => (
                    <button
                      key={dim.vtag_name}
                      onClick={() => handleSelectDimension(dim)}
                      className={`w-full flex items-center justify-between px-4 py-3 text-left hover:bg-gray-50 transition-colors ${
                        selectedDim === dim.vtag_name ? 'bg-primary-50 border-l-2 border-primary-500' : ''
                      }`}
                    >
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
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
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="text-sm font-medium">Name</label>
                        <Input value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="vtag_name" />
                      </div>
                      <div>
                        <label className="text-sm font-medium">Default Value</label>
                        <Input value={newDefault} onChange={(e) => setNewDefault(e.target.value)} />
                      </div>
                    </div>
                    {renderStatementEditor()}
                    {/* Live JSON preview */}
                    <div className="border-t pt-4">
                      <h4 className="text-sm font-medium mb-2">JSON</h4>
                      <pre className="p-3 bg-gray-50 rounded-lg text-xs font-mono overflow-auto max-h-64 text-gray-700 border">
                        {JSON.stringify({
                          vtag_name: newName || 'my_dimension',
                          defaultValue: newDefault,
                          statements: builderStatements.map(builderToStatement),
                        }, null, 2)}
                      </pre>
                    </div>
                  </>
                ) : (
                  <textarea
                    className="w-full h-64 p-3 font-mono text-xs border rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500"
                    value={jsonContent}
                    onChange={(e) => setJsonContent(e.target.value)}
                    placeholder={`{\n  "vtag_name": "my_dimension",\n  "defaultValue": "Unallocated",\n  "statements": [\n    {\n      "matchExpression": "TAG['key'] == 'value'",\n      "valueExpression": "'Result'"\n    }\n  ]\n}`}
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
                    Default: {content.defaultValue} | {content.statement_count} statements
                  </p>
                </div>
                <div className="flex gap-2">
                  <Input
                    placeholder="Filter statements..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="w-48 h-8 text-xs"
                  />
                  <Button size="sm" variant="outline" onClick={handleExport} title="Export JSON">
                    <Download className="h-4 w-4 mr-1" /> Export
                  </Button>
                  <Button size="sm" variant="outline" onClick={handleSave} disabled={updateMutation.isPending}>
                    <Save className="h-4 w-4 mr-1" /> Save
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Default value */}
                <div className="max-w-xs">
                  <label className="text-sm font-medium">Default Value</label>
                  <Input value={newDefault} onChange={(e) => setNewDefault(e.target.value)} />
                </div>

                {/* Editable statements */}
                {renderStatementEditor()}

                {/* Live JSON preview */}
                <div className="border-t pt-4">
                  <h4 className="text-sm font-medium mb-2">JSON</h4>
                  <pre className="p-3 bg-gray-50 rounded-lg text-xs font-mono overflow-auto max-h-64 text-gray-700 border">
                    {JSON.stringify({
                      vtag_name: content.vtag_name,
                      defaultValue: newDefault,
                      statements: builderStatements.map(builderToStatement),
                    }, null, 2)}
                  </pre>
                </div>

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
