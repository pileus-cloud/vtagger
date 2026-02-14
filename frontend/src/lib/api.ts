import type {
  AgentStatus,
  SimulationResults,
  DimensionDef,
  DimensionContent,
  DimensionValidation,
  DiscoveredTag,
  Job,
  DailyStat,
  StatsSummary,
  WeeklyTrend,
  CleanupPreview,
  SyncStatus,
  SyncProgressResult,
} from '@/types'

const API_BASE = '/api'

class ApiClient {
  private apiKey: string | null = null

  setApiKey(key: string) {
    this.apiKey = key
    localStorage.setItem('vtagger_api_key', key)
  }

  getApiKey(): string | null {
    if (!this.apiKey) {
      this.apiKey = localStorage.getItem('vtagger_api_key')
    }
    return this.apiKey
  }

  clearApiKey() {
    this.apiKey = null
    localStorage.removeItem('vtagger_api_key')
  }

  private async fetch<T>(path: string, options: RequestInit = {}): Promise<T> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...(options.headers as Record<string, string>),
    }

    const key = this.getApiKey()
    if (key) {
      headers['X-API-Key'] = key
    }

    const response = await fetch(`${API_BASE}${path}`, {
      ...options,
      headers,
    })

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Request failed' }))
      throw new Error(error.detail || `HTTP ${response.status}`)
    }

    return response.json()
  }

  // === Auth ===
  async validateKey(): Promise<{ valid: boolean }> {
    return this.fetch('/auth/validate')
  }

  async createKey(name: string): Promise<{ key: string; name: string }> {
    return this.fetch('/auth/keys', {
      method: 'POST',
      body: JSON.stringify({ name }),
    })
  }

  async checkKeyExists(): Promise<{ exists: boolean }> {
    return this.fetch('/auth/key-exists')
  }

  // === Status ===
  async getStatus(): Promise<AgentStatus> {
    return this.fetch('/status/progress')
  }

  subscribeToStatus(onMessage: (status: AgentStatus) => void): EventSource {
    const key = this.getApiKey()
    const url = key ? `${API_BASE}/status/events?api_key=${key}` : `${API_BASE}/status/events`
    const eventSource = new EventSource(url)
    eventSource.onmessage = (event) => {
      const status = JSON.parse(event.data)
      onMessage(status)
    }
    return eventSource
  }

  async forceReset(): Promise<{ message: string; state: string }> {
    return this.fetch('/status/reset', { method: 'POST' })
  }

  // === Simulation ===
  async runSimulation(
    weekNumber: number,
    year: number,
    filterMode: string = 'all',
    maxRecords: number = 1000,
    vtagFilterDimensions?: string[],
  ): Promise<{ status: string; message: string }> {
    // Convert week/year to date range
    const startDate = this.weekToDateRange(weekNumber, year)
    return this.fetch('/status/simulate', {
      method: 'POST',
      body: JSON.stringify({
        account_key: '0',
        start_date: startDate.start,
        end_date: startDate.end,
        vtag_filter_dimensions: vtagFilterDimensions,
        filter_mode: filterMode,
        max_records: maxRecords,
      }),
    })
  }

  async getSimulationResults(): Promise<SimulationResults | null> {
    try {
      return await this.fetch('/status/simulate/results')
    } catch {
      return null
    }
  }

  async clearSimulationResults(): Promise<void> {
    // No backend endpoint for this - noop
  }

  // === Dimensions ===
  async listDimensions(): Promise<DimensionDef[]> {
    const result = await this.fetch<{ dimensions: DimensionDef[]; count: number }>('/dimensions/')
    return result.dimensions
  }

  async createDimension(content: {
    vtag_name: string
    index: number
    kind?: string
    defaultValue?: string
    source?: string
    statements: Array<{ matchExpression: string; valueExpression: string }>
  }): Promise<{ vtag_name: string; index: number; statement_count: number; checksum: string }> {
    return this.fetch('/dimensions/', {
      method: 'POST',
      body: JSON.stringify(content),
    })
  }

  async getDimension(name: string, page = 1, pageSize = 50): Promise<DimensionContent> {
    return this.fetch(`/dimensions/${encodeURIComponent(name)}?page=${page}&page_size=${pageSize}`)
  }

  async updateDimension(name: string, content: {
    index?: number
    kind?: string
    defaultValue?: string
    statements?: Array<{ matchExpression: string; valueExpression: string }>
  }): Promise<{ vtag_name: string; index: number; statement_count: number; checksum: string }> {
    return this.fetch(`/dimensions/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify(content),
    })
  }

  async deleteDimension(name: string): Promise<{ deleted: string }> {
    return this.fetch(`/dimensions/${encodeURIComponent(name)}`, { method: 'DELETE' })
  }

  async validateDimension(content: Record<string, unknown>): Promise<DimensionValidation> {
    return this.fetch('/dimensions/validate', {
      method: 'POST',
      body: JSON.stringify(content),
    })
  }

  async searchDimension(name: string, query: string, page = 1, pageSize = 50): Promise<{
    vtag_name: string
    query: string
    statements: Array<{ matchExpression: string; valueExpression: string }>
    pagination: { page: number; page_size: number; total_statements: number; total_pages: number }
  }> {
    return this.fetch(`/dimensions/${encodeURIComponent(name)}/search?q=${encodeURIComponent(query)}&page=${page}&page_size=${pageSize}`)
  }

  async getDiscoveredTags(): Promise<{ tags: DiscoveredTag[]; count: number }> {
    const result = await this.fetch<{ discovered_tags: DiscoveredTag[]; count: number }>('/dimensions/discovered-tags')
    return { tags: result.discovered_tags, count: result.count }
  }

  async resolveTags(tags: Record<string, string>): Promise<{
    dimensions: Record<string, string>
    dimension_sources: Record<string, string>
  }> {
    const result = await this.fetch<{ resolved: Record<string, string> }>('/dimensions/resolve', {
      method: 'POST',
      body: JSON.stringify({ tags }),
    })
    return { dimensions: result.resolved, dimension_sources: {} }
  }

  async getDimensionHistory(name: string, limit = 50): Promise<{
    history: Array<{
      id: number
      vtag_name: string
      action: string
      previous_content: string | null
      new_content: string | null
      source: string | null
      created_at: string
    }>
    count: number
  }> {
    return this.fetch(`/dimensions/${encodeURIComponent(name)}/history?limit=${limit}`)
  }

  // === Jobs ===
  async listJobs(limit = 20, offset = 0): Promise<{ jobs: Job[]; count: number }> {
    const page = Math.floor(offset / limit) + 1
    const result = await this.fetch<{ jobs: Job[]; count: number; total: number; page: number; page_size: number; total_pages: number }>(
      `/jobs/?page=${page}&page_size=${limit}`
    )
    return { jobs: result.jobs, count: result.total }
  }

  async getJob(jobId: number): Promise<Job> {
    return this.fetch(`/jobs/${jobId}`)
  }

  async getCurrentWeek(): Promise<{ week_number: number; year: number; date: string }> {
    const now = new Date()
    const start = new Date(now.getFullYear(), 0, 1)
    const diff = now.getTime() - start.getTime()
    const weekNumber = Math.ceil(diff / (7 * 24 * 60 * 60 * 1000))
    return { week_number: weekNumber, year: now.getFullYear(), date: now.toISOString().split('T')[0] }
  }

  // === Stats ===
  async getDailyStats(days = 30): Promise<{ daily_stats: DailyStat[]; count: number }> {
    return this.fetch(`/stats/daily?limit=${days}`)
  }

  async getStatsSummary(days = 30): Promise<StatsSummary> {
    return this.fetch(`/stats/summary?days=${days}`)
  }

  async getWeeklyTrend(weeks = 12): Promise<{ weekly_trends: WeeklyTrend[]; count: number }> {
    return this.fetch(`/stats/weekly-trends?weeks=${weeks}`)
  }

  async getMappingBreakdown(days = 30): Promise<{
    breakdown: Array<{ source: string; count: number; percentage: number }>
    total: number
    start_date: string
    end_date: string
  }> {
    return this.fetch(`/stats/mapping-breakdown?days=${days}`)
  }

  async getRecentActivity(limit = 10): Promise<{
    recent_activity: Array<{
      job_id: number
      date: string
      status: string
      total_statements: number
      matched_statements: number
      unmatched_statements: number
      dimensions_applied: number
      match_rate: number
      started_at: string | null
      completed_at: string | null
    }>
    count: number
  }> {
    return this.fetch(`/stats/recent?limit=${limit}`)
  }

  // === Output Files ===
  async listSimulationFiles(): Promise<{
    files: Array<{
      name: string
      path: string
      size_bytes: number
      size_mb: number
      modified_at: string
    }>
    count: number
  }> {
    return this.fetch('/status/files')
  }

  // === VTag Upload ===
  async startVTagUpload(jsonlFile: string): Promise<{
    status: string
    message: string
    file: string
  }> {
    return this.fetch('/status/upload', {
      method: 'POST',
      body: JSON.stringify({
        account_key: '0',
        jsonl_file: jsonlFile,
      }),
    })
  }

  async getVTagUploadStatus(uploadId: string): Promise<Record<string, unknown>> {
    return this.fetch(`/status/uploads/${uploadId}`)
  }

  async getVTagUploadHistory(limit = 20): Promise<{ uploads: Record<string, unknown>[]; count: number }> {
    return this.fetch(`/status/uploads?limit=${limit}`)
  }

  // === Accounts ===
  async getAccounts(): Promise<{
    accounts: Array<{
      accountKey: number
      accountId: string
      accountName: string
      cloudType: string
      cloudTypeId: number
    }>
    count: number
  }> {
    return this.fetch('/auth/accounts')
  }

  // === Sync ===
  async startWeekSync(weekNumber: number, year: number, filterMode = 'not_vtagged', vtagFilterDimensions?: string[], accountKeys?: string[]): Promise<{
    status: string; message: string
  }> {
    const dateRange = this.weekToDateRange(weekNumber, year)
    return this.fetch('/status/sync/week', {
      method: 'POST',
      body: JSON.stringify({
        account_key: '0',
        account_keys: accountKeys && accountKeys.length > 0 ? accountKeys : undefined,
        start_date: dateRange.start,
        end_date: dateRange.end,
        vtag_filter_dimensions: vtagFilterDimensions,
        filter_mode: filterMode,
      }),
    })
  }

  async startMonthSync(year: number, month: number, filterMode = 'not_vtagged', vtagFilterDimensions?: string[], accountKeys?: string[]): Promise<{
    status: string; message: string
  }> {
    const monthStr = `${year}-${String(month).padStart(2, '0')}`
    return this.fetch('/status/sync/month', {
      method: 'POST',
      body: JSON.stringify({
        account_key: '0',
        account_keys: accountKeys && accountKeys.length > 0 ? accountKeys : undefined,
        month: monthStr,
        vtag_filter_dimensions: vtagFilterDimensions,
        filter_mode: filterMode,
      }),
    })
  }

  async startRangeSync(fromYear: number, fromMonth: number, toYear: number, toMonth: number, filterMode = 'not_vtagged', vtagFilterDimensions?: string[], accountKeys?: string[]): Promise<{
    status: string; message: string
  }> {
    const startDate = `${fromYear}-${String(fromMonth).padStart(2, '0')}-01`
    const lastDay = new Date(toYear, toMonth, 0).getDate()
    const endDate = `${toYear}-${String(toMonth).padStart(2, '0')}-${String(lastDay).padStart(2, '0')}`
    return this.fetch('/status/sync/range', {
      method: 'POST',
      body: JSON.stringify({
        account_key: '0',
        account_keys: accountKeys && accountKeys.length > 0 ? accountKeys : undefined,
        start_date: startDate,
        end_date: endDate,
        vtag_filter_dimensions: vtagFilterDimensions,
        filter_mode: filterMode,
      }),
    })
  }

  async getSyncProgress(): Promise<SyncProgressResult> {
    const result = await this.fetch<SyncProgressResult>('/status/sync/progress')
    console.log(`[API] getSyncProgress -> status=${result.status}, phase=${result.phase}`)
    return result
  }

  async getSyncStatus(syncId: string): Promise<SyncStatus> {
    return this.fetch(`/status/sync/months/${syncId}`)
  }

  async getSyncHistory(limit = 20): Promise<Array<{
    sync_id: string; year: number; month: number; status: string
    total_weeks: number; weeks_completed: number; total_resources: number
    started_at: string; completed_at: string | null
  }>> {
    const result = await this.fetch<{ syncs: Array<Record<string, unknown>>; count: number }>(`/status/sync/months?limit=${limit}`)
    return result.syncs as Array<{
      sync_id: string; year: number; month: number; status: string
      total_weeks: number; weeks_completed: number; total_resources: number
      started_at: string; completed_at: string | null
    }>
  }

  async cancelSync(): Promise<{ status: string; message: string }> {
    return this.fetch('/status/sync/cancel', { method: 'POST' })
  }

  async getLastSyncResult(): Promise<Record<string, unknown>> {
    return this.fetch('/status/sync/last-result')
  }

  async getImportStatus(): Promise<{
    status?: string
    last_result?: Record<string, unknown>
    import_statuses?: Array<{
      upload_id: string
      account_id?: string
      timestamp?: string
      sync_type?: string
      start_date?: string
      end_date?: string
      phase: string
      phase_description?: string
      total_rows?: number
      processed_rows?: number
      errors?: number
      error?: string
      status?: string
      import_mode?: string
      inserted?: number
      updated?: number
      deleted?: number
    }>
  }> {
    return this.fetch('/status/sync/import-status')
  }

  // === Cleanup ===
  async getCleanupPreview(_retentionDays = 30): Promise<CleanupPreview> {
    return this.fetch('/status/cleanup/stats')
  }

  async performCleanup(options: {
    deleteFiles?: boolean; cleanDatabase?: boolean; soft?: boolean; retentionDays?: number
  } = {}): Promise<Record<string, unknown>> {
    const { soft = false, retentionDays = 30 } = options
    const cleanupType = soft ? 'soft' : 'hard'
    return this.fetch('/status/cleanup', {
      method: 'POST',
      body: JSON.stringify({
        cleanup_type: cleanupType,
        older_than_days: soft ? retentionDays : null,
      }),
    })
  }

  async deleteSingleFile(_filename: string): Promise<{ message: string }> {
    // Delete via cleanup endpoint - not directly supported
    throw new Error('Single file deletion not supported. Use cleanup instead.')
  }

  // === Helpers ===

  private weekToDateRange(weekNumber: number, year: number): { start: string; end: string } {
    // ISO week to date range
    const jan4 = new Date(year, 0, 4)
    const dayOfWeek = jan4.getDay() || 7
    const mondayWeek1 = new Date(jan4)
    mondayWeek1.setDate(jan4.getDate() - dayOfWeek + 1)

    const startDate = new Date(mondayWeek1)
    startDate.setDate(mondayWeek1.getDate() + (weekNumber - 1) * 7)
    const endDate = new Date(startDate)
    endDate.setDate(startDate.getDate() + 6)

    const fmt = (d: Date) => d.toISOString().split('T')[0]
    return { start: fmt(startDate), end: fmt(endDate) }
  }

  getUploadedCsvDownloadUrl(uploadId: string): string {
    const key = this.getApiKey()
    const baseUrl = `${API_BASE}/status/files/${encodeURIComponent(uploadId)}`
    return key ? `${baseUrl}?api_key=${key}` : baseUrl
  }

  getUntaggedSamplesDownloadUrl(week?: number, year?: number): string {
    const key = this.getApiKey()
    const params = new URLSearchParams()
    if (key) params.set('api_key', key)
    if (week !== undefined) params.set('week', String(week))
    if (year !== undefined) params.set('year', String(year))
    const qs = params.toString()
    return `${API_BASE}/stats/untagged-samples/download${qs ? `?${qs}` : ''}`
  }
}

export const api = new ApiClient()
