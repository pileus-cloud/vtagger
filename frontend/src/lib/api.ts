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
  VTagUpload,
  CleanupPreview,
  SyncStatus,
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

  async createKey(name: string): Promise<{ api_key: string; name: string }> {
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
    return this.fetch('/status')
  }

  subscribeToStatus(onMessage: (status: AgentStatus) => void): EventSource {
    const key = this.getApiKey()
    const url = key ? `${API_BASE}/status/stream?api_key=${key}` : `${API_BASE}/status/stream`
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
  ): Promise<SimulationResults> {
    return this.fetch('/status/simulation', {
      method: 'POST',
      body: JSON.stringify({
        week_number: weekNumber,
        year,
        filter_mode: filterMode,
        max_records: maxRecords,
        vtag_filter_dimensions: vtagFilterDimensions,
      }),
    })
  }

  async getSimulationResults(): Promise<SimulationResults | null> {
    return this.fetch('/status/simulation/results')
  }

  async clearSimulationResults(): Promise<void> {
    await this.fetch('/status/simulation/results', { method: 'DELETE' })
  }

  // === Dimensions ===
  async listDimensions(): Promise<DimensionDef[]> {
    return this.fetch('/dimensions/')
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
    return this.fetch('/dimensions/discovered-tags')
  }

  async resolveTags(tags: Record<string, string>): Promise<{
    dimensions: Record<string, string>
    dimension_sources: Record<string, string>
  }> {
    return this.fetch('/dimensions/resolve', {
      method: 'POST',
      body: JSON.stringify({ tags }),
    })
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
    return this.fetch(`/jobs?limit=${limit}&offset=${offset}`)
  }

  async getJob(jobId: number): Promise<Job> {
    return this.fetch(`/jobs/${jobId}`)
  }

  async getCurrentWeek(): Promise<{ week_number: number; year: number; date: string }> {
    return this.fetch('/jobs/current/week')
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

  // === VTag Upload ===
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

  async startVTagUpload(simulationFile: string): Promise<{
    upload_id: string
    status: string
  }> {
    return this.fetch('/status/vtag-upload/start', {
      method: 'POST',
      body: JSON.stringify({ simulation_file: simulationFile }),
    })
  }

  async getVTagUploadStatus(uploadId: string): Promise<{
    status?: string
    phase?: string
    phaseDescription?: string
    totalRows?: number | null
    processedRows?: number | null
    errors?: number | null
  }> {
    return this.fetch(`/status/vtag-upload/status/${uploadId}`)
  }

  async getVTagUploadHistory(limit = 20): Promise<VTagUpload[]> {
    return this.fetch(`/status/vtag-upload/history?limit=${limit}`)
  }

  // === Sync ===
  async getWeeksForMonth(year: number, month: number): Promise<{
    year: number
    month: number
    weeks: Array<{ week_number: number; iso_year: number; start_date: string; end_date: string }>
  }> {
    return this.fetch(`/status/sync/weeks/${year}/${month}`)
  }

  async getWeeksForRange(fromYear: number, fromMonth: number, toYear: number, toMonth: number): Promise<{
    weeks: Array<{ week_number: number; iso_year: number; start_date: string; end_date: string }>
  }> {
    return this.fetch(`/status/sync/weeks/range?from_year=${fromYear}&from_month=${fromMonth}&to_year=${toYear}&to_month=${toMonth}`)
  }

  async startWeekSync(weekNumber: number, year: number, forceAll = false, vtagFilterDimensions?: string[]): Promise<{
    sync_id: string; status: string
  }> {
    return this.fetch('/status/sync/week', {
      method: 'POST',
      body: JSON.stringify({
        week_number: weekNumber, year, force_all: forceAll,
        vtag_filter_dimensions: vtagFilterDimensions,
      }),
    })
  }

  async startMonthSync(year: number, month: number, forceAll = false, vtagFilterDimensions?: string[]): Promise<{
    sync_id: string; status: string
  }> {
    return this.fetch('/status/sync/month', {
      method: 'POST',
      body: JSON.stringify({
        year, month, force_all: forceAll,
        vtag_filter_dimensions: vtagFilterDimensions,
      }),
    })
  }

  async startRangeSync(fromYear: number, fromMonth: number, toYear: number, toMonth: number, forceAll = false, vtagFilterDimensions?: string[]): Promise<{
    sync_id: string; status: string
  }> {
    return this.fetch('/status/sync/range', {
      method: 'POST',
      body: JSON.stringify({
        from_year: fromYear, from_month: fromMonth,
        to_year: toYear, to_month: toMonth,
        force_all: forceAll,
        vtag_filter_dimensions: vtagFilterDimensions,
      }),
    })
  }

  async getSyncStatus(syncId: string): Promise<SyncStatus> {
    return this.fetch(`/status/sync/status/${syncId}`)
  }

  async getSyncHistory(limit = 20): Promise<Array<{
    sync_id: string; year: number; month: number; status: string
    total_weeks: number; weeks_completed: number; total_resources: number
    started_at: string; completed_at: string | null
  }>> {
    return this.fetch(`/status/sync/history?limit=${limit}`)
  }

  async cancelSync(syncId: string): Promise<{ message: string }> {
    return this.fetch(`/status/sync/cancel/${syncId}`, { method: 'POST' })
  }

  // === Cleanup ===
  async getCleanupPreview(retentionDays = 30): Promise<CleanupPreview> {
    return this.fetch(`/status/cleanup/preview?retention_days=${retentionDays}`)
  }

  async performCleanup(options: {
    deleteFiles?: boolean; cleanDatabase?: boolean; soft?: boolean; retentionDays?: number
  } = {}): Promise<{
    message: string; files_deleted: number; bytes_freed: number
    database_cleaned: Record<string, { deleted: number; error: string | null }>
  }> {
    const { deleteFiles = true, cleanDatabase = true, soft = false, retentionDays = 30 } = options
    return this.fetch('/status/cleanup', {
      method: 'POST',
      body: JSON.stringify({ delete_files: deleteFiles, clean_database: cleanDatabase, soft, retention_days: retentionDays }),
    })
  }

  async deleteSingleFile(filename: string): Promise<{ message: string; bytes_freed: number }> {
    return this.fetch(`/status/cleanup/file/${encodeURIComponent(filename)}`, { method: 'DELETE' })
  }

  getUploadedCsvDownloadUrl(uploadId: string): string {
    const key = this.getApiKey()
    const baseUrl = `${API_BASE}/status/vtag-upload/download/${encodeURIComponent(uploadId)}`
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
