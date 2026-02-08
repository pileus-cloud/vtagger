export interface AgentStatus {
  state: string
  step: number
  total_steps: number
  message: string
  sub_progress?: number
  stats?: Record<string, number>
  is_running: boolean
}

export interface ResourceMappingResult {
  resource_id: string
  account_id?: string
  payer_account?: string
  mapping_source?: string
  dimensions: Record<string, string>
  dimension_sources?: Record<string, string>
  tags_extracted?: Record<string, string>
}

export interface SimulationResults {
  completed: boolean
  simulation: boolean
  week_number: number
  year: number
  duration_seconds: number
  total_processed: number
  dimension_matches: number
  unallocated: number
  already_tagged?: number
  samples: ResourceMappingResult[]
  unallocated_samples?: Array<{ resource_id: string; account_id: string }>
  vtag_names: string[]
}

export interface DimensionDef {
  id: number
  vtag_name: string
  index_number: number
  kind: string
  default_value: string
  source: string
  statement_count: number
  checksum: string
  created_at: string
  updated_at: string
}

export interface DimensionStatement {
  matchExpression: string
  valueExpression: string
}

export interface DimensionContent {
  vtag_name: string
  index: number
  kind: string
  defaultValue: string
  source: string
  statement_count: number
  checksum: string
  created_at: string
  updated_at: string
  statements: DimensionStatement[]
  pagination: {
    page: number
    page_size: number
    total_statements: number
    total_pages: number
  }
}

export interface DimensionValidation {
  valid: boolean
  errors: string[]
  warnings: string[]
}

export interface DiscoveredTag {
  tag_key: string
  sample_values: string[]
  first_seen_at: string
  last_seen_at: string
  occurrence_count: number
}

export interface Job {
  id: number
  job_date: string
  status: string
  total_statements: number
  matched_statements: number
  unmatched_statements: number
  dimensions_applied: number
  match_rate: number
  started_at: string | null
  completed_at: string | null
  created_at: string
}

export interface DailyStat {
  stat_date: string
  total_statements: number
  tagged_statements: number
  dimension_matches: number
  unmatched_statements: number
  match_rate: number
  dimension_percentage: number
  api_calls: number
  errors: number
}

export interface StatsSummary {
  start_date: string
  end_date: string
  total_days: number
  total_statements: number
  total_tagged: number
  total_dimension_matches: number
  total_unmatched: number
  avg_match_rate: number
  avg_dimension_percentage: number
  total_api_calls: number
  total_errors: number
}

export interface WeeklyTrend {
  week_start: string
  week_end: string
  total_statements: number
  tagged_statements: number
  dimension_matches: number
  unmatched_statements: number
  match_rate: number
  dimension_percentage: number
}

export interface SyncStatus {
  sync_id: string
  year: number
  month: number
  to_year?: number | null
  to_month?: number | null
  status: string
  total_weeks: number
  weeks_completed: number
  current_week: number | null
  current_phase: string | null
  total_resources: number
  error_message: string | null
  started_at: string
  completed_at: string | null
  weeks: SyncWeek[]
}

export interface SyncWeek {
  id: number
  sync_id: string
  week_number: number
  iso_year: number
  upload_id: string | null
  status: string
  resources_count: number
  uploaded_count: number
  error_message: string | null
  started_at: string | null
  completed_at: string | null
}

export interface VTagUpload {
  id: number
  upload_id: string
  simulation_file: string
  account_id: string
  account_name: string
  status: string
  total_rows: number
  processed_rows: number
  errors: number
  phase: string
  phase_description: string
  error_message: string | null
  started_at: string
  completed_at: string
  created_at: string
}

export interface CleanupPreview {
  files: {
    simulation_files: number
    total_size_mb: number
    details: Array<{
      path: string
      filename: string
      size_bytes: number
      size_mb: number
      modified: number
    }>
  }
  database: Record<string, { count: number; description: string }>
  database_older_than_days: {
    retention_days: number
    records: Record<string, { count: number; description: string }>
  }
}
