export interface AgentStatus {
  state: string
  progress: number
  message: string
  detail: string
  sub_progress: number
  sub_message: string
  step: number
  total_steps: number
  elapsed_seconds: number | null
  error: string | null
  stats: Record<string, number>
}

export interface SimulationResults {
  status: string
  start_date: string
  end_date: string
  total_assets: number
  matched_assets: number
  unmatched_assets: number
  dimension_matches: number
  match_rate: number
  vtag_names: string[]
  dimension_details: Record<string, number>
  sample_records: Array<{
    resourceid: string
    linkedaccid: string
    payeraccount: string
    dimensions: Record<string, string>
    tags: Record<string, string>
  }>
  elapsed_seconds: number
  error_message: string
  output_file: string
}

export interface DimensionDef {
  vtag_name: string
  index: number
  kind: string
  defaultValue: string
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
  tables: Record<string, number>
  output_dir: {
    path: string
    file_count: number
    total_size_bytes: number
    total_size_mb: number
  }
}
