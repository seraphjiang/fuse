// SPDX-License-Identifier: Apache-2.0

export interface QueryRequest {
  query: string;
  format: 'sql' | 'ppl';
  analyze?: boolean;
  timeout_ms?: number;
  page_size?: number;
  cursor?: string;
}

export interface DatasourceStat {
  rows: number;
  latency_ms: number;
}

export interface ProfileNode {
  op: string;
  datasource?: string;
  actual_rows: number;
  actual_ms: number;
  data_bytes?: number;
  pushdown: string[];
  children: ProfileNode[];
}

export interface ExecutionProfile {
  total_ms: number;
  nodes: ProfileNode[];
}

export interface QueryMetadata {
  total_rows: number;
  format: string;
  trace_id?: string;
  datasources_queried?: string[];
  datasource_stats?: Record<string, DatasourceStat>;
  execution_profile?: ExecutionProfile;
  next_cursor?: string;
}

export interface QueryResponse {
  columns: string[];
  rows: unknown[][];
  metadata: QueryMetadata;
}

export interface DatasourceInfo {
  id: string;
  connector_type: string;
  capabilities: ConnectorCapabilities;
}

export interface ConnectorCapabilities {
  supports_filtering: boolean;
  supports_projection: boolean;
  supports_aggregation: boolean;
  supports_sorting: boolean;
  supports_limit: boolean;
  supports_join: boolean;
  supports_streaming: boolean;
  max_concurrent_queries: number;
  latency_class: 'low' | 'medium' | 'high';
}

export interface HealthResponse {
  status: 'ok' | 'degraded' | 'error';
  connectors: Record<string, { status: string; latency_ms?: number; message?: string }>;
}

export interface ValidateResponse {
  valid: boolean;
  error?: string;
}

export interface PlanNode {
  op: string;
  detail?: string;
  estimated_rows?: number;
  estimated_cost?: number;
  children: PlanNode[];
}

export interface ExplainResponse {
  plan: string;
  plan_tree?: PlanNode;
}

export interface HistoryEntry {
  query: string;
  format: string;
  timestamp: number;
  latency_ms: number;
  row_count: number;
  error?: string;
}

export interface TraceSpan {
  datasource: string;
  timestamp?: string | null;
  fields: Record<string, unknown>;
}

export interface TraceResponse {
  trace_id: string;
  spans: TraceSpan[];
  datasources_searched: string[];
  datasources_matched: string[];
  total_spans: number;
  search_ms: number;
}

export const PLUGIN_ID = 'fuseQuery';
export const PLUGIN_NAME = 'Fuse Query';
export const API_BASE = '/api/fuse_query';

// === Federation types (#1001) ===

export interface FederatedInstance {
  id: string;
  url: string;
  name?: string;
  datasources: string[];
  status: 'healthy' | 'degraded' | 'unhealthy';
  latency_ms?: number;
}

export interface FederationTopology {
  instances: FederatedInstance[];
}

// === Stats types ===

export interface QueryStats {
  total_queries: number;
  avg_latency_ms: number;
  error_rate: number;
  queries_per_minute: number;
}

// === Saved queries types ===

export interface SavedQuery {
  name: string;
  query: string;
  format: 'sql' | 'ppl';
  description?: string;
  saved_at?: number;
}


// === Dashboard types (#521) ===

export interface DashboardPanel {
  id: string;
  title: string;
  query: string;
  format: 'sql' | 'ppl';
  chartType: string;
  width: number;
}

export interface DashboardVariable {
  name: string;
  label: string;
  type: 'custom' | 'query';
  values: string[];
  query?: string;
  current: string;
}

export interface SavedDashboard {
  title: string;
  panels: DashboardPanel[];
  variables: DashboardVariable[];
  timeRange: string;
  refreshInterval: number;
  savedAt?: number;
}
