// SPDX-License-Identifier: Apache-2.0

export interface QueryRequest {
  query: string;
  format: 'sql' | 'ppl';
  analyze?: boolean;
  timeout_ms?: number;
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
  datasources_queried?: string[];
  datasource_stats?: Record<string, DatasourceStat>;
  execution_profile?: ExecutionProfile;
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

export const PLUGIN_ID = 'fuseQuery';
export const PLUGIN_NAME = 'Fuse Query';
export const API_BASE = '/api/fuse_query';
