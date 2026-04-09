// SPDX-License-Identifier: Apache-2.0

export interface QueryRequest {
  query: string;
  format: 'sql' | 'ppl';
}

export interface QueryResponse {
  columns: string[];
  rows: unknown[][];
  total_rows: number;
  truncated: boolean;
}

export interface DatasourceInfo {
  id: string;
  type: string;
  status: string;
}

export interface HealthResponse {
  status: string;
  connectors: Record<string, { status: string; latency_ms?: number }>;
}

export interface ValidateResponse {
  valid: boolean;
  error?: string;
}

export interface ExplainResponse {
  plan: string;
}

export const PLUGIN_ID = 'fuseQuery';
export const PLUGIN_NAME = 'Fuse Query';
export const API_BASE = '/api/fuse_query';
