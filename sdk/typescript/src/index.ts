// SPDX-License-Identifier: Apache-2.0

/** Result of a Fuse query. */
export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  totalRows: number;
  format: string;
  traceId: string;
  datasourcesQueried?: string[];
  nextCursor?: string;
}

/** Result of a trace reconstruction. */
export interface TraceResult {
  traceId: string;
  spans: TraceSpan[];
  datasourcesSearched: string[];
  datasourcesMatched: string[];
  totalSpans: number;
  searchMs: number;
}

export interface TraceSpan {
  datasource: string;
  timestamp?: string | null;
  fields: Record<string, unknown>;
}

export interface HealthResponse {
  status: string;
  connectors: Record<string, { status: string; latency_ms?: number; message?: string }>;
}

export interface DatasourceInfo {
  id: string;
  connector_type: string;
}

export interface ExplainResponse {
  plan: string;
  plan_tree?: unknown;
}

export interface ValidateResponse {
  valid: boolean;
  error?: string;
}

export interface FuseClientOptions {
  baseUrl?: string;
  apiKey?: string;
  /** Custom fetch implementation (for Node.js or testing). */
  fetch?: typeof globalThis.fetch;
}

export class FuseError extends Error {
  statusCode: number;
  body: string;
  constructor(statusCode: number, body: string) {
    super(`HTTP ${statusCode}: ${body}`);
    this.name = 'FuseError';
    this.statusCode = statusCode;
    this.body = body;
  }
}

export class FuseClient {
  private baseUrl: string;
  private apiKey?: string;
  private _fetch: typeof globalThis.fetch;

  constructor(options: FuseClientOptions = {}) {
    this.baseUrl = (options.baseUrl || 'http://localhost:3000').replace(/\/$/, '');
    this.apiKey = options.apiKey;
    this._fetch = options.fetch || globalThis.fetch;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = { 'Content-Type': 'application/json' };
    if (this.apiKey) h['x-api-key'] = this.apiKey;
    return h;
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const resp = await this._fetch(`${this.baseUrl}${path}`, {
      method,
      headers: this.headers(),
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new FuseError(resp.status, text);
    }
    return resp.json() as Promise<T>;
  }

  /** Execute a SQL or PPL query. */
  async query(
    sql: string,
    options: { format?: string; params?: Record<string, unknown>; pageSize?: number; cursor?: string } = {},
  ): Promise<QueryResult> {
    const body: Record<string, unknown> = { query: sql, format: options.format || 'sql' };
    if (options.params) body.params = options.params;
    if (options.pageSize) body.page_size = options.pageSize;
    if (options.cursor) body.cursor = options.cursor;

    const resp = await this.request<any>('POST', '/api/fuse/query', body);
    const meta = resp.metadata || {};
    return {
      columns: resp.columns || [],
      rows: resp.rows || [],
      totalRows: meta.total_rows || 0,
      format: meta.format || options.format || 'sql',
      traceId: meta.trace_id || '',
      datasourcesQueried: meta.datasources_queried,
      nextCursor: resp.next_cursor,
    };
  }

  /** Query with automatic cursor pagination — fetches all pages. */
  async queryAll(sql: string, options: { format?: string; pageSize?: number } = {}): Promise<QueryResult> {
    const pageSize = options.pageSize || 1000;
    const first = await this.query(sql, { ...options, pageSize });
    const allRows = [...first.rows];
    let cursor = first.nextCursor;
    while (cursor) {
      const page = await this.query(sql, { ...options, pageSize, cursor });
      allRows.push(...page.rows);
      cursor = page.nextCursor;
    }
    return { ...first, rows: allRows, totalRows: allRows.length, nextCursor: undefined };
  }

  /** Get execution plan. */
  async explain(sql: string, format = 'sql'): Promise<ExplainResponse> {
    return this.request('POST', '/api/fuse/query/explain', { query: sql, format });
  }

  /** Validate query syntax. */
  async validate(sql: string, format = 'sql'): Promise<ValidateResponse> {
    return this.request('POST', '/api/fuse/query/validate', { query: sql, format });
  }

  /** Check connector health. */
  async health(): Promise<HealthResponse> {
    return this.request('GET', '/api/fuse/health');
  }

  /** List connected datasources. */
  async datasources(): Promise<DatasourceInfo[]> {
    return this.request('GET', '/api/fuse/datasources');
  }

  /** Reconstruct a trace across all datasources. */
  async trace(traceId: string): Promise<TraceResult> {
    const resp = await this.request<any>('GET', `/api/fuse/trace/${encodeURIComponent(traceId)}`);
    return {
      traceId: resp.trace_id,
      spans: resp.spans,
      datasourcesSearched: resp.datasources_searched,
      datasourcesMatched: resp.datasources_matched,
      totalSpans: resp.total_spans,
      searchMs: resp.search_ms,
    };
  }

  /** Get query history. */
  /** List saved queries. */
  async savedQueries(): Promise<unknown[]> {
    return this.request('GET', '/api/fuse/saved');
  }

  /** Save a query. */
  async saveQuery(name: string, query: string, description = ''): Promise<unknown> {
    return this.request('POST', '/api/fuse/saved', { name, query, description });
  }

  /** Get a saved query by name. */
  async getSavedQuery(name: string): Promise<unknown> {
    return this.request('GET', `/api/fuse/saved/${name}`);
  }

  /** Delete a saved query. */
  async deleteSavedQuery(name: string): Promise<unknown> {
    return this.request('DELETE', `/api/fuse/saved/${name}`);
  }


  /** Submit an async query. Returns job_id. */
  async submitAsync(sql: string, format = 'sql'): Promise<{ jobId: string }> {
    const resp = await this.request<{ job_id: string }>('POST', '/api/fuse/query/async', { query: sql, format });
    return { jobId: resp.job_id };
  }

  /** Poll async query status. */
  async pollAsync(jobId: string): Promise<{ status: string; result?: unknown; error?: string }> {
    return this.request('GET', `/api/fuse/query/async/${jobId}`);
  }

  /** Cancel an async query. */
  async cancelAsync(jobId: string): Promise<unknown> {
    return this.request('DELETE', `/api/fuse/query/async/${jobId}`);
  }

  async history(): Promise<unknown[]> {
    return this.request('GET', '/api/fuse/history');
  }

  // ── Sprint 18: Webhooks (#1811) ──

  async webhooks(): Promise<unknown[]> {
    return this.request('GET', '/api/fuse/webhooks');
  }

  async createWebhook(name: string, query: string, condition: unknown, callbackUrl: string, format = 'sql'): Promise<{ id: string }> {
    return this.request('POST', '/api/fuse/webhooks', { name, query, format, condition, callback_url: callbackUrl });
  }

  async deleteWebhook(id: string): Promise<unknown> {
    return this.request('DELETE', `/api/fuse/webhooks/${id}`);
  }

  async testWebhook(id: string): Promise<{ fired: boolean; row_count?: number }> {
    return this.request('POST', `/api/fuse/webhooks/${id}/test`);
  }

  // ── Sprint 18: Schema Relationships (#1831) ──

  async relationships(): Promise<unknown[]> {
    return this.request('GET', '/api/fuse/relationships');
  }

  // ── Sprint 18: CDC (#1852) ──

  async cdcStatus(): Promise<unknown> {
    return this.request('GET', '/api/fuse/cdc/status');
  }

  async cdcEvent(datasource: string, table: string, changeType = 'insert'): Promise<{ accepted: boolean; affected_views: string[] }> {
    return this.request('POST', '/api/fuse/cdc/events', { datasource, table, change_type: changeType, timestamp: Math.floor(Date.now() / 1000) });
  }

  // ── Predictive Performance ──

  async predict(query: string): Promise<{ estimated_ms: number; confidence: string }> {
    return this.request('GET', `/api/fuse/predict?query=${encodeURIComponent(query)}`);
  }
}

export default FuseClient;
