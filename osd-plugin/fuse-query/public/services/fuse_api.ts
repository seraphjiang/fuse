// SPDX-License-Identifier: Apache-2.0

import { HttpSetup } from '../../../src/core/public';
import {
  API_BASE,
  QueryRequest,
  QueryResponse,
  DatasourceInfo,
  HealthResponse,
  ValidateResponse,
  ExplainResponse,
  HistoryEntry,
  TraceResponse,
  SavedDashboard,
} from '../../common';

export class FuseApiService {
  constructor(private http: HttpSetup) {}

  async health(): Promise<HealthResponse> {
    return this.http.get(`${API_BASE}/health`);
  }

  async datasources(): Promise<DatasourceInfo[]> {
    return this.http.get(`${API_BASE}/datasources`);
  }

  async getSchemas(datasourceId: string): Promise<Array<{ name: string; schema_type: string }>> {
    return this.http.get(`${API_BASE}/datasources/${datasourceId}/schemas`);
  }

  async getFields(
    datasourceId: string,
    table: string
  ): Promise<Array<{ name: string; data_type: string; nullable: boolean }>> {
    return this.http.get(`${API_BASE}/datasources/${datasourceId}/schemas/${table}/fields`);
  }

  async query(request: QueryRequest): Promise<QueryResponse> {
    return this.http.post(`${API_BASE}/query`, { body: JSON.stringify(request) });
  }

  async validate(request: QueryRequest): Promise<ValidateResponse> {
    return this.http.post(`${API_BASE}/query/validate`, { body: JSON.stringify(request) });
  }

  async explain(request: QueryRequest): Promise<ExplainResponse> {
    return this.http.post(`${API_BASE}/query/explain`, { body: JSON.stringify(request) });
  }

  async history(): Promise<HistoryEntry[]> {
    return this.http.get(`${API_BASE}/history`);
  }

  async trace(traceId: string): Promise<TraceResponse> {
    return this.http.get(`${API_BASE}/trace/${encodeURIComponent(traceId)}`);
  }

  // Dashboard persistence (#521) — uses localStorage as OSD doesn't have a dashboard backend
  private readonly DASHBOARD_KEY = 'fuse-osd-dashboards';

  getDashboards(): Record<string, SavedDashboard> {
    try { return JSON.parse(localStorage.getItem(this.DASHBOARD_KEY) || '{}'); } catch { return {}; }
  }

  saveDashboard(dashboard: SavedDashboard): void {
    const all = this.getDashboards();
    all[dashboard.title] = { ...dashboard, savedAt: Date.now() };
    localStorage.setItem(this.DASHBOARD_KEY, JSON.stringify(all));
  }

  deleteDashboard(name: string): void {
    const all = this.getDashboards();
    delete all[name];
    localStorage.setItem(this.DASHBOARD_KEY, JSON.stringify(all));
  }
}
