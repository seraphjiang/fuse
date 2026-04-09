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
} from '../../common';

export class FuseApiService {
  constructor(private http: HttpSetup) {}

  async health(): Promise<HealthResponse> {
    return this.http.get(`${API_BASE}/health`);
  }

  async datasources(): Promise<DatasourceInfo[]> {
    return this.http.get(`${API_BASE}/datasources`);
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
}
