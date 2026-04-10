// SPDX-License-Identifier: Apache-2.0

import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  MutableDataFrame,
  FieldType,
} from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';
import { FuseQuery, FuseDataSourceOptions } from './types';

interface FuseApiResponse {
  columns: string[];
  rows: unknown[][];
  metadata: { total_rows: number };
}

export class FuseDatasource extends DataSourceApi<FuseQuery, FuseDataSourceOptions> {
  url: string;
  apiKey?: string;

  constructor(instanceSettings: DataSourceInstanceSettings<FuseDataSourceOptions>) {
    super(instanceSettings);
    this.url = instanceSettings.jsonData.url || instanceSettings.url || '';
    this.apiKey = instanceSettings.jsonData.apiKey;
  }

  async query(options: DataQueryRequest<FuseQuery>): Promise<DataQueryResponse> {
    const promises = options.targets
      .filter(t => !t.hide && t.queryText)
      .map(target => this.runQuery(target));

    const data = await Promise.all(promises);
    return { data };
  }

  private async runQuery(target: FuseQuery): Promise<MutableDataFrame> {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (this.apiKey) headers['X-API-Key'] = this.apiKey;

    const response = await getBackendSrv().post<FuseApiResponse>(
      `${this.url}/api/fuse/query`,
      { query: target.queryText, format: target.format || 'sql' },
      { headers }
    );

    const frame = new MutableDataFrame({ refId: target.refId, fields: [] });

    // Detect field types from first row
    response.columns.forEach((col, i) => {
      const sample = response.rows[0]?.[i];
      let type = FieldType.string;
      if (typeof sample === 'number' || (!isNaN(Number(sample)) && sample !== '')) type = FieldType.number;
      if (/^(timestamp|time|date)/i.test(col) || (!isNaN(Date.parse(String(sample))) && String(sample).includes('-'))) type = FieldType.time;
      frame.addField({ name: col, type });
    });

    response.rows.forEach(row => {
      const values: Record<string, unknown> = {};
      response.columns.forEach((col, i) => {
        const field = frame.fields.find(f => f.name === col);
        if (field?.type === FieldType.number) values[col] = Number(row[i]);
        else if (field?.type === FieldType.time) values[col] = new Date(String(row[i])).getTime();
        else values[col] = row[i];
      });
      frame.add(values);
    });

    return frame;
  }

  async testDatasource(): Promise<{ status: string; message: string }> {
    try {
      const headers: Record<string, string> = {};
      if (this.apiKey) headers['X-API-Key'] = this.apiKey;
      const health = await getBackendSrv().get(`${this.url}/api/fuse/health`, undefined, undefined, { headers });
      return health.status === 'ok'
        ? { status: 'success', message: `Connected to Fuse (${Object.keys(health.connectors || {}).length} connectors)` }
        : { status: 'error', message: `Fuse status: ${health.status}` };
    } catch (e: any) {
      return { status: 'error', message: e?.message || 'Failed to connect' };
    }
  }
}
