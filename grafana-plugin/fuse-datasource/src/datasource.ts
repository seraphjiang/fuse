// SPDX-License-Identifier: Apache-2.0

import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  MetricFindValue,
  MutableDataFrame,
  FieldType,
} from '@grafana/data';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { FuseQuery, FuseDataSourceOptions } from './types';

interface CostEstimateEntry {
  datasource: string;
  connector_type: string;
  estimated_cost_usd: number;
  cost_breakdown: string;
}

interface FuseApiResponse {
  columns: string[];
  rows: unknown[][];
  metadata: {
    total_rows: number;
    cost_estimate?: {
      total_cost_usd: number;
      per_datasource: CostEstimateEntry[];
    };
  };
}

export class FuseDatasource extends DataSourceApi<FuseQuery, FuseDataSourceOptions> {
  url: string;
  apiKey?: string;

  constructor(instanceSettings: DataSourceInstanceSettings<FuseDataSourceOptions>) {
    super(instanceSettings);
    this.url = instanceSettings.jsonData.url || instanceSettings.url || '';
    this.apiKey = instanceSettings.jsonData.apiKey;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = { 'Content-Type': 'application/json' };
    if (this.apiKey) h['X-API-Key'] = this.apiKey;
    return h;
  }

  async query(options: DataQueryRequest<FuseQuery>): Promise<DataQueryResponse> {
    const promises = options.targets
      .filter(t => !t.hide && t.queryText)
      .map(target => this.runQuery(target, options.scopedVars));

    const data = await Promise.all(promises);
    return { data };
  }

  private async runQuery(target: FuseQuery, scopedVars?: Record<string, any>): Promise<MutableDataFrame> {
    const queryText = getTemplateSrv().replace(target.queryText, scopedVars);

    const response = await getBackendSrv().post<FuseApiResponse>(
      `${this.url}/api/fuse/query`,
      { query: queryText, format: target.format || 'sql' },
      { headers: this.headers() }
    );

    const frame = new MutableDataFrame({ refId: target.refId, fields: [] });

    // Surface cost estimate as frame notice (#1803)
    if (response.metadata.cost_estimate) {
      const cost = response.metadata.cost_estimate;
      const details = cost.per_datasource
        .filter(e => e.estimated_cost_usd > 0)
        .map(e => `${e.datasource}: $${e.estimated_cost_usd.toFixed(4)} (${e.cost_breakdown})`);
      if (details.length > 0) {
        (frame.meta ??= {}).notices = [{
          severity: cost.total_cost_usd > 1.0 ? 'warning' : 'info',
          text: `Estimated cost: $${cost.total_cost_usd.toFixed(4)} — ${details.join(', ')}`,
        }];
      }
    }

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

  /**
   * Template variable support — enables $datasource and $table Grafana variables.
   *   datasources()          -> list registered datasources
   *   tables($datasource)    -> list tables for a datasource
   */
  async metricFindQuery(query: string): Promise<MetricFindValue[]> {
    const q = query.trim();

    if (q === 'datasources()') {
      const resp = await getBackendSrv().get(
        `${this.url}/api/fuse/datasources`,
        undefined, undefined, { headers: this.headers() }
      );
      return (resp as Array<{ id: string }>).map(d => ({ text: d.id }));
    }

    const m = q.match(/^tables\((.+)\)$/);
    if (m) {
      const ds = getTemplateSrv().replace(m[1].trim());
      const resp = await getBackendSrv().get(
        `${this.url}/api/fuse/datasources/${ds}/schemas`,
        undefined, undefined, { headers: this.headers() }
      );
      return (resp as Array<{ name: string }>).map(t => ({ text: t.name }));
    }

    return [];
  }

  async testDatasource(): Promise<{ status: string; message: string }> {
    try {
      const health = await getBackendSrv().get(
        `${this.url}/api/fuse/health`,
        undefined, undefined, { headers: this.headers() }
      );
      return health.status === 'healthy'
        ? { status: 'success', message: `Connected to Fuse (${Object.keys(health.connectors || {}).length} connectors)` }
        : { status: 'error', message: `Fuse status: ${health.status}` };
    } catch (e: any) {
      return { status: 'error', message: e?.message || 'Failed to connect' };
    }
  }
}
