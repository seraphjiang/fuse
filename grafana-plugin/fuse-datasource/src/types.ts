// SPDX-License-Identifier: Apache-2.0

import { DataSourceJsonData, DataQuery } from '@grafana/data';

export interface FuseQuery extends DataQuery {
  queryText: string;
  format: 'sql' | 'ppl';
}

export const defaultQuery: Partial<FuseQuery> = {
  queryText: 'SELECT * FROM cluster_a.application_logs LIMIT 20',
  format: 'sql',
};

export interface FuseDataSourceOptions extends DataSourceJsonData {
  url: string;
  apiKey?: string;
  defaultTimeout?: number;
}
