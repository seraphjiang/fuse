// SPDX-License-Identifier: Apache-2.0

import { DataSourcePlugin } from '@grafana/data';
import { FuseDatasource } from './datasource';
import { FuseConfigEditor } from './config_editor';
import { FuseQueryEditor } from './query_editor';
import { FuseQuery, FuseDataSourceOptions } from './types';

export const plugin = new DataSourcePlugin<FuseDatasource, FuseQuery, FuseDataSourceOptions>(FuseDatasource)
  .setConfigEditor(FuseConfigEditor)
  .setQueryEditor(FuseQueryEditor);
