// SPDX-License-Identifier: Apache-2.0

import React, { ChangeEvent } from 'react';
import { QueryEditorProps } from '@grafana/data';
import { InlineField, Input, Select } from '@grafana/ui';
import { FuseDatasource } from './datasource';
import { FuseQuery, FuseDataSourceOptions, defaultQuery } from './types';

type Props = QueryEditorProps<FuseDatasource, FuseQuery, FuseDataSourceOptions>;

export const FuseQueryEditor: React.FC<Props> = ({ query, onChange, onRunQuery }) => {
  const q = { ...defaultQuery, ...query };

  const onQueryTextChange = (e: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...q, queryText: e.target.value });
  };

  const onFormatChange = (v: any) => {
    onChange({ ...q, format: v.value });
    onRunQuery();
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      onRunQuery();
    }
  };

  return (
    <div className="gf-form-group">
      <InlineField label="Format" labelWidth={8}>
        <Select
          width={12}
          options={[{ label: 'SQL', value: 'sql' }, { label: 'PPL', value: 'ppl' }]}
          value={q.format}
          onChange={onFormatChange}
        />
      </InlineField>
      <InlineField label="Query" labelWidth={8} grow>
        <Input
          value={q.queryText || ''}
          onChange={onQueryTextChange}
          onKeyDown={onKeyDown}
          onBlur={onRunQuery}
          placeholder="SELECT * FROM cluster_a.application_logs LIMIT 20"
        />
      </InlineField>
    </div>
  );
};
