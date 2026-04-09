// SPDX-License-Identifier: Apache-2.0

import React, { useState, useMemo } from 'react';
import ReactDOM from 'react-dom';
import { AppMountParameters, CoreStart } from '../../../src/core/public';
import { QueryEditor } from './components/QueryEditor';
import { ResultsTable } from './components/ResultsTable';
import { DatasourceSelector } from './components/DatasourceSelector';
import { HealthIndicator } from './components/HealthIndicator';
import { FuseApiService } from './services/fuse_api';
import { QueryResponse } from '../common';

const FuseQueryApp: React.FC<{ http: CoreStart['http'] }> = ({ http }) => {
  const api = useMemo(() => new FuseApiService(http), [http]);
  const [format, setFormat] = useState<'sql' | 'ppl'>('sql');
  const [query, setQuery] = useState('');
  const [datasource, setDatasource] = useState('');
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleExecute = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const resp = await api.query({ query, format });
      setResult(resp);
    } catch (e: any) {
      setError(e?.body?.message || e?.message || 'Query failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: 24, maxWidth: 1200 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 20 }}>Fuse — Federated Query</h1>
        <HealthIndicator api={api} />
      </div>
      <DatasourceSelector api={api} selected={datasource} onChange={setDatasource} />
      <QueryEditor
        format={format}
        onFormatChange={setFormat}
        query={query}
        onQueryChange={setQuery}
        onExecute={handleExecute}
        loading={loading}
      />
      <ResultsTable result={result} error={error} />
    </div>
  );
};

export const renderApp = (core: CoreStart, { element }: AppMountParameters) => {
  ReactDOM.render(<FuseQueryApp http={core.http} />, element);
  return () => ReactDOM.unmountComponentAtNode(element);
};
