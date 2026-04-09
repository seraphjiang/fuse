// SPDX-License-Identifier: Apache-2.0

import React, { useState, useMemo } from 'react';
import ReactDOM from 'react-dom';
import { AppMountParameters, CoreStart } from '../../../src/core/public';
import { QueryEditor } from './components/QueryEditor';
import { ResultsTable } from './components/ResultsTable';
import { DatasourceSelector } from './components/DatasourceSelector';
import { HealthIndicator } from './components/HealthIndicator';
import { FuseApiService } from './services/fuse_api';
import { QueryResponse, ExplainResponse } from '../common';

const FuseQueryApp: React.FC<{ http: CoreStart['http'] }> = ({ http }) => {
  const api = useMemo(() => new FuseApiService(http), [http]);
  const [format, setFormat] = useState<'sql' | 'ppl'>('sql');
  const [query, setQuery] = useState('');
  const [datasource, setDatasource] = useState<string[]>([]);
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [analyze, setAnalyze] = useState(false);
  const [explainResult, setExplainResult] = useState<ExplainResponse | null>(null);

  const handleExecute = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);
    setExplainResult(null);
    try {
      const resp = await api.query({ query, format, analyze });
      setResult(resp);
    } catch (e: any) {
      setError(e?.body?.message || e?.message || 'Query failed');
    } finally {
      setLoading(false);
    }
  };

  const handleExplain = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);
    setExplainResult(null);
    try {
      const resp = await api.explain({ query, format });
      setExplainResult(resp);
    } catch (e: any) {
      setError(e?.body?.message || e?.message || 'Explain failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: 24, maxWidth: 1200 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 20, color: '#e1e4e8' }}>Fuse — Federated Query</h1>
        <HealthIndicator api={api} />
      </div>
      <DatasourceSelector api={api} selected={datasource} onChange={setDatasource} />
      <QueryEditor
        format={format}
        onFormatChange={setFormat}
        query={query}
        onQueryChange={setQuery}
        onExecute={handleExecute}
        onExplain={handleExplain}
        loading={loading}
        analyze={analyze}
        onAnalyzeChange={setAnalyze}
      />
      {explainResult && (
        <div style={{ marginBottom: 16, padding: 12, background: '#0d1117', border: '1px solid #30363d', borderRadius: 6 }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: '#58a6ff', marginBottom: 8 }}>Execution Plan</div>
          <pre style={{ margin: 0, fontSize: 12, color: '#e1e4e8', whiteSpace: 'pre-wrap' }}>{explainResult.plan}</pre>
        </div>
      )}
      <ResultsTable result={result} error={error} />
    </div>
  );
};

export const renderApp = (core: CoreStart, { element }: AppMountParameters) => {
  ReactDOM.render(<FuseQueryApp http={core.http} />, element);
  return () => ReactDOM.unmountComponentAtNode(element);
};
