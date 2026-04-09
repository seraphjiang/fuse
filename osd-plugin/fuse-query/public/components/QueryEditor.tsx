// SPDX-License-Identifier: Apache-2.0

import React from 'react';

interface Props {
  format: 'sql' | 'ppl';
  onFormatChange: (format: 'sql' | 'ppl') => void;
  query: string;
  onQueryChange: (query: string) => void;
  onExecute: () => void;
  loading: boolean;
}

export const QueryEditor: React.FC<Props> = ({
  format,
  onFormatChange,
  query,
  onQueryChange,
  onExecute,
  loading,
}) => {
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      onExecute();
    }
  };

  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{ marginBottom: 8, display: 'flex', gap: 8, alignItems: 'center' }}>
        <label>
          <input
            type="radio"
            value="sql"
            checked={format === 'sql'}
            onChange={() => onFormatChange('sql')}
          />{' '}
          SQL
        </label>
        <label>
          <input
            type="radio"
            value="ppl"
            checked={format === 'ppl'}
            onChange={() => onFormatChange('ppl')}
          />{' '}
          PPL
        </label>
      </div>
      <textarea
        value={query}
        onChange={(e) => onQueryChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={
          format === 'sql'
            ? 'SELECT * FROM cluster_a.logs WHERE status = 500'
            : 'source = cluster_a.logs | where status >= 500 | stats count() by service'
        }
        rows={6}
        style={{ width: '100%', fontFamily: 'monospace', fontSize: 14, padding: 8 }}
      />
      <button onClick={onExecute} disabled={loading || !query.trim()} style={{ marginTop: 8 }}>
        {loading ? 'Running...' : 'Run Query (Ctrl+Enter)'}
      </button>
    </div>
  );
};
