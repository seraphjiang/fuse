// SPDX-License-Identifier: Apache-2.0

import React, { useRef, useCallback } from 'react';

interface Props {
  format: 'sql' | 'ppl';
  onFormatChange: (format: 'sql' | 'ppl') => void;
  query: string;
  onQueryChange: (query: string) => void;
  onExecute: () => void;
  onExplain: () => void;
  loading: boolean;
  analyze: boolean;
  onAnalyzeChange: (v: boolean) => void;
}

// Minimal keyword sets for highlighting
const SQL_KEYWORDS = /\b(SELECT|FROM|WHERE|AND|OR|NOT|IN|LIKE|BETWEEN|JOIN|LEFT|RIGHT|INNER|OUTER|ON|GROUP|BY|ORDER|ASC|DESC|LIMIT|UNION|ALL|AS|COUNT|SUM|AVG|MIN|MAX|HAVING|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|IS|NULL|DISTINCT|CASE|WHEN|THEN|ELSE|END|EXISTS)\b/gi;
const PPL_KEYWORDS = /\b(source|where|stats|sort|head|fields|dedup|eval|by|as|count|sum|avg|min|max|and|or|not|in|like)\b/gi;

function highlightSyntax(text: string, format: 'sql' | 'ppl'): string {
  const escaped = text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  const keywords = format === 'sql' ? SQL_KEYWORDS : PPL_KEYWORDS;

  // Highlight strings
  let result = escaped.replace(/'[^']*'/g, '<span style="color:#a5d6ff">$&</span>');
  // Highlight numbers
  result = result.replace(/\b(\d+(?:\.\d+)?)\b/g, '<span style="color:#79c0ff">$1</span>');
  // Highlight keywords
  result = result.replace(keywords, '<span style="color:#ff7b72;font-weight:600">$&</span>');
  // Highlight datasource.table pattern
  result = result.replace(
    /\b(\w+)\.(\w+)\b/g,
    '<span style="color:#d2a8ff">$1</span>.<span style="color:#7ee787">$2</span>'
  );

  return result;
}

export const QueryEditor: React.FC<Props> = ({
  format,
  onFormatChange,
  query,
  onQueryChange,
  onExecute,
  onExplain,
  loading,
  analyze,
  onAnalyzeChange,
}) => {
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        onExecute();
      }
    },
    [onExecute]
  );

  const containerStyle: React.CSSProperties = {
    position: 'relative',
    marginBottom: 16,
  };

  const editorWrapStyle: React.CSSProperties = {
    position: 'relative',
    border: '1px solid #343741',
    borderRadius: 4,
    background: '#1d1e24',
    marginBottom: 8,
  };

  const sharedTextStyle: React.CSSProperties = {
    fontFamily: '"SF Mono", Consolas, "Liberation Mono", Menlo, monospace',
    fontSize: 14,
    lineHeight: '1.5',
    padding: 12,
    width: '100%',
    minHeight: 120,
    whiteSpace: 'pre-wrap',
    wordWrap: 'break-word',
    overflowWrap: 'break-word',
  };

  const highlightStyle: React.CSSProperties = {
    ...sharedTextStyle,
    color: '#dfe1e6',
    pointerEvents: 'none',
  };

  const textareaStyle: React.CSSProperties = {
    ...sharedTextStyle,
    position: 'absolute',
    top: 0,
    left: 0,
    height: '100%',
    background: 'transparent',
    color: 'transparent',
    caretColor: '#dfe1e6',
    border: 'none',
    outline: 'none',
    resize: 'vertical',
    boxSizing: 'border-box',
  };

  const toggleStyle = (active: boolean): React.CSSProperties => ({
    padding: '4px 12px',
    fontSize: 12,
    fontWeight: 600,
    border: '1px solid #343741',
    borderRadius: 4,
    cursor: 'pointer',
    background: active ? '#006bb4' : '#1d1e24',
    color: active ? '#fff' : '#98a2b3',
  });

  const btnStyle = (primary?: boolean): React.CSSProperties => ({
    padding: '6px 16px',
    fontSize: 13,
    fontWeight: 600,
    border: primary ? 'none' : '1px solid #343741',
    borderRadius: 4,
    cursor: loading ? 'not-allowed' : 'pointer',
    background: primary ? '#006bb4' : '#1d1e24',
    color: primary ? '#fff' : '#dfe1e6',
    opacity: loading ? 0.6 : 1,
  });

  return (
    <div style={containerStyle}>
      {/* Format toggle */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 8, alignItems: 'center' }}>
        <button style={toggleStyle(format === 'sql')} onClick={() => onFormatChange('sql')}>
          SQL
        </button>
        <button style={toggleStyle(format === 'ppl')} onClick={() => onFormatChange('ppl')}>
          PPL
        </button>
        <span style={{ flex: 1 }} />
        <label style={{ fontSize: 12, color: '#98a2b3', display: 'flex', alignItems: 'center', gap: 4, cursor: 'pointer' }}>
          <input type="checkbox" checked={analyze} onChange={(e) => onAnalyzeChange(e.target.checked)} />
          Analyze
        </label>
      </div>

      {/* Editor with syntax overlay */}
      <div style={editorWrapStyle}>
        <div
          style={highlightStyle}
          dangerouslySetInnerHTML={{ __html: highlightSyntax(query, format) + '\n' }}
          aria-hidden
        />
        <textarea
          ref={textareaRef}
          value={query}
          onChange={(e) => onQueryChange(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={
            format === 'sql'
              ? 'SELECT * FROM cluster_a.application_logs WHERE status >= 500 LIMIT 20'
              : 'source = cluster_a.application_logs | where status >= 500 | head 20'
          }
          style={textareaStyle}
          spellCheck={false}
          aria-label="Query editor"
        />
      </div>

      {/* Action buttons */}
      <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
        <button style={btnStyle(true)} onClick={onExecute} disabled={loading || !query.trim()}>
          {loading ? '⏳ Running...' : '▶ Run (Ctrl+Enter)'}
        </button>
        <button style={btnStyle()} onClick={onExplain} disabled={loading || !query.trim()}>
          Explain
        </button>
      </div>
    </div>
  );
};
