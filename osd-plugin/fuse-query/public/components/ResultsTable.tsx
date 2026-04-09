// SPDX-License-Identifier: Apache-2.0

import React, { useState, useMemo } from 'react';
import { QueryResponse, DatasourceStat } from '../../common';

interface Props {
  result: QueryResponse | null;
  error: string | null;
}

const PAGE_SIZE = 50;

const DS_COLORS: Array<{ bg: string; fg: string }> = [
  { bg: '#1f3a5f', fg: '#58a6ff' },
  { bg: '#2d1f3a', fg: '#bc8cff' },
  { bg: '#1a3a2a', fg: '#3fb950' },
  { bg: '#3a2a1a', fg: '#d29922' },
  { bg: '#3a1a1a', fg: '#f85149' },
  { bg: '#1a2a3a', fg: '#79c0ff' },
];

function getDsColor(name: string, map: Map<string, number>): { bg: string; fg: string } {
  if (!map.has(name)) map.set(name, map.size);
  return DS_COLORS[map.get(name)! % DS_COLORS.length];
}

export const ResultsTable: React.FC<Props> = ({ result, error }) => {
  const [sortCol, setSortCol] = useState<number | null>(null);
  const [sortAsc, setSortAsc] = useState(true);
  const [page, setPage] = useState(0);
  const dsColorMap = useMemo(() => new Map<string, number>(), [result]);

  if (error) {
    return (
      <div style={{ color: '#f85149', padding: 12, background: '#1c0d0d', border: '1px solid #f8514933', borderRadius: 6, fontSize: 13 }}>
        {error}
      </div>
    );
  }

  if (!result) return null;
  if (result.rows.length === 0) {
    return <div style={{ padding: 12, color: '#8b949e' }}>No results</div>;
  }

  const dsIdx = result.columns.indexOf('_datasource');

  // Sort
  const sorted = useMemo(() => {
    if (sortCol === null) return result.rows;
    return [...result.rows].sort((a, b) => {
      const va = a[sortCol];
      const vb = b[sortCol];
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === 'number' && typeof vb === 'number') return sortAsc ? va - vb : vb - va;
      const sa = String(va), sb = String(vb);
      return sortAsc ? sa.localeCompare(sb) : sb.localeCompare(sa);
    });
  }, [result.rows, sortCol, sortAsc]);

  const totalPages = Math.ceil(sorted.length / PAGE_SIZE);
  const pageRows = sorted.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  const handleSort = (idx: number) => {
    if (sortCol === idx) {
      setSortAsc(!sortAsc);
    } else {
      setSortCol(idx);
      setSortAsc(true);
    }
    setPage(0);
  };

  const thStyle: React.CSSProperties = {
    padding: '6px 12px',
    textAlign: 'left',
    whiteSpace: 'nowrap',
    borderBottom: '2px solid #30363d',
    cursor: 'pointer',
    userSelect: 'none',
    fontSize: 12,
    fontWeight: 600,
    color: '#8b949e',
    background: '#161b22',
  };

  const tdStyle: React.CSSProperties = {
    padding: '4px 12px',
    borderBottom: '1px solid #21262d',
    whiteSpace: 'nowrap',
    fontSize: 13,
    color: '#e1e4e8',
    maxWidth: 400,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
  };

  const stats = result.metadata.datasource_stats;

  return (
    <div>
      {/* Provenance bar */}
      {stats && Object.keys(stats).length > 0 && (
        <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginBottom: 8, fontSize: 12, color: '#8b949e' }}>
          <span>Results from:</span>
          {Object.entries(stats).map(([name, s]: [string, DatasourceStat]) => {
            const c = getDsColor(name, dsColorMap);
            return (
              <span key={name} style={{ padding: '2px 8px', borderRadius: 4, background: c.bg, color: c.fg, fontSize: 11, fontWeight: 500 }}>
                {name} ({s.rows} rows, {s.latency_ms}ms)
              </span>
            );
          })}
        </div>
      )}

      {/* Metadata */}
      <div style={{ marginBottom: 8, fontSize: 12, color: '#8b949e', display: 'flex', gap: 12 }}>
        <span>{result.metadata.total_rows} rows</span>
        <span>{result.columns.length} columns</span>
        {totalPages > 1 && <span>Page {page + 1} of {totalPages}</span>}
      </div>

      {/* Table */}
      <div style={{ overflowX: 'auto', border: '1px solid #30363d', borderRadius: 6 }}>
        <table style={{ borderCollapse: 'collapse', width: '100%' }}>
          <thead>
            <tr>
              {result.columns.map((col, i) => (
                <th key={i} style={thStyle} onClick={() => handleSort(i)}>
                  {col}
                  {sortCol === i ? (sortAsc ? ' ▲' : ' ▼') : ''}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageRows.map((row, ri) => (
              <tr key={ri} style={{ background: ri % 2 === 0 ? '#0d1117' : '#161b22' }}>
                {row.map((cell, ci) => {
                  // Color-code _datasource column
                  if (ci === dsIdx && dsIdx >= 0 && cell != null) {
                    const c = getDsColor(String(cell), dsColorMap);
                    return (
                      <td key={ci} style={tdStyle}>
                        <span style={{ padding: '1px 6px', borderRadius: 3, background: c.bg, color: c.fg, fontSize: 11, fontWeight: 600 }}>
                          {String(cell)}
                        </span>
                      </td>
                    );
                  }
                  return (
                    <td key={ci} style={tdStyle} title={cell != null ? String(cell) : ''}>
                      {cell == null ? <span style={{ color: '#484f58' }}>null</span> : String(cell)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div style={{ display: 'flex', gap: 8, marginTop: 8, alignItems: 'center' }}>
          <button
            onClick={() => setPage(Math.max(0, page - 1))}
            disabled={page === 0}
            style={{ padding: '4px 12px', fontSize: 12, cursor: page === 0 ? 'not-allowed' : 'pointer' }}
          >
            ← Prev
          </button>
          <span style={{ fontSize: 12, color: '#8b949e' }}>
            {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, sorted.length)} of {sorted.length}
          </span>
          <button
            onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
            disabled={page >= totalPages - 1}
            style={{ padding: '4px 12px', fontSize: 12, cursor: page >= totalPages - 1 ? 'not-allowed' : 'pointer' }}
          >
            Next →
          </button>
        </div>
      )}
    </div>
  );
};
