// SPDX-License-Identifier: Apache-2.0

import React from 'react';
import { QueryResponse } from '../../common';

interface Props {
  result: QueryResponse | null;
  error: string | null;
}

export const ResultsTable: React.FC<Props> = ({ result, error }) => {
  if (error) {
    return <div style={{ color: 'red', padding: 8 }}>Error: {error}</div>;
  }

  if (!result) {
    return null;
  }

  if (result.rows.length === 0) {
    return <div style={{ padding: 8, color: '#666' }}>No results</div>;
  }

  return (
    <div style={{ overflowX: 'auto' }}>
      <div style={{ marginBottom: 8, fontSize: 12, color: '#666' }}>
        {result.total_rows} rows{result.truncated ? ' (truncated)' : ''}
      </div>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: 13 }}>
        <thead>
          <tr>
            {result.columns.map((col) => (
              <th
                key={col}
                style={{
                  borderBottom: '2px solid #ddd',
                  padding: '6px 12px',
                  textAlign: 'left',
                  whiteSpace: 'nowrap',
                }}
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, i) => (
            <tr key={i}>
              {row.map((cell, j) => (
                <td
                  key={j}
                  style={{
                    borderBottom: '1px solid #eee',
                    padding: '4px 12px',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {cell == null ? <span style={{ color: '#999' }}>null</span> : String(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
