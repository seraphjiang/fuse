// SPDX-License-Identifier: Apache-2.0

import React, { useState, useEffect, useRef } from 'react';
import { FuseApiService } from '../services/fuse_api';
import { DatasourceInfo } from '../../common';

interface Props {
  api: FuseApiService;
  selected: string[];
  onChange: (ids: string[]) => void;
}

export const DatasourceSelector: React.FC<Props> = ({ api, selected, onChange }) => {
  const [datasources, setDatasources] = useState<DatasourceInfo[]>([]);
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    api.datasources().then(setDatasources).catch(() => setDatasources([]));
  }, [api]);

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const toggle = (id: string) => {
    onChange(selected.includes(id) ? selected.filter((s) => s !== id) : [...selected, id]);
  };

  const label =
    selected.length === 0
      ? 'All datasources'
      : selected.length === 1
      ? selected[0]
      : `${selected.length} datasources`;

  return (
    <div ref={ref} style={{ position: 'relative', display: 'inline-block' }}>
      <button
        onClick={() => setOpen((o) => !o)}
        style={{ minWidth: 160, textAlign: 'left', display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}
        aria-haspopup="listbox"
        aria-expanded={open}
      >
        <span>{label}</span>
        <span aria-hidden>▾</span>
      </button>

      {open && (
        <div
          role="listbox"
          aria-multiselectable="true"
          style={{
            position: 'absolute',
            top: '100%',
            left: 0,
            zIndex: 100,
            background: '#161b22',
            border: '1px solid #30363d',
            borderRadius: 6,
            minWidth: 220,
            boxShadow: '0 4px 12px rgba(0,0,0,.4)',
            padding: '4px 0',
          }}
        >
          {/* "All" option clears selection */}
          <label
            style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '6px 12px', cursor: 'pointer' }}
          >
            <input
              type="checkbox"
              checked={selected.length === 0}
              onChange={() => onChange([])}
            />
            <span style={{ color: '#8b949e', fontStyle: 'italic' }}>All datasources</span>
          </label>

          {datasources.map((ds) => (
            <label
              key={ds.id}
              role="option"
              aria-selected={selected.includes(ds.id)}
              style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '6px 12px', cursor: 'pointer' }}
            >
              <input
                type="checkbox"
                checked={selected.includes(ds.id)}
                onChange={() => toggle(ds.id)}
              />
              <span style={{ flex: 1 }}>{ds.id}</span>
              <span style={{ fontSize: 11, color: '#8b949e' }}>{ds.connector_type}</span>
            </label>
          ))}

          {datasources.length === 0 && (
            <div style={{ padding: '6px 12px', color: '#8b949e', fontSize: 12 }}>No datasources</div>
          )}
        </div>
      )}
    </div>
  );
};
