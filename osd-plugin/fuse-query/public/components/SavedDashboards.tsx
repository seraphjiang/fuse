// SPDX-License-Identifier: Apache-2.0

import React, { useState, useEffect, useCallback } from 'react';
import { FuseApiService } from '../services/fuse_api';
import { SavedDashboard, DashboardPanel as PanelType } from '../../common';

interface Props {
  api: FuseApiService;
  onLoad: (dashboard: SavedDashboard) => void;
  currentDashboard: SavedDashboard | null;
}

const S = {
  container: { padding: '12px 0' } as React.CSSProperties,
  header: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 } as React.CSSProperties,
  title: { fontSize: 14, fontWeight: 600, color: '#58a6ff' } as React.CSSProperties,
  btn: { background: '#21262d', color: '#e1e4e8', border: '1px solid #30363d', borderRadius: 6, padding: '6px 12px', fontSize: 12, cursor: 'pointer' } as React.CSSProperties,
  btnPrimary: { background: '#238636', borderColor: '#238636', color: '#fff', fontWeight: 600 } as React.CSSProperties,
  btnDanger: { background: 'none', borderColor: '#f8514933', color: '#f85149' } as React.CSSProperties,
  list: { display: 'flex', flexDirection: 'column' as const, gap: 8 },
  card: { padding: '10px 12px', border: '1px solid #21262d', borderRadius: 6, background: '#0d1117', cursor: 'pointer', transition: 'border-color 0.15s' } as React.CSSProperties,
  cardName: { fontSize: 13, fontWeight: 600, color: '#e1e4e8' } as React.CSSProperties,
  cardMeta: { fontSize: 11, color: '#8b949e', marginTop: 4 } as React.CSSProperties,
  empty: { color: '#484f58', fontSize: 13, padding: 24, textAlign: 'center' as const } as React.CSSProperties,
  actions: { display: 'flex', gap: 6, marginTop: 6 },
};

export const SavedDashboards: React.FC<Props> = ({ api, onLoad, currentDashboard }) => {
  const [dashboards, setDashboards] = useState<Record<string, SavedDashboard>>({});
  const [showExport, setShowExport] = useState(false);
  const [importJson, setImportJson] = useState('');

  const refresh = useCallback(() => setDashboards(api.getDashboards()), [api]);
  useEffect(() => { refresh(); }, [refresh]);

  const handleSave = () => {
    if (!currentDashboard) return;
    const name = prompt('Dashboard name:', currentDashboard.title || 'Untitled');
    if (!name) return;
    api.saveDashboard({ ...currentDashboard, title: name });
    refresh();
  };

  const handleDelete = (name: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!confirm(`Delete "${name}"?`)) return;
    api.deleteDashboard(name);
    refresh();
  };

  const handleExport = () => {
    if (!currentDashboard) return;
    const json = JSON.stringify(currentDashboard, null, 2);
    navigator.clipboard.writeText(json).catch(() => {});
    setShowExport(true);
    setTimeout(() => setShowExport(false), 2000);
  };

  const handleImport = () => {
    try {
      const d = JSON.parse(importJson) as SavedDashboard;
      if (!d.panels || !Array.isArray(d.panels)) throw new Error('Invalid');
      onLoad(d);
      setImportJson('');
    } catch { alert('Invalid dashboard JSON'); }
  };

  const entries = Object.entries(dashboards).sort(([, a], [, b]) => (b.savedAt || 0) - (a.savedAt || 0));

  return (
    <div style={S.container}>
      <div style={S.header}>
        <span style={S.title}>Saved Dashboards</span>
        <div style={{ display: 'flex', gap: 6 }}>
          <button style={{ ...S.btn, ...S.btnPrimary }} onClick={handleSave}>💾 Save Current</button>
          <button style={S.btn} onClick={handleExport}>
            {showExport ? '✓ Copied!' : '📋 Export JSON'}
          </button>
        </div>
      </div>

      {entries.length === 0 ? (
        <div style={S.empty}>No saved dashboards yet. Create panels and click "Save Current".</div>
      ) : (
        <div style={S.list}>
          {entries.map(([name, d]) => (
            <div key={name} style={S.card} onClick={() => onLoad(d)}
              onMouseEnter={e => (e.currentTarget.style.borderColor = '#58a6ff')}
              onMouseLeave={e => (e.currentTarget.style.borderColor = '#21262d')}>
              <div style={S.cardName}>{name}</div>
              <div style={S.cardMeta}>
                {d.panels.length} panels · {d.timeRange} range · {d.variables?.length || 0} variables
                {d.savedAt && ` · ${new Date(d.savedAt).toLocaleString()}`}
              </div>
              <div style={S.actions}>
                <button style={{ ...S.btn, ...S.btnDanger, padding: '2px 8px', fontSize: 11 }}
                  onClick={(e) => handleDelete(name, e)}>Delete</button>
              </div>
            </div>
          ))}
        </div>
      )}

      <div style={{ marginTop: 12 }}>
        <div style={{ fontSize: 11, color: '#8b949e', marginBottom: 4 }}>Import dashboard JSON:</div>
        <div style={{ display: 'flex', gap: 6 }}>
          <input type="text" value={importJson} onChange={e => setImportJson(e.target.value)}
            placeholder='Paste JSON here...'
            style={{ flex: 1, background: '#0d1117', color: '#e1e4e8', border: '1px solid #30363d', borderRadius: 6, padding: '6px 10px', fontSize: 12 }} />
          <button style={S.btn} onClick={handleImport} disabled={!importJson}>Import</button>
        </div>
      </div>
    </div>
  );
};

export default SavedDashboards;
