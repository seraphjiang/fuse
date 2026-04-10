// SPDX-License-Identifier: Apache-2.0

import React, { useState, useEffect, useCallback } from 'react';
import { FuseApiService } from '../services/fuse_api';
import { HistoryEntry } from '../../common';

interface Props {
  api: FuseApiService;
  onReplay: (query: string, format: 'sql' | 'ppl') => void;
}

const FAVORITES_KEY = 'fuse-osd-favorites';

const S = {
  container: { border: '1px solid #21262d', borderRadius: 8, background: '#0d1117', overflow: 'hidden' } as React.CSSProperties,
  header: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 12px', borderBottom: '1px solid #21262d', background: '#161b22' } as React.CSSProperties,
  title: { fontSize: 13, fontWeight: 600, color: '#58a6ff' } as React.CSSProperties,
  tabs: { display: 'flex', gap: 0 } as React.CSSProperties,
  tab: { padding: '4px 12px', fontSize: 11, cursor: 'pointer', border: '1px solid #30363d', background: '#21262d', color: '#8b949e' } as React.CSSProperties,
  tabActive: { background: '#238636', borderColor: '#238636', color: '#fff' } as React.CSSProperties,
  list: { maxHeight: 360, overflowY: 'auto' as const } as React.CSSProperties,
  item: { padding: '8px 12px', borderBottom: '1px solid #21262d', cursor: 'pointer', transition: 'background 0.1s' } as React.CSSProperties,
  query: { fontFamily: '"SF Mono", Consolas, monospace', fontSize: 11, color: '#e1e4e8', whiteSpace: 'nowrap' as const, overflow: 'hidden', textOverflow: 'ellipsis' } as React.CSSProperties,
  meta: { display: 'flex', gap: 10, fontSize: 10, color: '#8b949e', marginTop: 3 } as React.CSSProperties,
  error: { color: '#f85149' } as React.CSSProperties,
  badge: { fontSize: 9, padding: '1px 5px', borderRadius: 3 } as React.CSSProperties,
  badgeSql: { background: '#1f3a5f', color: '#58a6ff' } as React.CSSProperties,
  badgePpl: { background: '#2d1f3a', color: '#bc8cff' } as React.CSSProperties,
  starBtn: { background: 'none', border: 'none', cursor: 'pointer', fontSize: 14, padding: '0 4px' } as React.CSSProperties,
  empty: { color: '#484f58', fontSize: 12, padding: 24, textAlign: 'center' as const } as React.CSSProperties,
  actions: { display: 'flex', gap: 4, marginTop: 4 } as React.CSSProperties,
  actionBtn: { background: '#21262d', color: '#e1e4e8', border: '1px solid #30363d', borderRadius: 4, padding: '2px 8px', fontSize: 10, cursor: 'pointer' } as React.CSSProperties,
};

export const QueryHistory: React.FC<Props> = ({ api, onReplay }) => {
  const [tab, setTab] = useState<'history' | 'favorites'>('history');
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [favorites, setFavorites] = useState<HistoryEntry[]>([]);

  const loadHistory = useCallback(async () => {
    try { setHistory(await api.history()); } catch { setHistory([]); }
  }, [api]);

  const loadFavorites = useCallback(() => {
    try { setFavorites(JSON.parse(localStorage.getItem(FAVORITES_KEY) || '[]')); } catch { setFavorites([]); }
  }, []);

  useEffect(() => { loadHistory(); loadFavorites(); }, [loadHistory, loadFavorites]);

  const toggleFavorite = (entry: HistoryEntry, e: React.MouseEvent) => {
    e.stopPropagation();
    const existing = favorites.findIndex(f => f.query === entry.query && f.format === entry.format);
    let updated: HistoryEntry[];
    if (existing >= 0) {
      updated = favorites.filter((_, i) => i !== existing);
    } else {
      updated = [entry, ...favorites];
    }
    setFavorites(updated);
    localStorage.setItem(FAVORITES_KEY, JSON.stringify(updated));
  };

  const isFavorite = (entry: HistoryEntry) => favorites.some(f => f.query === entry.query && f.format === entry.format);

  const items = tab === 'history' ? history : favorites;

  return (
    <div style={S.container}>
      <div style={S.header}>
        <span style={S.title}>🕐 Query History</span>
        <div style={S.tabs}>
          <span style={{ ...S.tab, borderRadius: '4px 0 0 4px', ...(tab === 'history' ? S.tabActive : {}) }}
            onClick={() => setTab('history')}>Recent</span>
          <span style={{ ...S.tab, borderRadius: '0 4px 4px 0', ...(tab === 'favorites' ? S.tabActive : {}) }}
            onClick={() => setTab('favorites')}>★ Favorites ({favorites.length})</span>
        </div>
      </div>
      <div style={S.list}>
        {items.length === 0 && (
          <div style={S.empty}>
            {tab === 'history' ? 'No queries yet — run something first.' : 'No favorites yet — star a query to save it.'}
          </div>
        )}
        {items.map((entry, i) => (
          <div key={`${entry.timestamp}-${i}`} style={S.item}
            onMouseEnter={e => (e.currentTarget.style.background = '#161b22')}
            onMouseLeave={e => (e.currentTarget.style.background = '')}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <div style={{ ...S.query, flex: 1 }}>{entry.query}</div>
              <button style={S.starBtn} onClick={(e) => toggleFavorite(entry, e)}
                title={isFavorite(entry) ? 'Remove from favorites' : 'Add to favorites'}>
                {isFavorite(entry) ? '★' : '☆'}
              </button>
            </div>
            <div style={S.meta}>
              <span>{new Date(entry.timestamp * 1000).toLocaleTimeString()}</span>
              <span style={{ ...S.badge, ...(entry.format === 'ppl' ? S.badgePpl : S.badgeSql) }}>
                {entry.format.toUpperCase()}
              </span>
              {entry.error ? (
                <span style={S.error}>✗ {entry.error}</span>
              ) : (
                <>
                  <span>{entry.row_count} rows</span>
                  <span>{entry.latency_ms}ms</span>
                </>
              )}
            </div>
            <div style={S.actions}>
              <button style={S.actionBtn}
                onClick={() => onReplay(entry.query, entry.format as 'sql' | 'ppl')}>▶ Replay</button>
              <button style={S.actionBtn}
                onClick={() => { navigator.clipboard.writeText(entry.query).catch(() => {}); }}>📋 Copy</button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default QueryHistory;
