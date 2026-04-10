// SPDX-License-Identifier: Apache-2.0

import React, { useState, useMemo } from 'react';
import { TraceSpan, TraceResponse } from '../../common';

// ── Color palette for datasources ──
const DS_COLORS = ['#58a6ff', '#3fb950', '#d29922', '#f85149', '#bc8cff', '#39d2c0', '#ff7b72', '#79c0ff'];
function dsColor(ds: string, all: string[]): string {
  return DS_COLORS[all.indexOf(ds) % DS_COLORS.length];
}

interface Props {
  trace: TraceResponse;
}

const S = {
  container: { fontFamily: 'monospace', fontSize: 12, color: '#c9d1d9', padding: 8 } as React.CSSProperties,
  header: { display: 'flex', alignItems: 'center', gap: 12, marginBottom: 8, padding: '6px 0', borderBottom: '1px solid #21262d' } as React.CSSProperties,
  traceId: { fontSize: 14, fontWeight: 700, color: '#58a6ff' } as React.CSSProperties,
  stat: { fontSize: 11, color: '#8b949e' } as React.CSSProperties,
  legend: { display: 'flex', gap: 8, flexWrap: 'wrap' as const, marginBottom: 8 } as React.CSSProperties,
  legendItem: (color: string) => ({ display: 'flex', alignItems: 'center', gap: 4, fontSize: 11, color: '#c9d1d9' }) as React.CSSProperties,
  legendDot: (color: string) => ({ width: 8, height: 8, borderRadius: '50%', background: color }) as React.CSSProperties,
  timeline: { position: 'relative' as const, marginLeft: 60 } as React.CSSProperties,
  row: { display: 'flex', alignItems: 'center', padding: '2px 0', cursor: 'pointer', borderRadius: 3 } as React.CSSProperties,
  rowHover: { background: '#161b22' } as React.CSSProperties,
  ts: { width: 55, fontSize: 10, color: '#8b949e', textAlign: 'right' as const, marginRight: 8, flexShrink: 0 } as React.CSSProperties,
  bar: (color: string, left: number, width: number) => ({
    position: 'absolute' as const, height: 14, borderRadius: 3, background: color, opacity: 0.85,
    left: `${left}%`, width: `${Math.max(width, 0.5)}%`, top: 1,
  }) as React.CSSProperties,
  barContainer: { position: 'relative' as const, flex: 1, height: 16, background: '#0d1117', borderRadius: 3, overflow: 'hidden' as const } as React.CSSProperties,
  dsBadge: (color: string) => ({
    display: 'inline-block', padding: '0 5px', borderRadius: 3, fontSize: 10,
    fontWeight: 600, color: '#fff', background: color, marginLeft: 6, flexShrink: 0,
  }) as React.CSSProperties,
  detail: { fontSize: 11, color: '#8b949e', padding: '4px 8px 4px 72px', borderLeft: '2px solid #30363d', marginLeft: 60, marginBottom: 2 } as React.CSSProperties,
  empty: { color: '#8b949e', fontSize: 12, padding: 16, textAlign: 'center' as const } as React.CSSProperties,
};

function formatTs(ts?: string | null): string {
  if (!ts) return '—';
  // Show HH:MM:SS.mmm
  try {
    const d = new Date(ts);
    return d.toISOString().slice(11, 23);
  } catch {
    return String(ts).slice(11, 23) || String(ts);
  }
}

export const TraceTimeline: React.FC<Props> = ({ trace }) => {
  const [expanded, setExpanded] = useState<number | null>(null);

  const datasources = trace.datasources_matched;

  // Compute time range for bar positioning
  const { minMs, rangeMs } = useMemo(() => {
    const times = trace.spans
      .map(s => s.timestamp ? new Date(s.timestamp).getTime() : NaN)
      .filter(t => !isNaN(t));
    if (times.length === 0) return { minMs: 0, rangeMs: 1 };
    const min = Math.min(...times);
    const max = Math.max(...times);
    return { minMs: min, rangeMs: Math.max(max - min, 1) };
  }, [trace.spans]);

  if (trace.total_spans === 0) {
    return (
      <div style={S.empty}>
        No spans found for trace <code>{trace.trace_id}</code>.
        Searched {trace.datasources_searched.length} datasource{trace.datasources_searched.length !== 1 ? 's' : ''} in {trace.search_ms}ms.
      </div>
    );
  }

  return (
    <div style={S.container}>
      <div style={S.header}>
        <span style={S.traceId}>{trace.trace_id}</span>
        <span style={S.stat}>{trace.total_spans} spans</span>
        <span style={S.stat}>{trace.datasources_matched.length}/{trace.datasources_searched.length} datasources</span>
        <span style={S.stat}>{trace.search_ms}ms</span>
      </div>

      <div style={S.legend}>
        {datasources.map(ds => (
          <div key={ds} style={S.legendItem(dsColor(ds, datasources))}>
            <div style={S.legendDot(dsColor(ds, datasources))} />
            {ds}
          </div>
        ))}
      </div>

      {trace.spans.map((span, i) => {
        const t = span.timestamp ? new Date(span.timestamp).getTime() : NaN;
        const left = isNaN(t) ? 0 : ((t - minMs) / rangeMs) * 100;
        const color = dsColor(span.datasource, datasources);
        const isExpanded = expanded === i;

        return (
          <React.Fragment key={i}>
            <div
              style={S.row}
              onClick={() => setExpanded(isExpanded ? null : i)}
            >
              <span style={S.ts}>{formatTs(span.timestamp)}</span>
              <div style={S.barContainer}>
                <div style={S.bar(color, left, 2)} />
              </div>
              <span style={S.dsBadge(color)}>{span.datasource}</span>
            </div>
            {isExpanded && (
              <div style={S.detail}>
                {Object.entries(span.fields).map(([k, v]) => (
                  <div key={k}><strong>{k}:</strong> {String(v)}</div>
                ))}
              </div>
            )}
          </React.Fragment>
        );
      })}
    </div>
  );
};

export default TraceTimeline;
