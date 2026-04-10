// SPDX-License-Identifier: Apache-2.0

import React, { useState, useMemo, useRef, useEffect } from 'react';

// ── Types ──

export type ChartType = 'auto' | 'line' | 'bar' | 'stacked-bar' | 'pie' | 'area' | 'scatter' | 'histogram' | 'table';

type ColType = 'time' | 'number' | 'category' | 'datasource' | 'string' | 'unknown';

interface ChartSuggestion {
  type: string;
  x: string;
  y: string | null;
  series?: string | null;
  label?: string;
  value?: string;
}

interface PanelConfig {
  title: string;
  query: string;
  chartType: ChartType;
}

interface Props {
  columns: string[];
  rows: unknown[][];
  /** Panel config for dashboard mode */
  config?: PanelConfig;
  /** Callback when chart type changes */
  onChartTypeChange?: (type: ChartType) => void;
  /** External ECharts instance setter — parent provides the init'd chart */
  chartRef?: React.RefObject<HTMLDivElement>;
}

// ── Column type detection ──

function detectColumnTypes(columns: string[], rows: unknown[][]): Record<string, ColType> {
  const types: Record<string, ColType> = {};
  columns.forEach((col, i) => {
    const sample = rows.slice(0, 50).map(r => r[i]).filter(v => v != null && v !== '');
    if (!sample.length) { types[col] = 'unknown'; return; }
    if (col === '_datasource' || col === 'datasource') { types[col] = 'datasource'; return; }
    const allNum = sample.every(v => !isNaN(Number(v)));
    const allTime = sample.every(v => !isNaN(Date.parse(String(v))));
    const uniq = new Set(sample.map(String)).size;
    if (/^(timestamp|time|date|created|updated)/i.test(col) || allTime) types[col] = 'time';
    else if (allNum) types[col] = 'number';
    else if (uniq <= 30) types[col] = 'category';
    else types[col] = 'string';
  });
  return types;
}

function suggestChart(columns: string[], rows: unknown[][]): ChartSuggestion | null {
  const types = detectColumnTypes(columns, rows);
  const cats = columns.filter(c => types[c] === 'category' || types[c] === 'datasource');
  const nums = columns.filter(c => types[c] === 'number');
  const times = columns.filter(c => types[c] === 'time');

  if (times.length && nums.length) return { type: 'line', x: times[0], y: nums[0], series: cats[0] || null };
  if (cats.length && nums.length) {
    if (nums.length === 1 && rows.length <= 12) return { type: 'pie', label: cats[0], value: nums[0], x: cats[0], y: nums[0] };
    return { type: 'bar', x: cats[0], y: nums[0], series: cats[1] || null };
  }
  if (nums.length >= 2) return { type: 'scatter', x: nums[0], y: nums[1] };
  if (cats.length >= 1 && rows.length) return { type: 'bar', x: cats[0], y: null };
  return null;
}

// ── ECharts option builder ──

export function buildChartOption(
  suggestion: ChartSuggestion,
  columns: string[],
  rows: unknown[][],
): Record<string, unknown> {
  const ci = (col: string) => columns.indexOf(col);
  const vals = (col: string) => rows.map(r => r[ci(col)]);
  const numVals = (col: string) => rows.map(r => Number(r[ci(col)]));

  const dark = { backgroundColor: 'transparent', textStyle: { color: '#8b949e' } };
  const tooltip = { trigger: 'axis', backgroundColor: '#161b22', borderColor: '#30363d', textStyle: { color: '#e1e4e8' } };

  if (suggestion.type === 'pie') {
    const label = suggestion.label || suggestion.x;
    const value = suggestion.value || suggestion.y || '';
    const labels = vals(label).map(String);
    const values = numVals(value);
    return {
      ...dark,
      tooltip: { trigger: 'item' },
      series: [{
        type: 'pie', radius: ['40%', '70%'],
        data: labels.map((name, i) => ({ name, value: values[i] })),
        label: { color: '#c9d1d9' },
      }],
    };
  }

  if (suggestion.type === 'scatter') {
    return {
      ...dark, tooltip,
      xAxis: { type: 'value', name: suggestion.x },
      yAxis: { type: 'value', name: suggestion.y || '' },
      series: [{
        type: 'scatter',
        data: rows.map(r => [Number(r[ci(suggestion.x)]), Number(r[ci(suggestion.y || '')])]),
      }],
    };
  }

  // Line, bar, area, stacked-bar, histogram
  const xData = vals(suggestion.x).map(String);
  const isArea = suggestion.type === 'area';
  const isStacked = suggestion.type === 'stacked-bar';
  const echartsType = (isArea || suggestion.type === 'line') ? 'line' : 'bar';

  if (!suggestion.series || !suggestion.y) {
    const yData = suggestion.y ? numVals(suggestion.y) : xData.map(() => 1);
    return {
      ...dark, tooltip,
      xAxis: { type: 'category', data: xData },
      yAxis: { type: 'value' },
      series: [{
        type: echartsType, data: yData,
        smooth: echartsType === 'line',
        areaStyle: isArea ? { opacity: 0.3 } : undefined,
        stack: isStacked ? 'total' : undefined,
      }],
    };
  }

  // Grouped series
  const seriesCol = suggestion.series;
  const groups = [...new Set(vals(seriesCol).map(String))];
  return {
    ...dark, tooltip,
    legend: { data: groups, textStyle: { color: '#8b949e' } },
    xAxis: { type: 'category', data: [...new Set(xData)] },
    yAxis: { type: 'value' },
    series: groups.map(name => {
      const map: Record<string, number> = {};
      rows.forEach(r => { if (String(r[ci(seriesCol)]) === name) map[String(r[ci(suggestion.x)])] = Number(r[ci(suggestion.y!)]); });
      return {
        name, type: echartsType,
        data: [...new Set(xData)].map(x => map[x] || 0),
        smooth: echartsType === 'line',
        areaStyle: isArea ? { opacity: 0.3 } : undefined,
        stack: isStacked ? 'total' : undefined,
      };
    }),
  };
}

// ── Styles ──

const S = {
  container: { fontFamily: '-apple-system, sans-serif', fontSize: 13, color: '#c9d1d9' } as React.CSSProperties,
  toolbar: { display: 'flex', alignItems: 'center', gap: 8, padding: '4px 0', borderBottom: '1px solid #21262d', marginBottom: 8 } as React.CSSProperties,
  select: { background: '#161b22', color: '#c9d1d9', border: '1px solid #30363d', borderRadius: 4, padding: '3px 6px', fontSize: 12 } as React.CSSProperties,
  label: { fontSize: 11, color: '#8b949e' } as React.CSSProperties,
  badge: { fontSize: 10, padding: '1px 5px', borderRadius: 3, background: '#1f2937', color: '#58a6ff' } as React.CSSProperties,
  chartArea: { width: '100%', height: 320, background: '#0d1117', borderRadius: 6, border: '1px solid #21262d' } as React.CSSProperties,
  empty: { color: '#8b949e', fontSize: 12, padding: 32, textAlign: 'center' as const } as React.CSSProperties,
};

const CHART_OPTIONS: ChartType[] = ['auto', 'line', 'bar', 'stacked-bar', 'pie', 'area', 'scatter', 'histogram', 'table'];

// ── Component ──

export const DashboardPanel: React.FC<Props> = ({ columns, rows, config, onChartTypeChange, chartRef }) => {
  const [chartType, setChartType] = useState<ChartType>(config?.chartType || 'auto');

  const suggestion = useMemo(() => suggestChart(columns, rows), [columns, rows]);

  const resolved = useMemo(() => {
    if (chartType === 'auto' || chartType === 'table') return suggestion;
    if (!suggestion) return null;
    const s = { ...suggestion, type: chartType };
    if (chartType === 'pie') { s.label = s.label || s.x; s.value = s.value || s.y || undefined; }
    return s;
  }, [chartType, suggestion]);

  const chartOption = useMemo(() => {
    if (!resolved || chartType === 'table') return null;
    return buildChartOption(resolved, columns, rows);
  }, [resolved, columns, rows, chartType]);

  const handleTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const t = e.target.value as ChartType;
    setChartType(t);
    onChartTypeChange?.(t);
  };

  // Render chart via ECharts if available on window
  const internalRef = useRef<HTMLDivElement>(null);
  const ref = chartRef || internalRef;

  useEffect(() => {
    if (!chartOption || !ref.current) return;
    const echarts = (window as any).echarts;
    if (!echarts) return;
    const chart = echarts.init(ref.current, 'dark');
    chart.setOption(chartOption);
    const onResize = () => chart.resize();
    window.addEventListener('resize', onResize);
    return () => { chart.dispose(); window.removeEventListener('resize', onResize); };
  }, [chartOption, ref]);

  if (!columns.length || !rows.length) {
    return <div style={S.empty}>No data to visualize. Run a query first.</div>;
  }

  const showChart = chartType !== 'table' && chartOption;

  return (
    <div style={S.container}>
      <div style={S.toolbar}>
        {config?.title && <strong>{config.title}</strong>}
        <span style={S.label}>Chart:</span>
        <select style={S.select} value={chartType} onChange={handleTypeChange}>
          {CHART_OPTIONS.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
        {chartType === 'auto' && suggestion && (
          <span style={S.badge}>Auto: {suggestion.type}</span>
        )}
        <span style={S.label}>{rows.length} rows · {columns.length} cols</span>
      </div>

      {showChart ? (
        <div ref={ref} style={S.chartArea} />
      ) : (
        <div style={S.empty}>
          {chartType === 'table' ? 'Table view — use ResultsTable component' : 'Cannot render chart for this data'}
        </div>
      )}
    </div>
  );
};

// Re-export helpers for external use
export { detectColumnTypes, suggestChart };
export type { ChartSuggestion, ColType };

export default DashboardPanel;
