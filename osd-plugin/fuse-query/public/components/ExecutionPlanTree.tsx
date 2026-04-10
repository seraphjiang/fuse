import React, { useState, useMemo, useCallback } from 'react';
import { ProfileNode, PlanNode } from '../../common';

// ── Types ──

type TreeNode = ProfileNode | PlanNode;

interface Props {
  /** analyze=true profile data */
  profile?: { total_ms: number; nodes: ProfileNode[] };
  /** explain plan data */
  plan?: PlanNode;
}

// ── Helpers ──

function isProfile(n: TreeNode): n is ProfileNode {
  return 'actual_ms' in n;
}

function getMs(n: TreeNode): number {
  return isProfile(n) ? n.actual_ms : (n as PlanNode).estimated_cost ?? 0;
}

function getRows(n: TreeNode): number {
  return isProfile(n) ? n.actual_rows : (n as PlanNode).estimated_rows ?? 0;
}

/** Find max cost in tree for color normalization */
function maxCost(nodes: TreeNode[]): number {
  let m = 0;
  for (const n of nodes) {
    m = Math.max(m, getMs(n));
    if (n.children.length) m = Math.max(m, maxCost(n.children as TreeNode[]));
  }
  return m;
}

/** Green→Yellow→Red gradient based on 0-1 ratio */
function costColor(ratio: number): string {
  if (ratio < 0.33) return '#3fb950';
  if (ratio < 0.66) return '#d29922';
  return '#f85149';
}

/** Mark critical path (most expensive child at each level) */
function markCritical(nodes: TreeNode[]): Set<TreeNode> {
  const set = new Set<TreeNode>();
  function walk(ns: TreeNode[]) {
    if (!ns.length) return;
    const worst = ns.reduce((a, b) => getMs(a) >= getMs(b) ? a : b);
    set.add(worst);
    walk(worst.children as TreeNode[]);
  }
  walk(nodes);
  return set;
}

function formatBytes(b?: number): string {
  if (b == null) return '';
  if (b < 1024) return `${b}B`;
  if (b < 1048576) return `${(b / 1024).toFixed(1)}KB`;
  return `${(b / 1048576).toFixed(1)}MB`;
}

// ── Styles ──

const S = {
  container: { fontFamily: 'monospace', fontSize: 13, color: '#c9d1d9', padding: 8 } as React.CSSProperties,
  summary: { fontSize: 12, color: '#8b949e', marginBottom: 8, padding: '4px 0', borderBottom: '1px solid #21262d' } as React.CSSProperties,
  nodeRow: { display: 'flex', alignItems: 'center', padding: '3px 0', cursor: 'pointer', borderRadius: 4 } as React.CSSProperties,
  nodeRowHover: { background: '#161b22' } as React.CSSProperties,
  opBadge: (color: string) => ({
    display: 'inline-block', padding: '1px 6px', borderRadius: 3,
    fontSize: 11, fontWeight: 700, color: '#fff', background: color, marginRight: 6,
  } as React.CSSProperties),
  dsBadge: { display: 'inline-block', padding: '1px 5px', borderRadius: 3, fontSize: 10, background: '#1f2937', color: '#58a6ff', marginRight: 4 } as React.CSSProperties,
  pushBadge: { display: 'inline-block', padding: '1px 4px', borderRadius: 3, fontSize: 10, background: '#1c2333', color: '#3fb950', marginRight: 3 } as React.CSSProperties,
  stat: { fontSize: 11, color: '#8b949e', marginLeft: 6 } as React.CSSProperties,
  criticalBar: { width: 3, borderRadius: 2, background: '#f85149', marginRight: 4, alignSelf: 'stretch' as const } as React.CSSProperties,
  detail: { fontSize: 11, color: '#8b949e', padding: '4px 0 4px 28px', borderLeft: '1px solid #30363d', marginLeft: 14 } as React.CSSProperties,
  connector: { width: 1, borderLeft: '1px solid #30363d', marginLeft: 8, marginRight: 8, minHeight: 16 } as React.CSSProperties,
  arrow: { color: '#30363d', fontSize: 10, marginRight: 4 } as React.CSSProperties,
  costBar: (ratio: number, color: string) => ({
    height: 4, borderRadius: 2, background: color,
    width: `${Math.max(4, ratio * 100)}%`, marginTop: 2,
  } as React.CSSProperties),
};

// ── Node Component ──

const TreeNodeView: React.FC<{
  node: TreeNode;
  depth: number;
  maxMs: number;
  critical: Set<TreeNode>;
}> = ({ node, depth, maxMs, critical }) => {
  const [expanded, setExpanded] = useState(false);
  const [hovered, setHovered] = useState(false);

  const ms = getMs(node);
  const rows = getRows(node);
  const ratio = maxMs > 0 ? ms / maxMs : 0;
  const color = costColor(ratio);
  const isCritical = critical.has(node);
  const profile = isProfile(node) ? node : null;
  const plan = !isProfile(node) ? node : null;

  return (
    <div style={{ marginLeft: depth > 0 ? 20 : 0 }}>
      <div
        style={{ ...S.nodeRow, ...(hovered ? S.nodeRowHover : {}) }}
        onClick={() => setExpanded(!expanded)}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
      >
        {isCritical && <div style={S.criticalBar} />}
        {depth > 0 && <span style={S.arrow}>└─</span>}
        <span style={S.opBadge(color)}>{node.op}</span>
        {profile?.datasource && <span style={S.dsBadge}>{profile.datasource}</span>}
        <span style={S.stat}>{rows.toLocaleString()} rows</span>
        <span style={S.stat}>{ms.toFixed(1)}ms</span>
        {profile?.data_bytes != null && <span style={S.stat}>{formatBytes(profile.data_bytes)}</span>}
        {profile?.pushdown?.map((p, i) => <span key={i} style={S.pushBadge}>{p}</span>)}
        <span style={{ ...S.stat, marginLeft: 'auto', fontSize: 10 }}>{expanded ? '▼' : '▶'}</span>
      </div>
      <div style={{ marginLeft: depth > 0 ? 20 : 0, maxWidth: 300 }}>
        <div style={S.costBar(ratio, color)} />
      </div>
      {expanded && (
        <div style={S.detail}>
          <div><strong>Operation:</strong> {node.op}</div>
          {profile?.datasource && <div><strong>Datasource:</strong> {profile.datasource}</div>}
          <div><strong>Rows:</strong> {rows.toLocaleString()}</div>
          <div><strong>Time:</strong> {ms.toFixed(2)}ms ({maxMs > 0 ? (ratio * 100).toFixed(1) : 0}% of total)</div>
          {profile?.data_bytes != null && <div><strong>Data:</strong> {formatBytes(profile.data_bytes)}</div>}
          {profile?.pushdown?.length ? <div><strong>Pushdown:</strong> {profile.pushdown.join(', ')}</div> : null}
          {plan?.detail && <div><strong>Detail:</strong> {plan.detail}</div>}
          {plan?.estimated_cost != null && <div><strong>Est. Cost:</strong> {plan.estimated_cost.toFixed(2)}</div>}
        </div>
      )}
      {node.children.map((child, i) => (
        <TreeNodeView key={i} node={child as TreeNode} depth={depth + 1} maxMs={maxMs} critical={critical} />
      ))}
    </div>
  );
};

// ── Main Component ──

export const ExecutionPlanTree: React.FC<Props> = ({ profile, plan }) => {
  const nodes: TreeNode[] = useMemo(() => {
    if (profile?.nodes?.length) return profile.nodes;
    if (plan) return [plan];
    return [];
  }, [profile, plan]);

  const mMax = useMemo(() => maxCost(nodes), [nodes]);
  const critical = useMemo(() => markCritical(nodes), [nodes]);

  if (!nodes.length) {
    return <div style={{ color: '#8b949e', fontSize: 12, padding: 8 }}>No execution plan data. Run with <code>analyze=true</code> or use Explain.</div>;
  }

  return (
    <div style={S.container}>
      {profile && (
        <div style={S.summary}>
          Total: {profile.total_ms.toFixed(1)}ms
          {' · '}{nodes.length} node{nodes.length !== 1 ? 's' : ''}
          {' · '}<span style={{ color: '#f85149' }}>■</span> critical path
        </div>
      )}
      {nodes.map((node, i) => (
        <TreeNodeView key={i} node={node} depth={0} maxMs={mMax} critical={critical} />
      ))}
    </div>
  );
};

export default ExecutionPlanTree;
