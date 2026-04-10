// SPDX-License-Identifier: Apache-2.0

import React, { useState, useEffect, useCallback } from 'react';
import { FuseApiService } from '../services/fuse_api';
import { DatasourceInfo } from '../../common';

interface FieldInfo { name: string; data_type: string; nullable: boolean; }
interface SchemaInfo { name: string; schema_type: string; }

interface Props {
  api: FuseApiService;
  onInsert?: (text: string) => void;
}

const S = {
  container: { border: '1px solid #21262d', borderRadius: 8, background: '#0d1117', overflow: 'hidden' } as React.CSSProperties,
  header: { padding: '10px 12px', borderBottom: '1px solid #21262d', background: '#161b22', fontSize: 13, fontWeight: 600, color: '#58a6ff' } as React.CSSProperties,
  body: { display: 'flex', minHeight: 280, maxHeight: 400 } as React.CSSProperties,
  dsList: { width: 200, borderRight: '1px solid #21262d', overflowY: 'auto' as const } as React.CSSProperties,
  dsItem: { padding: '8px 12px', cursor: 'pointer', borderBottom: '1px solid #21262d', fontSize: 12 } as React.CSSProperties,
  dsActive: { background: '#161b22', borderLeft: '2px solid #58a6ff' } as React.CSSProperties,
  dsName: { fontWeight: 600, color: '#e1e4e8' } as React.CSSProperties,
  dsType: { fontSize: 10, color: '#8b949e', marginTop: 2 } as React.CSSProperties,
  detail: { flex: 1, overflowY: 'auto' as const, padding: 12 } as React.CSSProperties,
  schemaItem: { padding: '6px 0', cursor: 'pointer' } as React.CSSProperties,
  schemaName: { fontSize: 12, color: '#e1e4e8', fontWeight: 500 } as React.CSSProperties,
  fieldRow: { display: 'flex', justifyContent: 'space-between', padding: '3px 0 3px 16px', fontSize: 11 } as React.CSSProperties,
  fieldName: { color: '#e1e4e8', cursor: 'pointer' } as React.CSSProperties,
  fieldType: { color: '#8b949e', fontSize: 10 } as React.CSSProperties,
  badge: { fontSize: 9, padding: '1px 5px', borderRadius: 3, marginLeft: 4 } as React.CSSProperties,
  empty: { color: '#484f58', fontSize: 12, padding: 16, textAlign: 'center' as const } as React.CSSProperties,
  capRow: { display: 'flex', gap: 4, flexWrap: 'wrap' as const, marginTop: 8 } as React.CSSProperties,
  capBadge: { fontSize: 9, padding: '1px 5px', borderRadius: 3, background: '#1f3a5f', color: '#58a6ff' } as React.CSSProperties,
};

export const DatasourcePicker: React.FC<Props> = ({ api, onInsert }) => {
  const [datasources, setDatasources] = useState<DatasourceInfo[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [schemas, setSchemas] = useState<SchemaInfo[]>([]);
  const [expandedSchema, setExpandedSchema] = useState<string | null>(null);
  const [fields, setFields] = useState<FieldInfo[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    api.datasources().then(setDatasources).catch(() => {});
  }, [api]);

  const selectDs = useCallback(async (id: string) => {
    setSelected(id);
    setExpandedSchema(null);
    setFields([]);
    setLoading(true);
    try {
      const s = await api.getSchemas(id);
      setSchemas(s);
    } catch { setSchemas([]); }
    setLoading(false);
  }, [api]);

  const toggleSchema = useCallback(async (table: string) => {
    if (expandedSchema === table) { setExpandedSchema(null); setFields([]); return; }
    setExpandedSchema(table);
    if (!selected) return;
    try {
      const f = await api.getFields(selected, table);
      setFields(f);
    } catch { setFields([]); }
  }, [api, selected, expandedSchema]);

  const handleInsertTable = (table: string) => {
    if (onInsert && selected) onInsert(`${selected}.${table}`);
  };

  const handleInsertField = (field: string) => {
    if (onInsert) onInsert(field);
  };

  const ds = datasources.find(d => d.id === selected);

  return (
    <div style={S.container}>
      <div style={S.header}>📂 Datasource Browser</div>
      <div style={S.body}>
        <div style={S.dsList}>
          {datasources.map(d => (
            <div key={d.id} style={{ ...S.dsItem, ...(selected === d.id ? S.dsActive : {}) }}
              onClick={() => selectDs(d.id)}>
              <div style={S.dsName}>{d.id}</div>
              <div style={S.dsType}>{d.connector_type}</div>
            </div>
          ))}
          {!datasources.length && <div style={S.empty}>No datasources</div>}
        </div>
        <div style={S.detail}>
          {!selected && <div style={S.empty}>Select a datasource to browse schemas and fields</div>}
          {selected && loading && <div style={S.empty}>Loading...</div>}
          {selected && !loading && (
            <>
              {ds && (
                <div style={S.capRow}>
                  {ds.capabilities.supports_filtering && <span style={S.capBadge}>filter</span>}
                  {ds.capabilities.supports_projection && <span style={S.capBadge}>project</span>}
                  {ds.capabilities.supports_aggregation && <span style={S.capBadge}>aggregate</span>}
                  {ds.capabilities.supports_sorting && <span style={S.capBadge}>sort</span>}
                  {ds.capabilities.supports_limit && <span style={S.capBadge}>limit</span>}
                  {ds.capabilities.supports_join && <span style={S.capBadge}>join</span>}
                  <span style={{ ...S.capBadge, background: '#2d1f3a', color: '#bc8cff' }}>{ds.capabilities.latency_class}</span>
                </div>
              )}
              <div style={{ marginTop: 12 }}>
                {schemas.map(s => (
                  <div key={s.name} style={S.schemaItem}>
                    <div style={S.schemaName} onClick={() => toggleSchema(s.name)}>
                      {expandedSchema === s.name ? '▾' : '▸'} {s.name}
                      <span style={{ ...S.badge, background: '#21262d', color: '#8b949e' }}>{s.schema_type}</span>
                      {onInsert && (
                        <span style={{ ...S.badge, background: '#238636', color: '#fff', cursor: 'pointer', marginLeft: 6 }}
                          onClick={(e) => { e.stopPropagation(); handleInsertTable(s.name); }}>
                          + Insert
                        </span>
                      )}
                    </div>
                    {expandedSchema === s.name && fields.map(f => (
                      <div key={f.name} style={S.fieldRow}>
                        <span style={S.fieldName} onClick={() => handleInsertField(f.name)}>{f.name}</span>
                        <span style={S.fieldType}>
                          {f.data_type}{f.nullable && ' ?'}
                        </span>
                      </div>
                    ))}
                  </div>
                ))}
                {!schemas.length && <div style={S.empty}>No schemas found</div>}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default DatasourcePicker;
