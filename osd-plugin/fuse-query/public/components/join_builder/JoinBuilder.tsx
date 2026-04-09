// SPDX-License-Identifier: Apache-2.0

import React, { useState, useEffect } from 'react';
import { FuseApiService } from '../../services/fuse_api';

export interface JoinCondition {
  leftDatasource: string;
  leftTable: string;
  leftField: string;
  rightDatasource: string;
  rightTable: string;
  rightField: string;
  joinType: 'INNER' | 'LEFT' | 'CROSS';
}

interface Props {
  api: FuseApiService;
  onChange: (sql: string) => void;
}

interface TableOption {
  datasource: string;
  table: string;
}

interface FieldOption {
  name: string;
  data_type: string;
}

export const JoinBuilder: React.FC<Props> = ({ api, onChange }) => {
  const [tables, setTables] = useState<TableOption[]>([]);
  const [leftFields, setLeftFields] = useState<FieldOption[]>([]);
  const [rightFields, setRightFields] = useState<FieldOption[]>([]);
  const [join, setJoin] = useState<JoinCondition>({
    leftDatasource: '',
    leftTable: '',
    leftField: '',
    rightDatasource: '',
    rightTable: '',
    rightField: '',
    joinType: 'INNER',
  });

  // Load all tables from all datasources
  useEffect(() => {
    api.datasources().then(async (datasources) => {
      const opts: TableOption[] = [];
      for (const ds of datasources) {
        try {
          const schemas = await api.getSchemas(ds.id);
          for (const s of schemas) {
            opts.push({ datasource: ds.id, table: s.name });
          }
        } catch {}
      }
      setTables(opts);
    });
  }, [api]);

  const loadFields = async (datasource: string, table: string, side: 'left' | 'right') => {
    if (!datasource || !table) return;
    try {
      const fields = await api.getFields(datasource, table);
      if (side === 'left') setLeftFields(fields);
      else setRightFields(fields);
    } catch {}
  };

  const updateJoin = (patch: Partial<JoinCondition>) => {
    const updated = { ...join, ...patch };
    setJoin(updated);

    if (patch.leftDatasource !== undefined || patch.leftTable !== undefined) {
      loadFields(updated.leftDatasource, updated.leftTable, 'left');
    }
    if (patch.rightDatasource !== undefined || patch.rightTable !== undefined) {
      loadFields(updated.rightDatasource, updated.rightTable, 'right');
    }

    // Emit SQL when all fields are set
    if (
      updated.leftDatasource && updated.leftTable && updated.leftField &&
      updated.rightDatasource && updated.rightTable && updated.rightField
    ) {
      onChange(buildJoinSQL(updated));
    }
  };

  const tableOptions = tables.map((t) => (
    <option key={`${t.datasource}.${t.table}`} value={`${t.datasource}|${t.table}`}>
      {t.datasource}.{t.table}
    </option>
  ));

  return (
    <div style={{ border: '1px solid #ddd', borderRadius: 4, padding: 16, marginBottom: 16 }}>
      <h3 style={{ margin: '0 0 12px', fontSize: 14 }}>Visual Join Builder</h3>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr auto 1fr', gap: 16, alignItems: 'start' }}>
        {/* Left side */}
        <div>
          <label style={labelStyle}>Left Table</label>
          <select
            style={selectStyle}
            value={join.leftTable ? `${join.leftDatasource}|${join.leftTable}` : ''}
            onChange={(e) => {
              const [ds, tbl] = e.target.value.split('|');
              updateJoin({ leftDatasource: ds, leftTable: tbl, leftField: '' });
            }}
          >
            <option value="">Select table...</option>
            {tableOptions}
          </select>
          <label style={labelStyle}>Join Field</label>
          <select
            style={selectStyle}
            value={join.leftField}
            onChange={(e) => updateJoin({ leftField: e.target.value })}
            disabled={!leftFields.length}
          >
            <option value="">Select field...</option>
            {leftFields.map((f) => (
              <option key={f.name} value={f.name}>{f.name} ({f.data_type})</option>
            ))}
          </select>
        </div>

        {/* Join type */}
        <div style={{ textAlign: 'center', paddingTop: 24 }}>
          <select
            style={{ ...selectStyle, width: 90 }}
            value={join.joinType}
            onChange={(e) => updateJoin({ joinType: e.target.value as JoinCondition['joinType'] })}
          >
            <option value="INNER">INNER</option>
            <option value="LEFT">LEFT</option>
            <option value="CROSS">CROSS</option>
          </select>
          <div style={{ fontSize: 20, marginTop: 4 }}>⟕</div>
        </div>

        {/* Right side */}
        <div>
          <label style={labelStyle}>Right Table</label>
          <select
            style={selectStyle}
            value={join.rightTable ? `${join.rightDatasource}|${join.rightTable}` : ''}
            onChange={(e) => {
              const [ds, tbl] = e.target.value.split('|');
              updateJoin({ rightDatasource: ds, rightTable: tbl, rightField: '' });
            }}
          >
            <option value="">Select table...</option>
            {tableOptions}
          </select>
          <label style={labelStyle}>Join Field</label>
          <select
            style={selectStyle}
            value={join.rightField}
            onChange={(e) => updateJoin({ rightField: e.target.value })}
            disabled={!rightFields.length}
          >
            <option value="">Select field...</option>
            {rightFields.map((f) => (
              <option key={f.name} value={f.name}>{f.name} ({f.data_type})</option>
            ))}
          </select>
        </div>
      </div>

      {/* Preview */}
      {join.leftField && join.rightField && (
        <div style={{ marginTop: 12, padding: 8, background: '#f5f5f5', borderRadius: 4, fontFamily: 'monospace', fontSize: 12 }}>
          {buildJoinSQL(join)}
        </div>
      )}
    </div>
  );
};

function buildJoinSQL(j: JoinCondition): string {
  return (
    `SELECT l.*, r.*\n` +
    `FROM ${j.leftDatasource}.${j.leftTable} AS l\n` +
    `${j.joinType} JOIN ${j.rightDatasource}.${j.rightTable} AS r\n` +
    `  ON l.${j.leftField} = r.${j.rightField}`
  );
}

const labelStyle: React.CSSProperties = {
  display: 'block',
  fontSize: 11,
  fontWeight: 600,
  marginBottom: 4,
  color: '#555',
};

const selectStyle: React.CSSProperties = {
  width: '100%',
  padding: '4px 8px',
  marginBottom: 8,
  fontSize: 13,
};
