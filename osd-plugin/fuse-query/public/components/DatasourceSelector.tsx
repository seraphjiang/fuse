// SPDX-License-Identifier: Apache-2.0

import React, { useState, useEffect } from 'react';
import { FuseApiService } from '../services/fuse_api';
import { DatasourceInfo } from '../../common';

interface Props {
  api: FuseApiService;
  selected: string;
  onChange: (id: string) => void;
}

export const DatasourceSelector: React.FC<Props> = ({ api, selected, onChange }) => {
  const [datasources, setDatasources] = useState<DatasourceInfo[]>([]);

  useEffect(() => {
    api.datasources().then(setDatasources).catch(() => setDatasources([]));
  }, [api]);

  return (
    <select value={selected} onChange={(e) => onChange(e.target.value)}>
      <option value="">All datasources</option>
      {datasources.map((ds) => (
        <option key={ds.id} value={ds.id}>
          {ds.id} ({ds.type})
        </option>
      ))}
    </select>
  );
};
