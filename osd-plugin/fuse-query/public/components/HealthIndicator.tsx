// SPDX-License-Identifier: Apache-2.0

import React, { useState, useEffect } from 'react';
import { FuseApiService } from '../services/fuse_api';

interface Props {
  api: FuseApiService;
}

export const HealthIndicator: React.FC<Props> = ({ api }) => {
  const [healthy, setHealthy] = useState<boolean | null>(null);

  useEffect(() => {
    const check = () => {
      api
        .health()
        .then((h) => setHealthy(h.status === 'ok'))
        .catch(() => setHealthy(false));
    };
    check();
    const interval = setInterval(check, 30000);
    return () => clearInterval(interval);
  }, [api]);

  const color = healthy === null ? '#999' : healthy ? '#4caf50' : '#f44336';
  const label = healthy === null ? 'Checking...' : healthy ? 'Connected' : 'Disconnected';

  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontSize: 12 }}>
      <span
        style={{
          width: 8,
          height: 8,
          borderRadius: '50%',
          backgroundColor: color,
          display: 'inline-block',
        }}
        role="status"
        aria-label={`Fuse engine status: ${label}`}
      />
      {label}
    </span>
  );
};
