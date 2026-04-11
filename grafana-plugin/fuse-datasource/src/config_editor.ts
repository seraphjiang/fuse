// SPDX-License-Identifier: Apache-2.0

import React, { ChangeEvent } from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { InlineField, Input, SecretInput } from '@grafana/ui';
import { FuseDataSourceOptions } from './types';

type Props = DataSourcePluginOptionsEditorProps<FuseDataSourceOptions>;

export const FuseConfigEditor: React.FC<Props> = ({ options, onOptionsChange }) => {
  const { jsonData } = options;

  const onUrlChange = (e: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, url: e.target.value } });
  };

  const onApiKeyChange = (e: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, apiKey: e.target.value } });
  };

  const onApiKeyReset = () => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, apiKey: '' } });
  };

  const onTimeoutChange = (e: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, defaultTimeout: parseInt(e.target.value) || 0 } });
  };

  return (
    <div className="gf-form-group">
      <InlineField label="Fuse URL" labelWidth={14} tooltip="Base URL of the Fuse server">
        <Input width={40} value={jsonData.url || ''} onChange={onUrlChange} placeholder="http://localhost:9400" />
      </InlineField>
      <InlineField label="API Key" labelWidth={14} tooltip="Optional API key for authentication">
        <SecretInput width={40} isConfigured={!!jsonData.apiKey} value={jsonData.apiKey || ''} onChange={onApiKeyChange} onReset={onApiKeyReset} placeholder="fuse_..." />
      </InlineField>
      <InlineField label="Timeout (ms)" labelWidth={14} tooltip="Default query timeout in milliseconds">
        <Input width={20} type="number" value={jsonData.defaultTimeout || ''} onChange={onTimeoutChange} placeholder="30000" />
      </InlineField>
    </div>
  );
};
