// SPDX-License-Identifier: Apache-2.0

import {
  CoreSetup,
  CoreStart,
  Plugin,
  Logger,
  PluginInitializerContext,
  IRouter,
} from '../../../src/core/server';
import { API_BASE } from '../common';

interface FuseQueryConfig {
  fuseServerUrl: string;
}

export class FuseQueryServerPlugin implements Plugin {
  private readonly logger: Logger;

  constructor(context: PluginInitializerContext) {
    this.logger = context.logger.get();
  }

  public setup(core: CoreSetup) {
    const router = core.http.createRouter();
    const fuseServerUrl = (core.http as any).config?.fuseServerUrl || 'http://localhost:9400';

    this.registerProxyRoutes(router, fuseServerUrl);
    this.logger.info(`Fuse Query plugin: proxying to ${fuseServerUrl}`);
  }

  public start(_core: CoreStart) {}

  public stop() {}

  private registerProxyRoutes(router: IRouter, fuseServerUrl: string) {
    // Health
    router.get(
      { path: `${API_BASE}/health`, validate: false },
      async (_context, _request, response) => {
        try {
          const resp = await fetch(`${fuseServerUrl}/api/fuse/health`);
          const body = await resp.json();
          return response.ok({ body });
        } catch (e: any) {
          return response.customError({ statusCode: 502, body: { message: e.message } });
        }
      }
    );

    // Datasources
    router.get(
      { path: `${API_BASE}/datasources`, validate: false },
      async (_context, _request, response) => {
        try {
          const resp = await fetch(`${fuseServerUrl}/api/fuse/datasources`);
          const body = await resp.json();
          return response.ok({ body });
        } catch (e: any) {
          return response.customError({ statusCode: 502, body: { message: e.message } });
        }
      }
    );

    // Query
    router.post(
      { path: `${API_BASE}/query`, validate: false },
      async (_context, request, response) => {
        try {
          const resp = await fetch(`${fuseServerUrl}/api/fuse/query`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request.body),
          });
          const body = await resp.json();
          return response.ok({ body });
        } catch (e: any) {
          return response.customError({ statusCode: 502, body: { message: e.message } });
        }
      }
    );

    // Validate
    router.post(
      { path: `${API_BASE}/query/validate`, validate: false },
      async (_context, request, response) => {
        try {
          const resp = await fetch(`${fuseServerUrl}/api/fuse/query/validate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request.body),
          });
          const body = await resp.json();
          return response.ok({ body });
        } catch (e: any) {
          return response.customError({ statusCode: 502, body: { message: e.message } });
        }
      }
    );

    // Explain
    router.post(
      { path: `${API_BASE}/query/explain`, validate: false },
      async (_context, request, response) => {
        try {
          const resp = await fetch(`${fuseServerUrl}/api/fuse/query/explain`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request.body),
          });
          const body = await resp.json();
          return response.ok({ body });
        } catch (e: any) {
          return response.customError({ statusCode: 502, body: { message: e.message } });
        }
      }
    );
  }
}
