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

export class FuseQueryServerPlugin implements Plugin {
  private readonly logger: Logger;
  private readonly fuseEngineUrl: string;

  constructor(context: PluginInitializerContext) {
    this.logger = context.logger.get();
    // Read from env — set FUSE_ENGINE_URL in OSD's environment
    this.fuseEngineUrl = process.env.FUSE_ENGINE_URL || 'http://fuse-server:9400';
  }

  public setup(core: CoreSetup) {
    const router = core.http.createRouter();
    this.registerProxyRoutes(router);
    this.logger.info(`Fuse Query plugin: proxying to ${this.fuseEngineUrl}`);
  }

  public start(_core: CoreStart) {}
  public stop() {}

  private registerProxyRoutes(router: IRouter) {
    const proxy = async (path: string, method: 'GET' | 'POST' | 'DELETE', body?: unknown) => {
      const url = `${this.fuseEngineUrl}/api/fuse${path}`;
      const opts: RequestInit = {
        method,
        headers: { 'Content-Type': 'application/json' },
      };
      if (body) opts.body = JSON.stringify(body);
      const resp = await fetch(url, opts);
      return resp.json();
    };

    router.get({ path: `${API_BASE}/health`, validate: false }, async (_ctx, _req, res) => {
      try { return res.ok({ body: await proxy('/health', 'GET') }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.get({ path: `${API_BASE}/datasources`, validate: false }, async (_ctx, _req, res) => {
      try { return res.ok({ body: await proxy('/datasources', 'GET') }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.get({ path: `${API_BASE}/datasources/{id}/schemas`, validate: false }, async (_ctx, req, res) => {
      try { return res.ok({ body: await proxy(`/datasources/${(req.params as any).id}/schemas`, 'GET') }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.get({ path: `${API_BASE}/datasources/{id}/schemas/{table}/fields`, validate: false }, async (_ctx, req, res) => {
      const { id, table } = req.params as any;
      try { return res.ok({ body: await proxy(`/datasources/${id}/schemas/${table}/fields`, 'GET') }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.post({ path: `${API_BASE}/query`, validate: false }, async (_ctx, req, res) => {
      try { return res.ok({ body: await proxy('/query', 'POST', req.body) }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.post({ path: `${API_BASE}/query/validate`, validate: false }, async (_ctx, req, res) => {
      try { return res.ok({ body: await proxy('/query/validate', 'POST', req.body) }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.post({ path: `${API_BASE}/query/explain`, validate: false }, async (_ctx, req, res) => {
      try { return res.ok({ body: await proxy('/query/explain', 'POST', req.body) }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.get({ path: `${API_BASE}/history`, validate: false }, async (_ctx, _req, res) => {
      try { return res.ok({ body: await proxy('/history', 'GET') }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    // v1.1 API routes

    router.get({ path: `${API_BASE}/trace/{traceId}`, validate: false }, async (_ctx, req, res) => {
      try { return res.ok({ body: await proxy(`/trace/${(req.params as any).traceId}`, 'GET') }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.get({ path: `${API_BASE}/federation`, validate: false }, async (_ctx, _req, res) => {
      try { return res.ok({ body: await proxy('/federation', 'GET') }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.get({ path: `${API_BASE}/stats`, validate: false }, async (_ctx, _req, res) => {
      try { return res.ok({ body: await proxy('/stats', 'GET') }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.get({ path: `${API_BASE}/saved-queries`, validate: false }, async (_ctx, _req, res) => {
      try { return res.ok({ body: await proxy('/saved-queries', 'GET') }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.post({ path: `${API_BASE}/saved-queries`, validate: false }, async (_ctx, req, res) => {
      try { return res.ok({ body: await proxy('/saved-queries', 'POST', req.body) }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });

    router.delete({ path: `${API_BASE}/saved-queries/{name}`, validate: false }, async (_ctx, req, res) => {
      try { return res.ok({ body: await proxy(`/saved-queries/${(req.params as any).name}`, 'DELETE') }); }
      catch (e: any) { return res.customError({ statusCode: 502, body: { message: e.message } }); }
    });
  }
}
