// SPDX-License-Identifier: Apache-2.0
// Tests for fuse-client TypeScript SDK (no server required)

import assert from 'node:assert/strict';
import { FuseClient, FuseError } from '../src/index.ts';

// ── Mock fetch ──
function mockFetch(status, body) {
  return async () => ({
    ok: status >= 200 && status < 300,
    status,
    text: async () => JSON.stringify(body),
    json: async () => body,
  });
}

// ── Tests ──
console.log('Running fuse-client tests...\n');
let passed = 0;

function test(name, fn) {
  try { fn(); passed++; console.log(`  ✓ ${name}`); }
  catch (e) { console.error(`  ✗ ${name}: ${e.message}`); process.exit(1); }
}

async function testAsync(name, fn) {
  try { await fn(); passed++; console.log(`  ✓ ${name}`); }
  catch (e) { console.error(`  ✗ ${name}: ${e.message}`); process.exit(1); }
}

test('client defaults', () => {
  const c = new FuseClient();
  assert.equal(c['baseUrl'], 'http://localhost:3000');
  assert.equal(c['apiKey'], undefined);
});

test('client custom options', () => {
  const c = new FuseClient({ baseUrl: 'http://fuse:8080/', apiKey: 'key-1' });
  assert.equal(c['baseUrl'], 'http://fuse:8080');
  assert.equal(c['apiKey'], 'key-1');
});

test('headers without key', () => {
  const c = new FuseClient();
  const h = c['headers']();
  assert.equal(h['Content-Type'], 'application/json');
  assert.equal(h['x-api-key'], undefined);
});

test('headers with key', () => {
  const c = new FuseClient({ apiKey: 'abc' });
  const h = c['headers']();
  assert.equal(h['x-api-key'], 'abc');
});

test('FuseError', () => {
  const e = new FuseError(401, 'unauthorized');
  assert.equal(e.statusCode, 401);
  assert.equal(e.name, 'FuseError');
  assert.ok(e.message.includes('401'));
});

await testAsync('query parses response', async () => {
  const c = new FuseClient({
    fetch: mockFetch(200, {
      columns: ['a', 'b'],
      rows: [[1, 2]],
      metadata: { total_rows: 1, format: 'sql', trace_id: 't1', datasources_queried: ['ds1'] },
      next_cursor: 'fuse_c_1',
    }),
  });
  const r = await c.query('SELECT 1');
  assert.deepEqual(r.columns, ['a', 'b']);
  assert.equal(r.totalRows, 1);
  assert.equal(r.traceId, 't1');
  assert.equal(r.nextCursor, 'fuse_c_1');
  assert.deepEqual(r.datasourcesQueried, ['ds1']);
});

await testAsync('trace parses response', async () => {
  const c = new FuseClient({
    fetch: mockFetch(200, {
      trace_id: 'abc', spans: [{ datasource: 'ds1' }],
      datasources_searched: ['ds1', 'ds2'], datasources_matched: ['ds1'],
      total_spans: 1, search_ms: 5,
    }),
  });
  const t = await c.trace('abc');
  assert.equal(t.traceId, 'abc');
  assert.equal(t.totalSpans, 1);
  assert.equal(t.datasourcesMatched.length, 1);
});

await testAsync('error throws FuseError', async () => {
  const c = new FuseClient({ fetch: mockFetch(500, { error: 'boom' }) });
  await assert.rejects(() => c.health(), (e) => e instanceof FuseError && e.statusCode === 500);
});

await testAsync('health returns response', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, { status: 'ok', connectors: {} }) });
  const h = await c.health();
  assert.equal(h.status, 'ok');
});


await testAsync('savedQueries returns list', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, [{ name: 'q1', query: 'SELECT 1' }]) });
  const saved = await c.savedQueries();
  assert.equal(saved.length, 1);
  assert.equal(saved[0].name, 'q1');
});

await testAsync('saveQuery sends body', async () => {
  let sentBody;
  const c = new FuseClient({
    fetch: async (url, opts) => {
      sentBody = JSON.parse(opts.body);
      return { ok: true, status: 200, json: async () => ({ ok: true }), text: async () => '{}' };
    },
  });
  await c.saveQuery('myq', 'SELECT 1', 'test query');
  assert.equal(sentBody.name, 'myq');
  assert.equal(sentBody.query, 'SELECT 1');
});

await testAsync('deleteSavedQuery calls DELETE', async () => {
  let method;
  const c = new FuseClient({
    fetch: async (url, opts) => {
      method = opts.method;
      return { ok: true, status: 200, json: async () => ({ ok: true }), text: async () => '{}' };
    },
  });
  await c.deleteSavedQuery('myq');
  assert.equal(method, 'DELETE');
});


await testAsync('submitAsync returns jobId', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, { job_id: 'j-123' }) });
  const resp = await c.submitAsync('SELECT 1');
  assert.equal(resp.jobId, 'j-123');
});

await testAsync('pollAsync returns status', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, { job_id: 'j-123', status: 'completed', result: {} }) });
  const resp = await c.pollAsync('j-123');
  assert.equal(resp.status, 'completed');
});

await testAsync('cancelAsync calls DELETE', async () => {
  let method;
  const c = new FuseClient({
    fetch: async (url, opts) => {
      method = opts.method;
      return { ok: true, status: 200, json: async () => ({ ok: true }), text: async () => '{}' };
    },
  });
  await c.cancelAsync('j-123');
  assert.equal(method, 'DELETE');
});

// ── Sprint 18: Webhooks ──

await testAsync('webhooks returns list', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, [{ id: 'w1', name: 'alert' }]) });
  const ws = await c.webhooks();
  assert.equal(ws.length, 1);
  assert.equal(ws[0].id, 'w1');
});

await testAsync('createWebhook sends body', async () => {
  let sentBody;
  const c = new FuseClient({
    fetch: async (url, opts) => {
      sentBody = JSON.parse(opts.body);
      return { ok: true, status: 200, json: async () => ({ id: 'w-new' }), text: async () => '{}' };
    },
  });
  const resp = await c.createWebhook('alert', 'SELECT count(*) FROM ds.logs', { row_count_gt: 100 }, 'https://hook.example.com');
  assert.equal(resp.id, 'w-new');
  assert.equal(sentBody.name, 'alert');
  assert.equal(sentBody.callback_url, 'https://hook.example.com');
  assert.deepEqual(sentBody.condition, { row_count_gt: 100 });
});

await testAsync('deleteWebhook calls DELETE', async () => {
  let method, url;
  const c = new FuseClient({
    fetch: async (u, opts) => {
      method = opts.method; url = u;
      return { ok: true, status: 200, json: async () => ({ ok: true }), text: async () => '{}' };
    },
  });
  await c.deleteWebhook('w-1');
  assert.equal(method, 'DELETE');
  assert.ok(url.endsWith('/api/fuse/webhooks/w-1'));
});

await testAsync('testWebhook returns fired status', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, { fired: true, row_count: 42 }) });
  const resp = await c.testWebhook('w-1');
  assert.equal(resp.fired, true);
  assert.equal(resp.row_count, 42);
});

// ── Sprint 18: Relationships ──

await testAsync('relationships returns list', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, [{ left_datasource: 'a', right_datasource: 'b', confidence: 0.8 }]) });
  const rels = await c.relationships();
  assert.equal(rels.length, 1);
  assert.equal(rels[0].confidence, 0.8);
});

// ── Sprint 18: CDC ──

await testAsync('cdcStatus returns status', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, { enabled: true, tracked_views: 3 }) });
  const s = await c.cdcStatus();
  assert.equal(s.enabled, true);
});

await testAsync('cdcEvent sends body', async () => {
  let sentBody;
  const c = new FuseClient({
    fetch: async (url, opts) => {
      sentBody = JSON.parse(opts.body);
      return { ok: true, status: 200, json: async () => ({ accepted: true, affected_views: ['v1'] }), text: async () => '{}' };
    },
  });
  const resp = await c.cdcEvent('ds1', 'users', 'update');
  assert.equal(resp.accepted, true);
  assert.equal(sentBody.datasource, 'ds1');
  assert.equal(sentBody.table, 'users');
  assert.equal(sentBody.change_type, 'update');
  assert.ok(typeof sentBody.timestamp === 'number');
});

// ── Sprint 18: Predict ──

await testAsync('predict returns estimate', async () => {
  let url;
  const c = new FuseClient({
    fetch: async (u, opts) => {
      url = u;
      return { ok: true, status: 200, json: async () => ({ estimated_ms: 150, confidence: 'medium' }), text: async () => '{}' };
    },
  });
  const p = await c.predict('SELECT * FROM ds.logs');
  assert.equal(p.estimated_ms, 150);
  assert.equal(p.confidence, 'medium');
  assert.ok(url.includes('/api/fuse/predict?query='));
});

// ── Untested core methods ──

await testAsync('queryAll paginates automatically', async () => {
  let callCount = 0;
  const c = new FuseClient({
    fetch: async (url, opts) => {
      callCount++;
      const body = JSON.parse(opts.body);
      if (!body.cursor) {
        return { ok: true, status: 200, json: async () => ({
          columns: ['x'], rows: [[1], [2]], next_cursor: 'c2',
          metadata: { total_rows: 2, format: 'sql', trace_id: 't1' },
        }), text: async () => '{}' };
      }
      return { ok: true, status: 200, json: async () => ({
        columns: ['x'], rows: [[3]],
        metadata: { total_rows: 1, format: 'sql', trace_id: 't1' },
      }), text: async () => '{}' };
    },
  });
  const r = await c.queryAll('SELECT x FROM ds.t', { pageSize: 2 });
  assert.equal(r.rows.length, 3);
  assert.equal(callCount, 2);
  assert.equal(r.nextCursor, undefined);
});

await testAsync('explain returns plan', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, { plan: 'Scan: ds.logs', plan_tree: { type: 'scan' } }) });
  const e = await c.explain('SELECT * FROM ds.logs');
  assert.equal(e.plan, 'Scan: ds.logs');
  assert.ok(e.plan_tree);
});

await testAsync('validate valid query', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, { valid: true }) });
  const v = await c.validate('SELECT 1');
  assert.equal(v.valid, true);
});

await testAsync('validate invalid query', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, { valid: false, error: 'syntax error' }) });
  const v = await c.validate('SELEC 1');
  assert.equal(v.valid, false);
  assert.equal(v.error, 'syntax error');
});

await testAsync('datasources returns list', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, [{ id: 'ds1', connector_type: 'opensearch' }]) });
  const ds = await c.datasources();
  assert.equal(ds.length, 1);
  assert.equal(ds[0].connector_type, 'opensearch');
});

await testAsync('history returns list', async () => {
  const c = new FuseClient({ fetch: mockFetch(200, [{ query: 'SELECT 1', latency_ms: 10 }]) });
  const h = await c.history();
  assert.equal(h.length, 1);
  assert.equal(h[0].query, 'SELECT 1');
});

await testAsync('query with PPL format', async () => {
  let sentBody;
  const c = new FuseClient({
    fetch: async (url, opts) => {
      sentBody = JSON.parse(opts.body);
      return { ok: true, status: 200, json: async () => ({
        columns: ['x'], rows: [[1]], metadata: { total_rows: 1, format: 'ppl', trace_id: 't1' },
      }), text: async () => '{}' };
    },
  });
  await c.query('source = ds.logs | head 10', { format: 'ppl' });
  assert.equal(sentBody.format, 'ppl');
});

await testAsync('query with cursor', async () => {
  let sentBody;
  const c = new FuseClient({
    fetch: async (url, opts) => {
      sentBody = JSON.parse(opts.body);
      return { ok: true, status: 200, json: async () => ({
        columns: ['x'], rows: [[1]], metadata: { total_rows: 1, format: 'sql', trace_id: 't1' },
      }), text: async () => '{}' };
    },
  });
  await c.query('SELECT * FROM ds.t', { pageSize: 10, cursor: 'fuse_c_abc' });
  assert.equal(sentBody.page_size, 10);
  assert.equal(sentBody.cursor, 'fuse_c_abc');
});

await testAsync('error includes status and body', async () => {
  const c = new FuseClient({ fetch: mockFetch(422, { error: 'invalid query' }) });
  try {
    await c.query('BAD');
    assert.fail('should have thrown');
  } catch (e) {
    assert.ok(e instanceof FuseError);
    assert.equal(e.statusCode, 422);
    assert.ok(e.body.includes('invalid query'));
  }
});

console.log(`\n${passed} tests passed.`);
