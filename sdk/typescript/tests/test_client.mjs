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

console.log(`\n${passed} tests passed.`);
