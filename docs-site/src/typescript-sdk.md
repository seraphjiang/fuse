# TypeScript SDK Quick Start

Query Fuse from Node.js, Deno, or the browser in 5 minutes.

## Install

```bash
# From the repo
cd sdk/typescript
npm install
npm run build

# In your project
npm install ../path/to/sdk/typescript
```

## Connect

```typescript
import { FuseClient } from 'fuse-client';

// Local
const fuse = new FuseClient({ baseUrl: 'http://localhost:9400' });

// With API key
const fuse = new FuseClient({
  baseUrl: 'https://fuse.huanji.profile.aws.dev',
  apiKey: 'your-key',
});
```

## Query

```typescript
const result = await fuse.query(
  'SELECT service, count(*) as errors FROM cluster_a.application_logs WHERE status >= 500 GROUP BY service ORDER BY errors DESC LIMIT 5'
);

console.log(result.columns);   // ['service', 'errors']
console.log(result.rows);      // [['api-gateway', 42], ...]
console.log(result.totalRows); // 5
```

### PPL

```typescript
const result = await fuse.query(
  'source = cluster_a.application_logs | where status >= 500 | stats count() by service',
  { format: 'ppl' }
);
```

### Parameters

```typescript
const result = await fuse.query(
  'SELECT * FROM cluster_a.application_logs WHERE service = $svc LIMIT $n',
  { params: { svc: 'api-gateway', n: 10 } }
);
```

## Paginate

```typescript
// Page by page
const page1 = await fuse.query(
  'SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC',
  { pageSize: 50 }
);
console.log(`${page1.rows.length} rows, cursor: ${page1.nextCursor}`);

const page2 = await fuse.query(
  'SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC',
  { pageSize: 50, cursor: page1.nextCursor }
);

// Or fetch all pages
const all = await fuse.queryAll(
  'SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC',
  { pageSize: 500 }
);
console.log(`${all.totalRows} total rows`);
```

## Trace

```typescript
const trace = await fuse.trace('trace-001');

console.log(`${trace.totalSpans} spans from ${trace.datasourcesMatched} in ${trace.searchMs}ms`);
for (const span of trace.spans) {
  console.log(`  [${span.datasource}] ${span.timestamp} — ${span.fields.service}`);
}
```

## Explore

```typescript
// Health
const health = await fuse.health();
console.log(health.status);

// Datasources
const sources = await fuse.datasources();
sources.forEach(ds => console.log(`  ${ds.id} (${ds.connector_type})`));

// Explain
const plan = await fuse.explain('SELECT * FROM cluster_a.application_logs LIMIT 10');
console.log(plan.plan);

// Validate
const valid = await fuse.validate('SELECT * FROM cluster_a.application_logs');
console.log(valid.valid); // true
```

## Error Handling

```typescript
import { FuseError } from 'fuse-client';

try {
  await fuse.query('SELECT * FROM nonexistent.table');
} catch (e) {
  if (e instanceof FuseError) {
    console.error(`HTTP ${e.statusCode}: ${e.body}`);
  }
}
```

## Node.js (CommonJS)

```javascript
const { FuseClient } = require('fuse-client');
const fuse = new FuseClient({ baseUrl: 'http://localhost:9400' });

fuse.query('SELECT * FROM cluster_a.application_logs LIMIT 5')
  .then(r => console.log(r.rows));
```

## Next Steps

- [API Reference](api-reference-guide.md) — all endpoints
- [SQL Reference](https://seraphjiang.github.io/fuse/sql-reference.html) — JOINs, UNION, CTEs
- [Python SDK](python-sdk-quickstart.md) — Python equivalent
