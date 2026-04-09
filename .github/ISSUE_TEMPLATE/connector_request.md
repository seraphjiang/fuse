---
name: Connector Request
about: Request or propose a new datasource connector
title: "[Connector] "
labels: connector
assignees: ''
---

## Datasource

- **Type**: (e.g., PostgreSQL, Prometheus, MongoDB, Kafka)
- **Protocol**: (e.g., REST API, native driver, gRPC)
- **Docs**: (link to the datasource's query/API documentation)

## Use Case

Why do you need this connector? What queries would you run across this
datasource and existing ones?

## Authentication

What auth methods does this datasource support?
- [ ] No auth
- [ ] Basic (username/password)
- [ ] API key / Bearer token
- [ ] AWS SigV4
- [ ] mTLS / certificates
- [ ] Other: ___

## Example Queries

Show example queries you'd like to run through Fuse:

```sql
-- Cross-source join
SELECT o.order_id, l.message
FROM opensearch_cluster.logs l
JOIN postgres_db.orders o ON l.order_id = o.id
WHERE l.status = 500
```

## Capabilities

Which push-down operations does this datasource support natively?
- [ ] Filtering (WHERE)
- [ ] Projection (SELECT columns)
- [ ] Aggregation (COUNT, SUM, AVG)
- [ ] Sorting (ORDER BY)
- [ ] Limit
- [ ] Joins
- [ ] Streaming / cursors

## Willingness to Contribute

- [ ] I'd like to implement this connector myself
- [ ] I'd like help implementing it
- [ ] I'm requesting it — someone else would need to build it

## Additional Context

Links, related issues, or anything else relevant.
