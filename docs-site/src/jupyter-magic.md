# Jupyter Magic

Query Fuse directly from Jupyter notebooks with `%fuse` and `%%fuse` magic commands.

## Install

```bash
pip install fuse-client
```

## Setup

```python
%load_ext fuse_magic
```

Set the Fuse URL via environment variable or in-notebook:

```python
%env FUSE_URL=http://localhost:9400
```

## Usage

### Inline SQL

```python
%fuse SELECT service, count(*) FROM cluster_a.logs GROUP BY service
```

### Multi-line SQL

```python
%%fuse
SELECT l.service, u.name, count(*)
FROM cluster_a.logs l
JOIN dynamodb.users u ON l.user_id = u.user_id
WHERE l.status >= 500
GROUP BY l.service, u.name
ORDER BY count(*) DESC
LIMIT 10
```

### PPL

```python
%%fuse ppl
source = cluster_a.logs
| where status >= 500
| stats count() by service
| sort - count()
```

Results are automatically returned as a pandas DataFrame.
