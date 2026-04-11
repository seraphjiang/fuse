# Natural Language Queries

Ask questions in plain English and Fuse generates SQL using an LLM backend.

## Usage

```bash
curl -X POST http://localhost:9400/api/fuse/query/nl \
  -H 'Content-Type: application/json' \
  -d '{"question": "Show me the top 10 error-producing services in the last hour"}'
```

Response:
```json
{
  "generated_sql": "SELECT service, count(*) as error_count FROM cluster_a.logs WHERE status >= 500 AND timestamp > NOW() - INTERVAL '1 hour' GROUP BY service ORDER BY error_count DESC LIMIT 10",
  "columns": [...],
  "rows": [...]
}
```

## Configuration

Configure the LLM backend in `fuse.toml`:

```toml
[engine.nl]
backend = "openai"           # "openai" or "bedrock"
model = "gpt-4"
api_key = "secret://openai-key"
```

## How It Works

1. Fuse discovers schemas from all configured datasources
2. Builds a schema-aware prompt with table names, column names, and types
3. Sends the prompt + user question to the LLM
4. Extracts SQL from the response (strips markdown fences)
5. Executes the generated SQL and returns results

## Playground

The playground has a natural language input mode — click the 🗣️ icon next to the format selector.
