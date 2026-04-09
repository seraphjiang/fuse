---
name: Bug Report
about: Report a bug or unexpected behavior
title: "[Bug] "
labels: bug
assignees: ''
---

## Description

A clear description of the bug.

## Steps to Reproduce

1. Start fuse-server with config: `...`
2. Run query: `curl -X POST .../api/fuse/query -d '...'`
3. Observe error

## Expected Behavior

What you expected to happen.

## Actual Behavior

What actually happened. Include error messages, HTTP status codes, or logs.

## Environment

- **OS**: (e.g., Ubuntu 22.04, macOS 14)
- **Rust version**: (`rustc --version`)
- **Fuse version/commit**: (`git rev-parse --short HEAD`)
- **OpenSearch version**: (if applicable)
- **Docker**: (yes/no, version)

## Logs

<details>
<summary>Server logs</summary>

```
Paste relevant fuse-server logs here
```

</details>

## Additional Context

Any other context — screenshots, config snippets, related issues.
