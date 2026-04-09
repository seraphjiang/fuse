# Hive Team — Agent Roster

## Active Agents (Session: xds)

| Hive Name | Role | Crate Ownership | Skills |
|-----------|------|-----------------|--------|
| sisyphus | Lead / Coordinator | workspace root, project mgmt | Synthesis, delegation, integration, git |
| planner | Engine Architect | fuse-engine | Rust, DataFusion, query planning, optimization |
| explorer | Researcher / API Dev | fuse-server | Ecosystem research, REST API design, axum |
| general | Core / Connector Dev | fuse-core, fuse-connector-opensearch | Rust traits, OpenSearch, type systems |
| infra | Infrastructure / DevOps | Dockerfile, CI/CD, AWS | AWS, Docker, CodePipeline, ECS, ALB |

## Communication Protocol

- Agents report status to sisyphus using tags: `[DONE]`, `[BLOCKED]`, `[PROGRESS]`, `[QUESTION]`, `[ERROR]`
- sisyphus distributes work via `[HANDOFF]` messages
- Agents can communicate directly for technical questions
- All design decisions go through sisyphus for ratification

## Escalation Path

1. Agent is blocked → report `[BLOCKED]` to sisyphus with details
2. Sisyphus unblocks or reassigns
3. If sisyphus can't resolve → escalate to user
