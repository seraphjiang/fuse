# Fuse Hive Team — Recovery & Operations Guide

How to rebuild the agent team if a hive session crashes or needs to be recreated.

## Quick Recovery

```bash
# 1. Start a new hive session
kiro-hive create xds

# 2. Spawn the team (4 agents)
kiro-hive spawn general          # Core types + OpenSearch connector
kiro-hive spawn metis::planner   # Engine architect (DataFusion)
kiro-hive spawn general:opus:explorer  # Researcher + API dev
kiro-hive spawn general          # You (sisyphus / coordinator) — or join existing pane

# 3. Onboard each agent with context
kiro-hive tell planner "$(cat .fuse-project/team/agents/planner.md)"
kiro-hive tell explorer "$(cat .fuse-project/team/agents/explorer.md)"
kiro-hive tell general "$(cat .fuse-project/team/agents/general.md)"

# 4. Share project state
kiro-hive broadcast "Read .fuse-project/backlog/backlog.md for current work items and .fuse-project/sprints/sprint-01.md for the active sprint. The repo is at ~/wss/fuse/"
```

## Team Composition

| Hive Name | Spawn Command | Role | Owns |
|-----------|--------------|------|------|
| sisyphus | (coordinator pane) | Lead / Coordinator | workspace root, .fuse-project/, git |
| planner | `kiro-hive spawn metis::planner` | Engine Architect | crates/fuse-engine/ |
| explorer | `kiro-hive spawn general:opus:explorer` | Researcher / API Dev | crates/fuse-server/ |
| general | `kiro-hive spawn general` | Core / Connector Dev | crates/fuse-core/, crates/fuse-connectors/ |

## Onboarding a New Agent

When spawning a replacement or additional agent:

1. Spawn: `kiro-hive spawn <type>::<hive-name>`
2. Send their agent profile: `kiro-hive tell <name> "$(cat .fuse-project/team/agents/<name>.md)"`
3. Send current sprint: `kiro-hive tell <name> "$(cat .fuse-project/sprints/sprint-01.md)"`
4. Assign work from backlog: `kiro-hive tell <name> "[HANDOFF] Pick up backlog item #NNN..."`
5. Create/update their agent profile in `.fuse-project/team/agents/<name>.md`
6. Update roster in `.fuse-project/team/roster.md`

## Onboarding Message Template

```
You are <HIVE_NAME>, working on the Fuse project — a Cross-Datasource Federated
Query Engine for OpenSearch Dashboards (Rust + DataFusion).

Repo: ~/wss/fuse/
GitHub: https://github.com/seraphjiang/fuse
Proposal: https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705

Your role: <ROLE>
Your crates: <CRATE_LIST>

Key files to read:
- .fuse-project/team/agents/<your-name>.md  (your profile)
- .fuse-project/backlog/backlog.md          (work items)
- .fuse-project/sprints/sprint-01.md        (current sprint)
- .fuse-project/requirements/phase-1-acceptance-criteria.md
- docs/design/connector-interface-and-query-parser.md

Build: source ~/.cargo/env && cd ~/wss/fuse && cargo check 2>&1 | tail -20

Report status to sisyphus using [DONE], [BLOCKED], [PROGRESS], [QUESTION].
```

## Session Health Checks

```bash
# Check who's alive
kiro-hive status

# If an agent is dead, kill and respawn
kiro-hive kill <agent>
kiro-hive cleanup              # remove all dead agents
kiro-hive spawn <type>::<name> # respawn with same hive name
# Then re-onboard (see above)
```

## Communication Protocol

| Situation | Command |
|-----------|---------|
| Assign work | `kiro-hive tell <agent> "[HANDOFF] ..."` |
| Check status | `kiro-hive tell <agent> "status?"` |
| Broadcast update | `kiro-hive broadcast "[PROGRESS] ..."` |
| Agent reports done | Agent sends `[DONE]` to sisyphus |
| Agent is stuck | Agent sends `[BLOCKED] reason` to sisyphus |
| Agent has question | Agent sends `[QUESTION] ...` to sisyphus |

## Status Tags

- `[DONE]` — task complete, ready for review
- `[BLOCKED]` — can't proceed, needs help
- `[PROGRESS]` — working, here's an update
- `[QUESTION]` — need clarification
- `[ERROR]` — something broke
- `[HANDOFF]` — assigning work to another agent

## Scaling the Team

To add a specialist agent (e.g., for S3 connector work in Phase 2):

```bash
# Spawn
kiro-hive spawn general::s3-dev

# Onboard with S3-specific context
kiro-hive tell s3-dev "[HANDOFF] You own crates/fuse-connectors/s3/.
Read .fuse-project/team/skills/skills-registry.md for required skills.
Read docs/design/connector-interface-and-query-parser.md for the connector trait.
Look at crates/fuse-connectors/opensearch/ as a reference implementation.
Implement the S3/Parquet connector (backlog #020)."

# Create agent profile
# Update .fuse-project/team/agents/s3-dev.md
# Update .fuse-project/team/roster.md
# Update .fuse-project/backlog/backlog.md (assign #020 to s3-dev)
```

## Disaster Recovery Checklist

If everything is lost and you're starting from scratch:

1. [ ] Clone repo: `git clone https://github.com/seraphjiang/fuse.git ~/wss/fuse`
2. [ ] Install Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y`
3. [ ] Install OpenSSL dev: `sudo yum install -y openssl-devel`
4. [ ] Verify build: `source ~/.cargo/env && cd ~/wss/fuse && cargo check`
5. [ ] Create hive session: `kiro-hive create xds`
6. [ ] Spawn team (see Quick Recovery above)
7. [ ] Read `.fuse-project/sprints/` for current sprint
8. [ ] Read `.fuse-project/backlog/backlog.md` for work status
9. [ ] Resume work
