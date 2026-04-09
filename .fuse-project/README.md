# .fuse-project — Project Management Hub

This directory is the single source of truth for project planning, requirements,
team coordination, and decision tracking. It's designed for both human and AI
agent consumption.

## Structure

```
.fuse-project/
├── requirements/       # What we're building (PRDs, specs, acceptance criteria)
├── backlog/            # Work items: todo, in-progress, done
├── decisions/          # Architecture Decision Records (ADRs)
├── sprints/            # Sprint plans and retrospectives
└── team/
    ├── agents/         # Agent profiles: who does what, current assignments
    └── skills/         # Reusable agent SOPs and skill definitions
```

## Conventions

- Backlog items use format: `NNN-short-name.md` (e.g., `001-fuse-core.md`)
- ADRs use format: `ADR-NNN-title.md`
- Sprint plans: `sprint-NN.md`
- Agent profiles: `<hive-name>.md`
- All files are Markdown for human + AI readability
