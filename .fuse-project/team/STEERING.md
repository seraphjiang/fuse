# Team Steering Rules

Non-negotiable rules for all agents. Read before starting any work.

## Rule 1: Testing is #1 Priority — Never Skip Tests

**Every code change MUST have tests. No exceptions.**

- New feature → new tests covering happy path + edge cases
- Bug fix → regression test proving the fix
- Refactor → existing tests must still pass, add tests if coverage gaps found
- No PR/commit is considered "done" without tests

**Before reporting [DONE]:**
1. `cargo test --all-targets` must pass with ZERO failures
2. New code must have at least one test per public function/method
3. Edge cases must be covered (empty input, error paths, boundary values)
4. If you can't test it, explain why in your [DONE] message

**Tester agent has veto power.** If tester reports insufficient coverage on any commit, the owning agent must add tests before moving to the next task.

## Rule 2: Build Must Be Green

- `cargo check` — zero errors, zero warnings
- `cargo clippy` — no warnings
- Never commit code that breaks the build for other agents

## Rule 3: Don't Duplicate Work

- Before starting a task, check:
  1. `git status` — is someone else's uncommitted work in the same file?
  2. `.fuse-project/backlog/backlog.md` — is the item already done?
  3. Ask sisyphus if unsure
- If you find stale backlog entries, report to sisyphus — don't just pick them up

## Rule 4: Report Accurately

- `[DONE]` means: code written, tests passing, build clean
- `[PROGRESS]` means: actively working, here's what's done so far
- `[BLOCKED]` means: can't proceed, here's exactly what I need
- Don't report [DONE] if tests are missing or build has warnings

## Rule 5: Read Your Inbox

- Check for new [HANDOFF] messages before picking up backlog items
- Sisyphus may have already assigned you specific work
- If you have a pending assignment, do that first

## Rule 6: Environment Awareness

- Read `.fuse-project/ENVIRONMENT.md` for live URLs, AWS resources, API examples
- Playground: https://fuse.huanji.profile.aws.dev
- All pushes go to GitHub + CodeCommit (triggers pipeline)

## Rule 7: Self-Assign (Sprint 2+)

- When done with a task, pick the next unblocked item from the backlog yourself
- Do NOT wait for sisyphus to assign you work
- Update backlog status to `in-progress` when you start, `done` when complete
- Priority order: P0 blockers → test coverage gaps → community adoption → features

## Rule 8: Completion Protocol

Every [DONE] report MUST include:
1. Commit hash (from `git log --oneline -1`)
2. Backlog item(s) updated to `done`
3. Test count (before → after)

Format: `[DONE] #NNN — <description>. Commit: <hash>. Tests: N → M.`

## Rule 9: Heartbeat

- If working on a task for 30+ minutes, send a [HEARTBEAT] to sisyphus
- Include: what's done, what's in progress, any blockers
- This prevents duplicate work and lets sisyphus re-route if needed
