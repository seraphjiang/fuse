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

## Priority Rule (added Sprint 1)
1. Check inbox FIRST — directed tasks from lead override everything
2. Then self-assign from backlog
3. P0 from lead > backlog self-assign > think-big picks

---

## Sprint 1 Retro Additions (Rules 10-14)

## Rule 10: Check Inbox Before EVERY Self-Assign
Before picking up new work:
1. Check inbox for PM directives or P0/P1 overrides
2. If an override exists, drop current self-assignment immediately
3. Acknowledge receipt: `[ACK] Dropping X, starting Y per PM directive`
4. Failure to check inbox = performance downgrade

## Rule 11: File Ownership During Active Work
- Announce when editing shared files: `[WORKING] api.rs`
- Other agents MUST NOT edit that file until `[DONE]` or `[RELEASED]`
- Shared files (Cargo.toml, main.rs, api.rs) require coordination via sisyphus
- If you need to edit a file another agent owns, ask sisyphus first

## Rule 12: Single Owner Per Artifact
| Artifact | Owner |
|----------|-------|
| OpenAPI spec | explorer |
| fuse.toml | infra |
| playground/index.html | explorer (UI), general (features — coordinate) |
| STEERING.md | PM |
| backlog.md | PM |
| docs-site/ | explorer |
| DEPLOYMENT.md | infra |

Don't update artifacts you don't own. If you see a gap, tell the owner.

## Rule 13: Coverage Target, Not Coverage Sweep
- Test all public functions with non-trivial logic
- SKIP: Display impls, trivial getters, derive-generated code, simple constructors
- When PM says "coverage complete, shift to features" — obey immediately
- Max 30 min on coverage sweeps before checking in with PM

## Rule 14: Sprint Backlog Required
- Agents self-assign FROM the backlog only
- Ad-hoc features (not in backlog) require PM approval
- If backlog is empty, ask PM for Sprint N+1 items — don't freelance

## Sprint 2 Retro Additions (Rules 15-17)

## Rule 15: Verify Gap Before Starting
Before implementing a backlog item, spend 2 min checking if it's already done:
1. `grep` for the feature in codebase
2. Run relevant tests
3. If already implemented, report `[ALREADY DONE]` and pick next item

## Rule 16: Idle Agent Protocol
If no work for 5+ minutes:
1. Check inbox
2. Check backlog for unassigned items
3. If nothing, report `[IDLE]` to sisyphus
4. Sisyphus will assign or approve stand-by

## Rule 17: Shared File Coordination (Strengthened)
api.rs, main.rs, Cargo.toml, lib.rs are HIGH-CONTENTION files:
- Announce `[WORKING] filename` before editing
- Other agents MUST wait for `[RELEASED]` before touching
- If urgent, ask sisyphus to coordinate handoff

## Sprint 3 Retro Additions (Rules 18-19)

## Rule 18: Pre-Commit Build Check
Before committing, MUST run `cargo check` and `cargo test` on affected crates. Do NOT commit code that breaks the build for other agents. If build fails, fix before committing.

## Rule 19: Fix All Instances
When fixing a bug, grep the entire codebase for the same pattern. If the same bug exists in another code path, fix ALL instances in the same commit. The scroll API bug (UNION ALL fixed but JOIN had the identical issue) must not repeat.
