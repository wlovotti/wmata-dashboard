---
description: Drive one full NOTES.md cycle — pick task, dispatch a subagent to implement+test+PR, watch CI, prompt for merge. Designed to run under `/loop /notes-cycle` for back-to-back cycles without parent-context rot.
---

This command drives one iteration of the NOTES.md cycle: read NOTES.md →
confirm next task → dispatch a subagent to implement → watch CI → prompt
for merge → cleanup. Two human checkpoints (task confirmation, merge
approval). Everything else autonomous.

The skill is composable: run it standalone for one cycle, or under
`/loop /notes-cycle` for autonomous back-to-back cycles. Under `/loop`
the parent context grows by only the subagent's short summary per
iteration — the heavy implementation transcript stays in the subagent
and dies with it.

# Step 1 — Pre-flight (parent does this directly)

Confirm the working tree is ready:

```bash
git status --porcelain  # must be empty
git branch --show-current  # must be `main`
git pull --ff-only
```

If any of these fail (dirty tree, on a feature branch, pull conflict):
**STOP**. Tell the user what's wrong and let them resolve before
re-running. Do not auto-stash or auto-checkout — those are destructive
shortcuts for problems we should investigate.

# Step 2 — Read state (parent does this directly)

Read `NOTES.md`. Identify each open item by its priority tier:

- **P4 — Surface to API + UI**
- **P5 — Cleanup**
- **Independent of the redesign**

For each item, note the severity tag and dependency line. An item is
**unblocked** if its `### Dependencies` section says "Independent" or
references only items already removed. An item is **blocked** if it
depends on a still-open `NOTES-N`.

# Step 3 — Propose & confirm next task (HUMAN CHECKPOINT #1)

Pick a recommended next task. Selection rule of thumb:

1. Prefer **unblocked** items
2. Among unblocked, prefer **higher priority tier** (P4 > P5 > Independent)
3. Among same tier, prefer **higher severity** (medium > low)
4. Tiebreak on **smallest scope** (favor closing items quickly)

Pick up to 2 alternates with different scope/risk profiles.

Ask via `AskUserQuestion`:

- Question: "Which NOTES item should this cycle close?"
- Header: "NOTES item"
- Options: recommended (with "(Recommended)" suffix) + 2 alternates
- For each option, the description must include: priority tier,
  severity, dependency status, and your scope estimate (small / medium
  / large)

The user may pick the recommended, an alternate, or "Other" with a
different NOTES-N. Capture the chosen NOTES-N for the rest of the cycle.

# Step 4 — Dispatch subagent for implementation

Invoke the `Agent` tool with `subagent_type: "general-purpose"` and a
self-contained prompt. The subagent does the heavy lifting; its
context dies on return so the parent stays slim under `/loop`.

Subagent prompt template (fill in `{{NOTES-N}}` and `{{section_summary}}`
from what you read in step 2):

```
You are closing NOTES-{{N}} from /Users/wlovotti/repos/wmata-dashboard/NOTES.md
in one PR. Here is the item's section verbatim:

{{section_summary}}

Repo conventions (from CLAUDE.md):
- PostgreSQL only; never propose SQLite fallbacks
- Pre-computed aggregations are the architectural rule
- Datetime storage is naive UTC; use src/timezones.py for service-date math
- GTFS queries must filter is_current=True
- stop_id is not direction-unique; group by (route_id, direction_id, stop_id)
- WMATA API budget: 10 calls/sec, 50k/day

Git policy (from global CLAUDE.md):
- Never commit to main directly
- Branch naming: feature/ fix/ docs/ refactor/

Your task:

1. Create a feature branch named after this item, e.g.
   `feature/notes-{{N}}-<short-slug>` (use feature/ fix/ docs/ or
   refactor/ as appropriate).

2. Implement the change. Follow the item's "Implementation" or
   "Remaining work" section if present. Keep scope tight — don't
   refactor adjacent code.

3. If you discover a side effect or new issue worth tracking, ADD a
   new NOTES-N item to NOTES.md in the SAME edit session, using the
   next sequential number after the current max. Don't open a second
   PR.

4. Run verification:
     uv run pytest -m smoke
     uv run ruff check src/ scripts/ api/ pipelines/
     uv run ruff format --check src/ scripts/ api/ pipelines/
   Fix failures and re-run until clean. Run the broader test suite
   (`uv run pytest`) if the change touches anything beyond a single
   small surface.

5. Fold the NOTES.md edits into the PR by following the procedure
   in /Users/wlovotti/repos/wmata-dashboard/.claude/commands/update-notes-in-pr.md
   (do NOT invoke a separate slash command — just follow the markdown's
   procedure inline so this stays one subagent invocation).

6. Commit, push, and open the PR. Title style per recent merged PRs:
   `feat:` / `fix:` / `docs:` / `refactor:` prefix, short summary,
   parenthetical NOTES reference where appropriate. Body must explain
   *why* the change was made — the body becomes the durable record once
   NOTES-{{N}} is deleted from NOTES.md.

7. Return ONLY:
   - PR_NUMBER: <int>
   - PR_URL: <url>
   - SUMMARY: one paragraph, what changed and what verification ran
   - NEW_NOTES: list of new NOTES-N items added (if any), or "none"

If you hit architectural ambiguity that needs a human decision,
STOP and return:
   - STATUS: needs_user
   - QUESTION: <what you need decided>
Do NOT guess — the parent will route the question.
```

Capture the subagent's return value. If it returned `STATUS:
needs_user`, route the question to the user via `AskUserQuestion` and
re-dispatch with the answer. If it returned a PR number, continue to
step 5.

# Step 5 — Watch CI (parent does this directly)

Stream CI checks for the PR:

```bash
gh pr checks <PR_NUMBER> --watch
```

If `--watch` is unavailable in this gh version, fall back to a brief
poll loop with `gh pr checks <PR_NUMBER> --json state,name,bucket`.

On **all green**: continue to step 6.

On **any failure**: surface the failed check name and a short excerpt
of its log via `gh run view <run_id> --log-failed`. **STOP the cycle**.
Do NOT auto-retry or attempt a fix. Tell the user the PR is open with
failing CI and ask them whether to (a) fix in a follow-up subagent
dispatch, or (b) abandon and pick a different task next cycle.

# Step 6 — Prompt merge (HUMAN CHECKPOINT #2)

Once CI is green, ask via `AskUserQuestion`:

- Question: "PR #N for NOTES-X is green. Merge?"
- Header: "Merge"
- Options:
  - "Merge (squash + delete branch)" (recommended)
  - "Hold — I'll merge later"
  - "Abort — don't merge this cycle"

On "Merge":

```bash
gh pr merge <PR_NUMBER> --squash --delete-branch
```

On "Hold": skip step 7's branch cleanup but do switch back to main.
End the cycle with a one-liner reminder of the open PR.

On "Abort": leave the PR open and the branch alone. End the cycle.

# Step 7 — Cleanup (parent does this directly)

After a successful merge:

```bash
git checkout main
git pull --ff-only
```

Then prune any stale local branches that were merged remotely. Follow
the procedure in `commit-commands:clean_gone` (delete `[gone]` branches
and their worktrees). If running it inline rather than as a separate
command, the equivalent is roughly:

```bash
git fetch --prune
git branch -vv | awk '/: gone]/{print $1}' | xargs -r git branch -D
```

Confirm `git status` shows clean working tree on `main`.

# Step 8 — End message

Print one line, nothing more:

> Cycle complete: NOTES-{{N}} closed (PR #M merged). NEW_NOTES added: {{...}}.

If running under `/loop`, the driver will fire the next iteration
automatically — the next iteration's `Step 2` will read NOTES.md fresh
(now without the just-closed item) and propose the next task.

If running standalone, suggest the user `/clear` and re-run
`/notes-cycle` for the next cycle, or `/compact` if they want to keep
session history but reduce context size.

# Invariants this command protects

- **The parent thread stays slim.** Heavy file reads, edits, test
  output, and lint logs all live in the subagent and don't bloat the
  parent's context across `/loop` iterations.
- **Two human checkpoints, always.** Task selection and merge approval
  are non-skippable — wrong-task selection and unintended merges are
  the highest-cost mistakes, so they're cheap to confirm.
- **No auto-retry on CI failure.** A red CI is a signal to think, not
  to grind. The user decides whether to fix or abandon.
- **No destructive recovery.** If the working tree is dirty, refuse to
  start. If a merge conflict appears, surface and stop. Never
  `git stash` / `git reset --hard` / `git checkout -f` as a shortcut.
- **NOTES.md edits ride on the closing PR.** No standalone
  reconciliation PRs. The subagent folds the edit in alongside the
  substantive change.
- **NOTES-N item numbers are stable.** When the subagent adds a new
  item, it uses the next unused number — never renumbers existing items.
