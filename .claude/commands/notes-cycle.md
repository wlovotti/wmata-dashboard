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

Invoke the `Agent` tool with:

- `subagent_type: "general-purpose"`
- `model: "sonnet"` — the task is bounded mechanical work (branch,
  edit, test, PR). Sonnet is cheaper and faster; genuinely hard items
  kick back via `STATUS: needs_user` and the parent (Opus) re-routes.
- a self-contained prompt (template below)

The subagent loads the project's `CLAUDE.md` automatically, so the
prompt does NOT restate repo conventions — it just hands over the task
and the closing-PR checklist.

Subagent prompt template (fill in `{{N}}` and `{{section_summary}}`
from what you read in step 2):

```
Close NOTES-{{N}} from /Users/wlovotti/repos/wmata-dashboard/NOTES.md
in one PR. Item section verbatim:

{{section_summary}}

Execute this checklist top-to-bottom. Do not deviate.

1. BRANCH. From `main`:
     git checkout -b <prefix>/notes-{{N}}-<short-slug>
   `<prefix>` ∈ {feature, fix, docs, refactor} per the item's nature.

2. IMPLEMENT. Follow the item's "Implementation" / "Remaining work"
   section. Keep scope tight; do NOT refactor adjacent code.

3. SIDE EFFECTS. If you discover a new issue worth tracking, APPEND
   a NOTES-<next-unused-N> entry to NOTES.md in this same session.
   Never open a second PR. Never renumber existing items.

4. VERIFY (run in order; fix and re-run until each is clean):
     uv run pytest -m smoke
     uv run ruff check src/ scripts/ api/ pipelines/
     uv run ruff format --check src/ scripts/ api/ pipelines/
   If the change touches more than one small surface, also run the
   full suite: `uv run pytest`.

5. FOLD NOTES.md EDITS onto this branch (no separate PR):
   a. Delete the NOTES-{{N}} section wholesale (its header through
      the next `---` separator).
   b. Remove the NOTES-{{N}} bullet from "Active priorities". If its
      priority subsection becomes empty, remove the subsection header.
   c. Rewrite surviving cross-references to NOTES-{{N}} into a
      descriptive PR-anchored phrase, e.g.
        `the route_service_profile rollout (PR #M)`
      Use the in-flight PR number once known; otherwise leave a TODO
      and patch on PR open.
   d. Sweep the repo for stale references and rewrite them the same way:
        grep -rn 'NOTES-{{N}}' --include='*.md' --include='*.py' \
          --include='*.tsx' --include='*.ts'
   e. Update the "Last edited YYYY-MM-DD" line at the top of NOTES.md
      to today's date.

6. COMMIT. Format:
     <prefix>: <short summary> (NOTES-{{N}})

7. OPEN PR with `gh pr create`. Title mirrors the commit. Body MUST
   explain *why* the change was scoped this way — it becomes the
   durable record once NOTES-{{N}} is deleted. A one-line body is
   not acceptable.

8. RETURN ONLY these four fields (no preamble, no recap):
     PR_NUMBER: <int>
     PR_URL: <url>
     SUMMARY: one paragraph — what changed and what verification ran
     NEW_NOTES: list of new NOTES-N items added, or "none"

ESCAPE HATCH: if you hit architectural ambiguity that needs a human
decision, STOP and return:
     STATUS: needs_user
     QUESTION: <what you need decided>
Do not guess.
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
