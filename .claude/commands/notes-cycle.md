---
description: Use when closing a NOTES.md punch-list item as a PR — the user names a NOTES-N, asks to work the punch list, or runs back-to-back cycles under `/loop`.
---

This command drives one iteration of the punch-list cycle: read the
`NOTES.md` index (item bodies live in `notes/NOTES-N.md`) →
confirm next task → dispatch a subagent to implement → dispatch the
`pr-reviewer` agent to review the diff → watch CI → prompt for merge →
cleanup. Task confirmation may
be satisfied by the user naming the item at invocation (the parent
echoes the resolved title before dispatch instead of asking); merge
approval is never skippable. Everything else autonomous.

The skill is composable: run it standalone for one cycle, or under
`/loop /notes-cycle` for autonomous back-to-back cycles. Under `/loop`
the parent context grows by only the subagent's short summary per
iteration — the heavy implementation transcript stays in the subagent
and dies with it. When the index has 2-3 unblocked, small/mechanical
items with disjoint footprints, `/notes-batch` runs them concurrently
instead.

# Step 1 — Pre-flight (parent does this directly)

Confirm the working tree is ready:

```bash
git status --porcelain         # see below — empty OR only allowlisted paths
git branch --show-current      # must be `main`
git pull --ff-only
```

Pre-flight rule: the porcelain output may be empty, OR may contain
**only** allowlisted paths — `CLAUDE.md` / `NOTES.md` at the repo
root, any file under `notes/`, or any file under `.claude/commands/`
or `.claude/agents/`.
Any other dirty path
(including a staged file, a new untracked file, or a deletion) blocks
the cycle. Capture the list of "riding-along" files for later steps —
call this `RIDE_ALONG_FILES`.

Concretely, if the porcelain output, after stripping the leading
status bytes, contains any line outside the allowlist
`{CLAUDE.md, NOTES.md, notes/*.md, .claude/commands/*.md, .claude/agents/*.md}`,
**STOP**. Tell the user which path blocked it and let them resolve
before re-running. Likewise STOP on a feature-branch checkout or a
pull conflict. Do not auto-stash or auto-checkout — those are
destructive shortcuts for problems we should investigate.

The riding-along files travel onto the feature branch naturally:
`git checkout -b` from main keeps unstaged changes in the working
tree, so the subagent inherits them and will commit them in step 5
(see Step 4 prompt). The user gets a confirmation chance in Step 3
before any of that happens, in case they'd rather stash instead.

# Step 2 — Read state (parent does this directly)

Read `NOTES.md` (the index — cheap by design). Each open item is one
line under a track heading, linking to its body in `notes/NOTES-N.md`.
The index line carries severity/effort and a blocked-by note; open the
item file only for the candidates you're actually considering.

Track priority, top to bottom of the index: the **active sprint**
track first, then the **ops floor**, then everything else. Parked
tracks ("parked …" in the heading) are eligible only when the user
names an item from them explicitly.

An item is **unblocked** if its index line says so or its
`## Dependencies` section references only closed items. An item is
**blocked** if it depends on a still-open `NOTES-N`.

# Step 3 — Propose & confirm next task (HUMAN CHECKPOINT #1)

Pick a recommended next task. Selection rule of thumb:

1. Prefer **unblocked** items
2. Among unblocked, prefer **earlier track** (active sprint > ops
   floor > everything else; parked tracks only on explicit user ask)
3. Among same track, prefer **higher severity** (high > medium > low)
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

If the user already named a specific NOTES-N when invoking the command
(check ARGUMENTS), that satisfies this checkpoint — skip the question,
but still echo a one-line confirmation of the resolved item before
dispatching: "Closing NOTES-N: <item title> — scope: <small/mechanical
or medium+/design-ambiguous>". This is a mistype guard, not a second
question — proceed to the scope gate and Step 4 immediately after
echoing it, without waiting for a reply.

**Scope gate (applies to whichever item was chosen):** classify the
item before dispatching.

- **Small / mechanical** — the NOTES body reads as a spec (named files,
  enumerated work items, no open design questions): proceed to Step 4.
- **Medium+ / design-ambiguous** — new architecture, schema changes,
  cross-cutting surfaces, or a NOTES body that says "needs its own
  spec/plan cycle": do NOT dispatch. Tell the user this item outgrows
  the cycle and route it through **superpowers:brainstorming** then
  **superpowers:writing-plans** in the main thread instead. End the
  cycle there.

If `RIDE_ALONG_FILES` from Step 1 is non-empty, mention it in the
question prose (not as a separate question), naming the exact files,
e.g. *"Note: your uncommitted edits to `<paths>` will ride on this
PR. Stash them first if that's not what you want."* This is the
user's last chance to back out — once Step 4 dispatches, those files
are committed on the feature branch.

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

Subagent prompt template (fill in `{{N}}`, `{{section_summary}}`,
and `{{ride_along_files}}` — the last is either "none" or a
comma-separated list of allowlisted paths from Step 1):

```
Close NOTES-{{N}} in one PR. Its body lives in
/Users/wlovotti/repos/wmata-dashboard/notes/NOTES-{{N}}.md; its index
line is in NOTES.md at the repo root. Item body verbatim:

{{section_summary}}

Pre-existing uncommitted edits in the working tree: {{ride_along_files}}.
The parent already vetted these — they're intentional and should ride
on this PR. They will appear as already-modified files when you start.
Do NOT stash, revert, or `git checkout --` them. Commit them on the
feature branch alongside your substantive change (either folded into
the main commit if scope-related, or as a separate commit on the
same branch with message `chore: roll up doc / tooling drafts`
if unrelated).

Execute this checklist top-to-bottom. Do not deviate.

1. BRANCH. From `main`:
     git checkout -b <prefix>/notes-{{N}}-<short-slug>
   `<prefix>` ∈ {feature, fix, docs, refactor} per the item's nature.
   The riding-along files (if any) will travel with the checkout —
   verify with `git status` before proceeding.

2. IMPLEMENT. Follow the item's "Implementation" / "Remaining work"
   section. Keep scope tight; do NOT refactor adjacent code.
   If the change adds or alters logic in src/, api/, or pipelines/:
   REQUIRED SUB-SKILL: superpowers:test-driven-development — write the
   failing test before the implementation. Doc, config, and shell
   plumbing changes are exempt (verify those in step 4 instead).

3. SIDE EFFECTS. If you discover a new issue worth tracking, create
   `notes/NOTES-<next-unused-N>.md` and add its one-line entry to the
   NOTES.md index in this same session (verify the next unused number
   against git history per the index header, not just the visible
   files). Never open a second PR. Never renumber existing items.

4. VERIFY (run in order; fix and re-run until each is clean):
     uv run pytest -m smoke
     uv run ruff check src/ scripts/ api/ pipelines/ tests/
     uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
   Match CI exactly — both ruff gates must include `tests/` or test-only
   lint errors will slip through and break the PR after push (this is
   what happened on PR #151 — heartbeat-table cutover for NOTES-72 Phase E.2).
   If the change touches more than one small surface, also run the
   full suite: `uv run pytest`.

5. FOLD PUNCH-LIST EDITS onto this branch (no separate PR):
   a. Delete the item file: `git rm notes/NOTES-{{N}}.md`.
   b. Remove the NOTES-{{N}} line from the NOTES.md index. If its
      track section becomes empty, remove the section header.
   c. Rewrite surviving cross-references to NOTES-{{N}} (in other
      `notes/*.md` files, code comments, docs) into a descriptive
      PR-anchored phrase, e.g.
        `the route_service_profile rollout (PR #M)`
      Use the in-flight PR number once known; otherwise leave a TODO
      and patch on PR open.
   d. Sweep the repo for stale references and rewrite them the same way:
        grep -rn 'NOTES-{{N}}' --include='*.md' --include='*.py' \
          --include='*.tsx' --include='*.ts' --include='*.jsx'
   There is no changelog line to update — git history is the record.

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
step 4.5.

# Step 4.5 — Review the diff (dispatch the `pr-reviewer` agent)

The subagent's work does not reach the merge prompt unreviewed — but
the parent does not read the diff itself. Dispatch the checked-in
`pr-reviewer` agent (`.claude/agents/pr-reviewer.md`, pinned to
`model: opus` so the review does not burn parent-tier tokens):

- `subagent_type: "pr-reviewer"`
- prompt containing: the PR number, the PR branch name, NOTES-{{N}},
  and the NOTES item section text verbatim (the same
  `{{section_summary}}` used in Step 4).

The agent reads every hunk, checks spec fidelity, sweeps for stale
NOTES-N references, runs the `code-review` skill on code-bearing
diffs, and returns a `VERDICT` plus findings tagged
cosmetic / substantive / fundamental.

The parent adjudicates the returned findings (it does NOT re-read the
diff): cosmetic → note them at the merge prompt; substantive → fix
via a follow-up dispatch to the same branch (never silently merge over
them); fundamental → STOP and surface to the user, same as a CI
failure. On `VERDICT: clean`, continue to step 5. If a finding's
severity tag seems miscalibrated or the verdict is ambiguous, the
parent may spot-check the specific hunks in question — that is the
exception, not the routine.

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
- **Task confirmation can be pre-satisfied; merge approval never is.**
  A user-named NOTES-N at invocation satisfies task confirmation, but
  the parent still echoes the resolved item title before dispatch as a
  mistype guard. Merge approval has no such shortcut — it always waits
  for an explicit answer, since an unintended merge is the highest-cost
  mistake in the cycle.
- **No auto-retry on CI failure.** A red CI is a signal to think, not
  to grind. The user decides whether to fix or abandon.
- **No unreviewed merges.** The dedicated `pr-reviewer` agent (Opus)
  reads the full PR diff — plus the `code-review` skill for
  code-bearing diffs — before the merge prompt, and the parent
  adjudicates its structured findings (step 4.5). Green CI alone does
  not qualify a PR for step 6.
- **Right-size the process.** The scope gate (step 3) keeps big or
  ambiguous items out of blind dispatch — those route to
  superpowers:brainstorming / superpowers:writing-plans instead.
- **No destructive recovery.** If the working tree has any dirty path
  outside the riding-along allowlist (`CLAUDE.md`, `NOTES.md`,
  `notes/*.md`, `.claude/commands/*.md`, `.claude/agents/*.md`),
  refuse to start. If a merge conflict appears, surface and stop. Never
  `git stash` / `git reset --hard` / `git checkout -f` as a shortcut.
- **Punch-list edits ride on the closing PR.** No standalone
  reconciliation PRs. The subagent folds the edit in alongside the
  substantive change.
- **Parallel cycles are allowed on disjoint items.** A closing PR
  touches only `notes/NOTES-N.md` plus one index line, so two cycles
  closing different items may run concurrently in separate git
  worktrees. Before dispatching a second concurrent cycle, check that
  neither item's cross-reference sweep will edit a file the other
  cycle's PR touches (including each other's item files) — if they
  overlap, serialize instead.
- **Pre-existing allowlist-file drafts also ride on the next PR.**
  Uncommitted edits to allowlisted paths (`CLAUDE.md`, `NOTES.md`,
  `notes/*.md`, `.claude/commands/*.md`, `.claude/agents/*.md`) at
  pre-flight are not a blocker — they
  travel onto the feature branch via the `git checkout -b` and are
  committed by the subagent. Edits to any other path still block the
  cycle. Step 3 surfaces this to the user before dispatch so they can
  stash if the timing is wrong.
- **NOTES-N item numbers are stable.** When the subagent adds a new
  item, it uses the next unused number — never renumbers existing items.
