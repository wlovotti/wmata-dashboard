---
description: Use when closing 2-3 disjoint GitHub issues concurrently — multiple unblocked, small/mechanical issues with non-overlapping footprints. Single issues, ambiguous scopes, or ride-along drafts go through /issue-cycle instead.
---

This command batches up to 3 punch-list cycles: one selection
checkpoint → a fully autonomous middle (parallel worktree
implementers, per-lane review and CI) → an end-of-batch serialized
merge walkthrough. Fire-and-come-back by design: nothing prompts
mid-flight.

Relationship to `/issue-cycle`: read that file first. The serial
command is the single-issue path and the fallback whenever batching
isn't worth it. Its Step-4 subagent prompt template is used verbatim
here except for the deltas in Step 4 below. Its invariants all apply
unless this file says otherwise.

Design rationale (written against the old NOTES.md punch list, but the
lane/merge mechanics are unchanged) lives in
`docs/superpowers/specs/2026-08-09-notes-batch-design.md`.

# Step 1 — Pre-flight (parent does this directly)

```bash
git status --porcelain         # must be EMPTY — stricter than serial
git branch --show-current      # must be `main`
git pull --ff-only
```

Batch pre-flight requires a **fully clean tree**: there is no
ride-along allowlist here, because dirty files cannot travel into
fresh worktrees and must never be committed by two lanes. If
`git status --porcelain` prints anything, STOP and tell the user to
either run a serial `/issue-cycle` (which honors ride-alongs) or
stash. No auto-stash, no exceptions. Likewise STOP on a
feature-branch checkout or a pull conflict.

# Step 2 — Selection (HUMAN CHECKPOINT #1 — once per batch)

```bash
gh issue list --state open --limit 100 --json number,title,labels
```

Build the candidate list with the serial rules: **unblocked** issues
only (no `blocked` label, no open dependency), **small/mechanical**
only (the serial scope gate applies at candidacy — design-ambiguous
issues or ones that say "needs its own spec/plan cycle" never appear
as options), ordered by track priority (`track:ops` > `track:ux` >
unlabeled; `track:deferred` only on explicit user ask) then severity.
Open issue bodies only for the candidates under consideration.

Ask via `AskUserQuestion`:

- Question: "Which issues should this batch close? (up to 3)"
- Header: "Batch issues"
- `multiSelect: true`
- Options: up to 4 candidates (the tool's option cap), each
  description carrying track, severity, dependency status, and a
  scope estimate.

If the user selects more than 3, keep the top 3 by track priority
then severity and name the deferred ones in the dispatch
announcement. If the user named issues at invocation
(`/issue-batch 247 250`), that pre-satisfies this checkpoint: validate
each (unblocked? passes the scope gate?) and echo the resolved titles
as a mistype guard before proceeding.

# Step 3 — Overlap check (parent does this directly)

For each selected issue, compute a **predicted touch-set**: the files
named in the issue body's work section, widened by convention — for
each named file under `src/`, `api/`, or `pipelines/`, also include
`tests/test_<name>.py`. Add any file whose prose the reference sweep
would reword (`grep -rln '#N\b' --include='*.md' --include='*.py'
--include='*.tsx' --include='*.ts' --include='*.sh' --include='*.yml'
| grep -v '^\./docs/superpowers/' | grep -v 'docs/POSTMORTEM_'`).

If two touch-sets intersect, drop the lower-priority issue and name
it in the announcement, e.g. "#250 dropped: overlaps #247 on
`tests/conftest.py` — run it serially after." This check is a
**cost filter, not a correctness gate**: issue bodies name the center
of a change, not its blast radius, so misses are possible and
acceptable — merges are serialized and re-checked in Step 7, so a
missed overlap costs one visible rebase dispatch, never silent
corruption. The exact check runs post-return in Step 5.

If only one issue survives selection + overlap, say so and run the
serial `/issue-cycle` flow for it instead — a batch of one isn't
worth worktree overhead.

# Step 4 — Dispatch all lanes (one message)

Fire every lane in a single message so they run concurrently. Each
lane is an `Agent` call with:

- `subagent_type: "general-purpose"`
- `model: "sonnet"`
- `isolation: "worktree"`
- background execution (the default)

The prompt is the serial `/issue-cycle` Step-4 template with exactly
three deltas:

1. **Drop the ride-along paragraph** entirely (the clean tree is
   guaranteed; there are no pre-existing uncommitted edits). This
   also drops the checklist's step-1 sentence about riding-along
   files travelling with the checkout.
2. **Add a worktree note** after the issue body: "You are working in
   an isolated git worktree. Branch creation, implementation,
   `git push`, and `gh pr create` all behave normally there."
3. **Replace the SIDE EFFECTS step** with: "If you discover a new
   issue worth tracking, do NOT file it yourself. Return it under
   NEW_ISSUES as a proposal: title, severity/effort guess, and a
   two-sentence body sketch. Only the parent files issues during a
   batch, so two lanes reporting the same discovery don't produce
   duplicates." This redefines the return contract's `NEW_ISSUES`
   field accordingly: proposals, or "none".

Everything else carries over verbatim: the TDD requirement for logic
changes, the verify block matching CI, the reference sweep, the
`Closes #N` PR-body requirement, the compact four-field return, and
the `STATUS: needs_user` escape hatch.

# Step 5 — Autonomous middle (pipeline, not barrier)

Track each lane through: **dispatched → PR returned → reviewed →
green**, or **parked** with a reason. A returned lane advances
immediately; nothing waits for slower siblings, and no lane failure
ever halts a sibling (lane isolation).

**Post-return verification (exact touch-sets):** on each return, run
`gh pr view <N> --json files` and diff actual files against the
other lanes'. Record real collisions — they shape Step 7's merge
order.

**Review:** dispatch the `pr-reviewer` agent per lane (background),
exactly as serial Step 4.5. Adjudication deltas for
fire-and-come-back: *cosmetic* findings are noted for the
walkthrough; *substantive* findings trigger an automatic follow-up
fix dispatch to the same branch (a fresh worktree checking out the
existing branch), then re-review; *fundamental* findings park the
lane. Nothing merges over findings.

**CI:** after review-clean, poll all live lanes in one loop with
`gh pr checks <N> --json name,bucket`. A red check parks the lane
with the failed-check name and a `gh run view <run_id> --log-failed`
excerpt — no auto-retry, per the standing invariant. A lane
returning `STATUS: needs_user` parks with the question attached;
batch mode never prompts mid-flight, so it waits for the
walkthrough.

The middle ends when every lane is **green** or **parked**.

# Step 6 — Batch report

Print one line per lane: issue, PR number, review verdict, CI state,
real collisions found, and for parked lanes the reason (log excerpt,
finding, or question). Then begin the walkthrough.

# Step 7 — Merge walkthrough (HUMAN CHECKPOINT #2 — per PR, never skippable)

Walk green lanes first, in collision-aware order: independent PRs
before a colliding pair, and the pair in an explicit proposed order.
Each PR gets the exact serial-mode question (Merge (squash + delete
branch) / Hold — I'll merge later / Abort). After **every**
squash-merge:

1. `git pull --ff-only` on main.
2. Verify the issue auto-closed (`gh issue view <N> --json state`);
   close by hand with a "Closed by PR #M" comment if not.
3. Re-check each remaining open PR:
   `gh pr view <N> --json mergeable,mergeStateStatus`.
4. If a remaining PR just went conflicted, offer an inline option:
   dispatch a small rebase agent — rebase onto `main`, resolve, push,
   re-watch CI — or Hold / Abort that PR.

Parked lanes come last, each with fix-via-follow-up-dispatch /
abandon / leave-open choices, mirroring the serial CI-failure
protocol. `needs_user` questions are answered here; the lane either
re-dispatches with the answer or is abandoned.

# Step 8 — File NEW_ISSUES

After all merge decisions: collect NEW_ISSUES proposals across lanes,
dedup them (two implementers can report the same discovery), and
check each against open issues (`gh issue list --search`). File the
survivors with `gh issue create` and the standard labels (see the
`file-issue` skill). No PR is needed. Skip this step entirely if no
lane proposed anything.

# Step 9 — Cleanup

The `Agent` tool auto-removes unchanged worktrees, but lanes that
committed leave their worktree directories on disk — `git worktree
prune` only drops admin records for already-*deleted* directories,
so it can't clean those up by itself, and a subsequent
`git branch -D` on a still-checked-out branch fails with "cannot
delete branch ... used by worktree". List the worktrees first and
explicitly remove each one whose branch has merged, THEN prune, then
run the existing checkout/pull/fetch/branch-deletion tail:

```bash
git worktree list
git worktree remove <path>   # once per merged-branch lane worktree
git worktree prune
git checkout main
git pull --ff-only
git fetch --prune
git branch -vv | awk '/: gone]/{print $1}' | xargs -r git branch -D
```

Confirm `git status` shows a clean tree on `main`.

# Step 10 — End message

Print a short summary: issues closed (with PR numbers), lanes parked
and why, new issues filed. Suggest `/clear` before the next batch or
cycle.

# Crash recovery

If the session dies mid-batch, nothing is lost: PRs and branches
live on GitHub. Recovery is handling each open PR serially — review
state and CI are visible via `gh pr view` / `gh pr checks` — and
`git worktree prune` clears worktree litter. Do not treat leftover
batch PRs as a mystery; they are ordinary open PRs.

# Invariants this command protects

- All serial `/issue-cycle` invariants apply: merge approval per-PR
  and never skippable; no auto-retry on red CI; no unreviewed
  merges; no destructive recovery (`stash` / `reset --hard` /
  `checkout -f`); the parent thread stays slim (lanes return the
  compact four-field report; heavy transcripts die with their
  subagents).
- **Only the parent files issues during a batch.**
- **Batch pre-flight requires a fully clean tree.**
- **Lane isolation:** no lane failure halts a sibling; every failure
  mode converges to a labeled parked lane, so the batch always
  terminates with a complete, decision-ready report.
