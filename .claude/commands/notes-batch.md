---
description: Use when closing 2-3 disjoint NOTES punch-list items concurrently — multiple unblocked, small/mechanical items with non-overlapping footprints. Single items, ambiguous scopes, or ride-along drafts go through /notes-cycle instead.
---

This command batches up to 3 punch-list cycles: one selection
checkpoint → a fully autonomous middle (parallel worktree
implementers, per-lane review and CI) → an end-of-batch serialized
merge walkthrough. Fire-and-come-back by design: nothing prompts
mid-flight.

Relationship to `/notes-cycle`: read that file first. The serial
command is the single-item path and the fallback whenever batching
isn't worth it. Its Step-4 subagent prompt template is used verbatim
here except for the three deltas in Step 4 below. Its invariants all
apply unless this file says otherwise.

Design rationale lives in
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
either run a serial `/notes-cycle` (which honors ride-alongs) or
stash. No auto-stash, no exceptions. Likewise STOP on a
feature-branch checkout or a pull conflict.

# Step 2 — Selection (HUMAN CHECKPOINT #1 — once per batch)

Read the NOTES.md index. Build the candidate list with the serial
rules: **unblocked** items only, **small/mechanical** only (the
serial scope gate applies at candidacy — items that are
design-ambiguous or say "needs its own spec/plan cycle" never appear
as options), ordered by track priority (active sprint > ops floor >
everything else; parked tracks only on explicit user ask) then
severity. Open item files only for the candidates under
consideration.

Ask via `AskUserQuestion`:

- Question: "Which NOTES items should this batch close? (up to 3)"
- Header: "Batch items"
- `multiSelect: true`
- Options: up to 4 candidates (the tool's option cap), each
  description carrying track, severity, dependency status, and a
  scope estimate.

If the user selects more than 3, keep the top 3 by track priority
then severity and name the deferred ones in the dispatch
announcement. If the user named items at invocation
(`/notes-batch 12 34`), that pre-satisfies this checkpoint: validate
each (unblocked? passes the scope gate?) and echo the resolved
titles as a mistype guard before proceeding.

# Step 3 — Overlap check (parent does this directly)

For each selected item, compute a **predicted touch-set**:

1. `notes/NOTES-N.md` (its own body file);
2. every file its cross-reference sweep would edit:
   `grep -rln 'NOTES-N' --include='*.md' --include='*.py'
   --include='*.tsx' --include='*.ts' --include='*.jsx'
   | grep -v '^\./docs/superpowers/' | grep -v 'docs/POSTMORTEM_'`
   — the fold sweep never edits `docs/superpowers/` or
   `docs/POSTMORTEM_*.md` (frozen artifacts), so matches there are
   excluded from the touch-set (this portion is exact);
3. files named in the item body's work section, widened by
   convention: for each named file under `src/`, `api/`, or
   `pipelines/`, also include `tests/test_<name>.py`.

`NOTES.md` itself is exempt — every closing PR touches it on
different lines; Step 7's mergeability re-check handles that case.

If two touch-sets intersect, drop the lower-priority item and name
it in the announcement, e.g. "NOTES-34 dropped: overlaps NOTES-12 on
`src/wmata_collector.py` — run it serially after." This check is a
**cost filter, not a correctness gate**: item bodies name the center
of a change, not its blast radius, so misses are possible and
acceptable — merges are serialized and re-checked in Step 7, so a
missed overlap costs one visible rebase dispatch, never silent
corruption. The exact check runs post-return in Step 5.

If only one item survives selection + overlap, say so and run the
serial `/notes-cycle` flow for it instead — a batch of one isn't
worth worktree overhead.

# Step 4 — Dispatch all lanes (one message)

Fire every lane in a single message so they run concurrently. Each
lane is an `Agent` call with:

- `subagent_type: "general-purpose"`
- `model: "sonnet"`
- `isolation: "worktree"`
- background execution (the default)

The prompt is the serial `/notes-cycle` Step-4 template with exactly
three deltas:

1. **Drop the ride-along paragraph** entirely (the clean tree is
   guaranteed; there are no pre-existing uncommitted edits). This
   also drops the serial checklist's step-1 sentence "The
   riding-along files (if any) will travel with the checkout — verify
   with `git status` before proceeding" — it's obsolete under the
   same no-ride-alongs guarantee.
2. **Add a worktree note** after the item body: "You are working in
   an isolated git worktree. Branch creation, implementation,
   `git push`, and `gh pr create` all behave normally there."
3. **Replace the SIDE EFFECTS step** with: "If you discover a new
   issue worth tracking, do NOT create `notes/NOTES-<N>.md` or touch
   the NOTES.md index. Instead return it under NEW_NOTES as a
   proposal: title, severity/effort guess, and a two-sentence body
   sketch. Only the parent mints item numbers." (This is the
   number-collision fix.) This also redefines the serial return
   contract's `NEW_NOTES` field: instead of "list of new NOTES-N
   items added, or 'none'", it becomes "NEW_NOTES: proposals (title,
   severity/effort guess, two-sentence body sketch), or 'none'".

Everything else carries over verbatim, with the NEW_NOTES return
field redefined by delta 3: the TDD requirement for logic changes,
the verify block matching CI, the fold-punch-list-edits step (each
lane deletes its own item file and index line — disjoint by
construction), the compact four-field return, and the
`STATUS: needs_user` escape hatch.

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

Print one line per lane: item, PR number, review verdict, CI state,
real collisions found, and for parked lanes the reason (log excerpt,
finding, or question). Then begin the walkthrough.

# Step 7 — Merge walkthrough (HUMAN CHECKPOINT #2 — per PR, never skippable)

Walk green lanes first, in collision-aware order: independent PRs
before a colliding pair, and the pair in an explicit proposed order.
Each PR gets the exact serial-mode question (Merge (squash + delete
branch) / Hold — I'll merge later / Abort). After **every**
squash-merge:

1. `git pull --ff-only` on main.
2. Re-check each remaining open PR:
   `gh pr view <N> --json mergeable,mergeStateStatus`.
3. If a remaining PR just went conflicted (the
   adjacent-index-line-deletion case), offer an inline option:
   dispatch a small rebase agent — rebase onto `main`, resolve the
   NOTES.md conflict by the deterministic rule that **both
   index-line deletions are kept**, push, re-watch CI — or Hold /
   Abort that PR.

Parked lanes come last, each with fix-via-follow-up-dispatch /
abandon / leave-open choices, mirroring the serial CI-failure
protocol. `needs_user` questions are answered here; the lane either
re-dispatches with the answer or is abandoned.

# Step 8 — NEW_NOTES chore PR

After all merge decisions: collect NEW_NOTES proposals across lanes
and dedup them (two implementers can report the same discovery).
Assign numbers serially against the git-history max-N check from the
index header. The parent writes the `notes/NOTES-N.md` files and
index lines itself (no subagent) on a `docs/` branch, opens one
small PR, and appends it to the walkthrough as a final approval
prompt. Skip this step entirely if no lane proposed anything.

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

Print a short summary: items closed (with PR numbers), lanes parked
and why, NEW_NOTES filed. Suggest `/clear` before the next batch or
cycle.

# Crash recovery

If the session dies mid-batch, nothing is lost: PRs and branches
live on GitHub. Recovery is handling each open PR serially — review
state and CI are visible via `gh pr view` / `gh pr checks` — and
`git worktree prune` clears worktree litter. Do not treat leftover
batch PRs as a mystery; they are ordinary open PRs.

# Invariants this command protects

- All serial `/notes-cycle` invariants apply: merge approval per-PR
  and never skippable; no auto-retry on red CI; no unreviewed
  merges; no destructive recovery (`stash` / `reset --hard` /
  `checkout -f`); the parent thread stays slim (lanes return the
  compact four-field report; heavy transcripts die with their
  subagents).
- **Only the parent mints NOTES-N numbers during a batch.**
- **Batch pre-flight requires a fully clean tree.**
- **Lane isolation:** no lane failure halts a sibling; every failure
  mode converges to a labeled parked lane, so the batch always
  terminates with a complete, decision-ready report.
