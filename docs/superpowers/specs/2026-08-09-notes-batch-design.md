# /notes-batch — parallel punch-list cycles (NOTES-101)

**Date:** 2026-08-09
**Status:** approved design, pre-implementation
**Closes:** NOTES-101 (parallel notes-cycle driver, batch mode)

## Goal

Run up to 3 punch-list cycles concurrently when the index has multiple
unblocked, small/mechanical items. The PR #186 split made closing PRs
file-disjoint by construction; this command supplies the orchestration
that the serial `/notes-cycle` still lacks. The payoff is wall-clock,
not tokens: a batch spawns the same implementer + reviewer agents as
the equivalent serial cycles, and the parent-side overhead is
single-digit percent.

Known limitation, accepted: batch mode does not help dependency
chains. The comparison sprint (96 → 100 → 99) runs serial regardless;
batching pays off on the independent tail (ops floor + parked items)
and any future track that fans out.

## Shape

A sibling command, `.claude/commands/notes-batch.md`. The serial
`/notes-cycle` stays untouched and remains both the single-item path
and the fallback whenever batching is not worth it. The batch command
references the serial Step-4 subagent prompt template and states only
its deltas, so the two files cannot drift far. This mirrors the
NOTES.md index/body economics: keep the hot path slim, load the batch
machinery only when invoked.

Alternatives rejected:

- **Batch mode folded into `/notes-cycle`** — doubles a 374-line
  command; every serial invocation would pay tokens for machinery it
  isn't using.
- **`Workflow`-script driver** — workflow scripts cannot pause for
  `AskUserQuestion`, so merge approvals would happen after the script
  returns anyway; background `Agent` dispatch gives the same result
  with less machinery at N ≤ 3.

## Fixed decisions

| Decision | Choice |
|---|---|
| Interaction model | Fire-and-come-back: zero mid-flight prompts; all approvals in an end-of-batch walkthrough |
| Ride-alongs | Not supported in batch — pre-flight requires a fully clean tree; serial `/notes-cycle` keeps the convention |
| NEW_NOTES | Subagents return proposals; parent files them in one batch-end chore PR |
| Batch cap | 3 lanes |
| Scope gate | Unchanged from serial — only small/mechanical items are batch-eligible |
| Failure policy | Lane isolation: a failed lane parks; it never halts siblings |
| Merge approval | Per-PR, serialized, never skippable — identical question to serial checkpoint #2 |

## Flow

### 1. Pre-flight

On `main`, `git pull --ff-only`, and `git status --porcelain` must be
**empty** — stricter than serial, which allowlists ride-along paths.
Any dirty path stops the batch with a pointer to run a serial cycle
(which honors ride-alongs) or stash. No auto-stash, no exceptions.

### 2. Selection (human checkpoint #1 — once per batch)

The parent reads the NOTES.md index and builds candidates using the
serial rules: unblocked, small/mechanical (the scope gate applies at
candidacy — design-ambiguous items never appear as options), ordered
by track priority then severity. One `AskUserQuestion` with
`multiSelect: true` offers up to four candidates (the tool's option
cap), each described with track, severity, and scope estimate; the
user picks up to 3. If more than 3 are selected, the parent keeps the
top 3 by track priority then severity and names the ones deferred.

Items named at invocation (`/notes-batch 94 88`) pre-satisfy the
checkpoint; the parent validates each (unblocked, scope-gate pass) and
echoes the resolved titles as a mistype guard.

### 3. Overlap check (predicted — a cost filter, not a correctness gate)

Per selected item, compute a predicted touch-set:

- `notes/NOTES-N.md`
- every file its cross-reference sweep would edit
  (`grep -rln 'NOTES-N'` with the standard include list — this
  portion is exact)
- files named in the item body's work section, widened by convention:
  for each named file under `src/`, `api/`, or `pipelines/`, include
  `tests/test_<name>.py`

If two touch-sets intersect, drop the lower-priority item and name it
in the announcement (e.g. "NOTES-88 dropped: overlaps NOTES-94 on
`src/wmata_collector.py` — run it serially after"). `NOTES.md` itself
is exempt: every closing PR touches it on different lines; the merge
stage handles that case.

Prediction accuracy is medium — item bodies name the center of a
change, not its blast radius — which is acceptable because correctness
never depends on it: merges are serialized and re-checked (step 7),
so a missed overlap costs one visible rebase dispatch, never silent
corruption. The exact check happens post-return (step 5).

If only one item survives, fall back to the serial flow — a batch of
one isn't worth worktree overhead.

### 4. Dispatch

All lanes fire in a single message: `general-purpose` subagents,
`model: sonnet`, `isolation: "worktree"`, background. Each prompt is
the serial Step-4 template with three deltas:

1. **No ride-along paragraph** — the clean tree is guaranteed.
2. **Worktree note** — the agent works in an isolated worktree;
   branch, implement, push, `gh pr create` all behave normally there.
3. **SIDE EFFECTS inverted** — do NOT create `notes/NOTES-<next>.md`
   or touch the index for new discoveries; return NEW_NOTES as
   structured proposals (title, severity/effort guess, two-sentence
   body sketch) in the final report. This is the number-collision fix:
   only the parent mints numbers, and only serially.

Everything else carries over verbatim: TDD requirement for logic
changes, the verify block matching CI, the fold-punch-list-edits step
(deleting the item file and its index line — that part is per-lane
and disjoint by construction), the compact four-field return, the
`STATUS: needs_user` escape hatch.

### 5. Lanes: pipeline, not barrier

Each lane advances independently: dispatched → PR returned → reviewed
→ CI green, or **parked** with a reason. A returned lane starts its
post-return verification and review immediately; nothing waits for
slower siblings.

**Post-return verification (exact touch-sets):** on each return, run
`gh pr view <N> --json files` and diff actual files against other
lanes'. Real collisions are recorded and shape the merge order in the
walkthrough.

**Review:** same `pr-reviewer` (Opus) dispatch as serial, per lane, in
the background. Adjudication deltas for fire-and-come-back:
substantive findings trigger an automatic follow-up fix dispatch to
the same branch (fresh worktree checking out the existing branch),
then re-review; fundamental findings park the lane. Nothing merges
over findings.

**CI:** after review-clean, the parent polls all live lanes in one
loop (`gh pr checks <N> --json name,bucket`). A red check parks the
lane with the failed-check name and a `gh run view --log-failed`
excerpt — no auto-retry. `STATUS: needs_user` parks the lane with the
question attached; in batch mode nothing prompts mid-flight, so it
waits for the walkthrough.

The middle ends when every lane is green or parked. Every failure
mode converges to a labeled parked lane, so the batch always
terminates with a complete, decision-ready report.

### 6. Walkthrough opening (batch report)

One line per lane: item, PR number, review verdict, CI state, real
collisions found, and for parked lanes the reason (log excerpt,
finding, or question).

### 7. Merge serialization (human checkpoint #2 — per PR, never skippable)

Green lanes first, in collision-aware order: independent PRs before a
colliding pair, and the pair in an explicit proposed order. Each PR
gets the exact serial-mode question (Merge / Hold — I'll merge later /
Abort). After **every** squash-merge:

1. `git pull --ff-only` on main.
2. Re-check each remaining open PR:
   `gh pr view <N> --json mergeable,mergeStateStatus`.
3. A PR that just went conflicted (the adjacent-index-line-deletion
   case) gets an inline option: dispatch a small rebase agent —
   rebase onto `main`, resolve the NOTES.md conflict by the
   deterministic rule that **both index-line deletions are kept**,
   push, re-watch CI — or Hold / Abort.

Parked lanes come last, each with fix-via-dispatch / abandon /
leave-open choices, mirroring the serial CI-failure protocol.
`needs_user` questions are answered here and the lane either
re-dispatches with the answer or is abandoned.

### 8. NEW_NOTES chore PR

After all merge decisions: collect proposals across lanes, dedup
(two implementers can report the same discovery), assign numbers
serially against the git-history max-N check, write the `notes/`
files + index lines directly (parent-authored, no subagent) on a
`docs/` branch, open one small PR, append it to the walkthrough as a
final approval. Skip entirely if no lane proposed anything.

### 9. Cleanup

The `Agent` tool auto-removes unchanged worktrees; the parent prunes
the rest after their branches merge (`git worktree prune`, remove
leftovers), then the standard tail: `git checkout main`,
`git pull --ff-only`, `git fetch --prune`, delete `[gone]` branches,
confirm clean tree.

**Crash recovery:** if the session dies mid-batch, nothing is lost —
PRs and branches live on GitHub; recovery is handling each open PR
serially (review state and CI are visible via `gh`), and
`git worktree prune` clears worktree litter. The command file carries
a one-line note to this effect so a future session doesn't treat
leftover batch PRs as a mystery.

### 10. End message

Items closed (with PR numbers), lanes parked and why, NEW_NOTES filed.

## Invariants (inherited and new)

- Merge approval is per-PR and never skippable; an unintended merge
  remains the highest-cost mistake.
- No auto-retry on red CI; no unreviewed merges; no destructive
  recovery (`stash` / `reset --hard` / `checkout -f`) — all inherited
  from serial.
- **New:** only the parent mints NOTES-N numbers during a batch.
- **New:** batch pre-flight requires a fully clean tree.
- **New:** lane isolation — no lane failure halts a sibling.
- The parent thread stays slim: lanes return the serial four-field
  compact report; heavy transcripts die with their subagents.

## Implementation notes

- One new file: `.claude/commands/notes-batch.md`.
- One edit to `.claude/commands/notes-cycle.md`: a cross-reference in
  the intro ("for ≥2 unblocked disjoint items, see `/notes-batch`")
  and nothing else.
- The closing PR for NOTES-101 deletes `notes/NOTES-101.md` and its
  index line per convention.
- No product code changes; smoke tests and ruff gates are unaffected
  but run anyway per the standard checklist.
