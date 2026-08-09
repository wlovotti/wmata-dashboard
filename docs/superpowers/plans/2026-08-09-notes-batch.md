# /notes-batch Parallel Punch-List Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `/notes-batch` slash command that runs up to 3 punch-list
cycles concurrently in isolated worktrees, closing NOTES-101.

**Architecture:** One new command file (`.claude/commands/notes-batch.md`)
that references the serial `/notes-cycle` Step-4 prompt template and states
only its deltas; a one-sentence cross-reference added to
`.claude/commands/notes-cycle.md`; the NOTES-101 item file and index line
removed per the closing-PR convention. Markdown only — no product code.

**Tech Stack:** Claude Code slash commands (checked-in markdown under
`.claude/commands/`), git, `gh` CLI.

**Spec:** `docs/superpowers/specs/2026-08-09-notes-batch-design.md`
(committed on this branch). The spec is authoritative; this plan
transcribes it into files.

## Global Constraints

- Work on the existing branch `docs/notes-batch-spec` — NEVER commit to `main`.
- Batch cap is 3 lanes; candidate question offers at most 4 options
  (`AskUserQuestion` option cap).
- Batch pre-flight requires a fully clean tree (no ride-along allowlist).
- Only the parent mints NOTES-N numbers during a batch.
- Merge approval is per-PR, serialized, never skippable.
- Verification must match CI: `uv run pytest -m smoke`, then
  `uv run ruff check src/ scripts/ api/ pipelines/ tests/`, then
  `uv run ruff format --check src/ scripts/ api/ pipelines/ tests/`.
- `docs/superpowers/` files are frozen artifacts — the spec's own
  NOTES-101 references are exempt from the stale-reference sweep.

---

### Task 1: Create `.claude/commands/notes-batch.md`

**Files:**
- Create: `.claude/commands/notes-batch.md`

**Interfaces:**
- Consumes: the Step-4 subagent prompt template in
  `.claude/commands/notes-cycle.md` (referenced, not duplicated).
- Produces: the `/notes-batch` command that Task 2's cross-reference
  points at.

- [ ] **Step 1: Write the file with exactly this content**

````markdown
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
(`/notes-batch 94 88`), that pre-satisfies this checkpoint: validate
each (unblocked? passes the scope gate?) and echo the resolved
titles as a mistype guard before proceeding.

# Step 3 — Overlap check (parent does this directly)

For each selected item, compute a **predicted touch-set**:

1. `notes/NOTES-N.md` (its own body file);
2. every file its cross-reference sweep would edit:
   `grep -rln 'NOTES-N' --include='*.md' --include='*.py'
   --include='*.tsx' --include='*.ts' --include='*.jsx'`
   (this portion is exact);
3. files named in the item body's work section, widened by
   convention: for each named file under `src/`, `api/`, or
   `pipelines/`, also include `tests/test_<name>.py`.

`NOTES.md` itself is exempt — every closing PR touches it on
different lines; Step 7's mergeability re-check handles that case.

If two touch-sets intersect, drop the lower-priority item and name
it in the announcement, e.g. "NOTES-88 dropped: overlaps NOTES-94 on
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
   guaranteed; there are no pre-existing uncommitted edits).
2. **Add a worktree note** after the item body: "You are working in
   an isolated git worktree. Branch creation, implementation,
   `git push`, and `gh pr create` all behave normally there."
3. **Replace the SIDE EFFECTS step** with: "If you discover a new
   issue worth tracking, do NOT create `notes/NOTES-<N>.md` or touch
   the NOTES.md index. Instead return it under NEW_NOTES as a
   proposal: title, severity/effort guess, and a two-sentence body
   sketch. Only the parent mints item numbers." (This is the
   number-collision fix.)

Everything else carries over verbatim: the TDD requirement for logic
changes, the verify block matching CI, the fold-punch-list-edits
step (each lane deletes its own item file and index line — disjoint
by construction), the compact four-field return, and the
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

The `Agent` tool auto-removes unchanged worktrees. Prune the rest
after their branches merge:

```bash
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
````

- [ ] **Step 2: Verify the file parses as a command**

Run: `head -3 .claude/commands/notes-batch.md`
Expected: the `---` frontmatter open and the `description:` line.

- [ ] **Step 3: Commit**

```bash
git add .claude/commands/notes-batch.md
git commit -m "feature: add /notes-batch parallel punch-list command (NOTES-101)"
```

---

### Task 2: Cross-reference in `.claude/commands/notes-cycle.md`

**Files:**
- Modify: `.claude/commands/notes-cycle.md:14-18` (the composability
  paragraph)

**Interfaces:**
- Consumes: the `/notes-batch` command created in Task 1.
- Produces: nothing downstream — a discoverability pointer only. This
  is the ONLY edit to the serial command; do not restructure it.

- [ ] **Step 1: Apply exactly this edit**

Old text (verbatim — the tail of the composability paragraph):

```
iteration — the heavy implementation transcript stays in the subagent
and dies with it.
```

New text:

```
iteration — the heavy implementation transcript stays in the subagent
and dies with it. When the index has 2-3 unblocked, small/mechanical
items with disjoint footprints, `/notes-batch` runs them concurrently
instead.
```

- [ ] **Step 2: Verify no other lines changed**

Run: `git diff .claude/commands/notes-cycle.md | grep -c '^[+-][^+-]'`
Expected: `4` (one removed line, three added lines).

- [ ] **Step 3: Commit**

```bash
git add .claude/commands/notes-cycle.md
git commit -m "docs: point notes-cycle at /notes-batch for disjoint items"
```

---

### Task 3: Close NOTES-101 (punch-list fold)

**Files:**
- Delete: `notes/NOTES-101.md`
- Modify: `NOTES.md` (remove the NOTES-101 index line under
  "Deferred / trigger-based")

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: the punch-list state the PR body describes.

- [ ] **Step 1: Delete the item file**

```bash
git rm notes/NOTES-101.md
```

- [ ] **Step 2: Remove the index line**

In `NOTES.md`, delete the single line starting
`- [NOTES-101](notes/NOTES-101.md)` under the
"Deferred / trigger-based" heading. The section keeps its other
items (88, 49, 50) — do not remove the heading.

- [ ] **Step 3: Sweep for stale references**

Run:

```bash
grep -rn 'NOTES-101' --include='*.md' --include='*.py' \
  --include='*.tsx' --include='*.ts' --include='*.jsx' .
```

Expected surviving matches: ONLY files under `docs/superpowers/`
(the spec and this plan — frozen artifacts, exempt) and
`.claude/commands/notes-batch.md` / this plan's spec pointer, which
reference the spec by design. Any match in `notes/*.md`, `NOTES.md`,
or code must be rewritten to the descriptive phrase
"the /notes-batch parallel-cycle command (PR #<this PR>)". As of
plan-writing the expectation is zero such matches.

- [ ] **Step 4: Commit**

```bash
git add NOTES.md
git commit -m "docs: close NOTES-101 (shipped as /notes-batch)"
```

---

### Task 4: Verify, push, open the PR

**Files:**
- No new files; runs gates and opens the PR for the whole branch
  (spec commit + Tasks 1-3).

**Interfaces:**
- Consumes: all prior commits on `docs/notes-batch-spec`.
- Produces: the open PR for human review + CI.

- [ ] **Step 1: Run the CI-matching gates**

```bash
uv run pytest -m smoke
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
```

Expected: all pass — the branch is markdown-only, so any failure is
pre-existing; STOP and surface it rather than fixing unrelated code.

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin docs/notes-batch-spec
gh pr create --title "feature: /notes-batch parallel punch-list command (NOTES-101)" --body "$(cat <<'EOF'
## Why

NOTES-101: the PR #186 index/body split made closing PRs for different
items file-disjoint, but /notes-cycle remained structurally serial.
This adds the batch orchestration as a sibling command rather than a
mode inside notes-cycle, keeping the serial hot path slim (same
economics as the #186 split itself).

Design decisions (user-approved, full rationale in the committed spec
docs/superpowers/specs/2026-08-09-notes-batch-design.md):
- Fire-and-come-back: zero mid-flight prompts; end-of-batch merge
  walkthrough with per-PR, never-skippable approvals.
- Clean-tree pre-flight: batch mode drops the ride-along convention;
  serial /notes-cycle keeps it.
- NEW_NOTES as proposals: only the parent mints item numbers, filed in
  one batch-end chore PR — fixes number collisions.
- Two-layer overlap handling: predicted touch-sets pre-dispatch (cost
  filter), exact `gh pr view --json files` post-return, mergeability
  re-check after every squash-merge (correctness).
- Batch cap 3; lane isolation (failures park, never halt siblings).

## What

- New `.claude/commands/notes-batch.md` (references the serial Step-4
  template, states only deltas).
- One-sentence cross-reference in `.claude/commands/notes-cycle.md`.
- Spec committed under docs/superpowers/specs/.
- notes/NOTES-101.md deleted + index line removed (punch-list fold).

Markdown-only; smoke tests and both ruff gates run clean.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Report**

Return the PR number and URL. Do not merge — merge approval is the
user's.
