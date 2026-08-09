---
description: Fold punch-list edits into the PR that closes them — delete completed items (notes/NOTES-N.md + index line), rewrite cross-refs to PR numbers. Run this WHILE preparing a PR that closes one or more NOTES-N items, not after merge.
---

The punch list is a forward-looking index (`NOTES.md`) plus one body
file per open item (`notes/NOTES-N.md`). Completed items get deleted,
not archived — git log + PR descriptions are the durable history. Item
numbers (`NOTES-N`) are stable; never renumber. New items take the next
unused number (verify against git history per the index header).

Punch-list edits **never get a dedicated PR**. They ride on the
substantive PR that closes the item, or piggyback on the next
unrelated PR. A standalone "reconcile NOTES.md" PR is churn.

# When to run

While preparing a PR (pre-merge, on a feature branch) that:

- closes one or more NOTES-N items, OR
- happens to need other punch-list cleanup (stale references, new
  item) and is a convenient ride-along.

The closing PR is the moment of truth: the code that delivers the fix
and the deletion of the item's planning notes land together. After
merge, the punch list is automatically up to date — no separate
reconciliation step.

# Steps

1. **Confirm which NOTES-N items this PR closes.** Look at the PR body
   draft and the branch's commits. If the PR doesn't explicitly close
   any items but you have an unrelated punch-list edit to ride along
   (stale ref, etc.), skip the deletion-specific steps and jump to
   step 4 for the targeted edit.

2. **Refuse on thin PR descriptions.** Closing a NOTES-N item deletes
   its body file, so the closing PR's body becomes the
   durable record of *why* the item was scoped that way. If the body
   is one line or empty, refuse to delete and ask the user to either
   expand the PR description or confirm the deletion explicitly.

3. **Verify the fix is implemented in this branch.** Read the changed
   files; confirm the NOTES-N item's prescribed fix is actually
   present (don't just trust the PR title). If the item's section
   names a verification mechanism (a script, a SQL query, a test),
   run it. If verification fails, do NOT delete — flag the gap.

4. **Apply the punch-list edits:**
   - Delete the completed item's body file: `git rm notes/NOTES-<N>.md`.
   - Remove its line from the NOTES.md index; if its track section
     becomes empty, remove the section header.
   - Rewrite cross-references in surviving item files: `NOTES-<deleted>`
     becomes a descriptive PR reference, e.g.
     `the route_service_profile rollout (PR #37)` rather than `NOTES-16`.
     Use the in-progress PR's number if known; otherwise use a
     descriptive placeholder and update on PR open.
   - There is no "Last edited" changelog line — git history is the
     record; do not add one.

5. **Sweep the rest of the repo for stale references** to deleted
   items in code comments / CLAUDE.md / etc. and rewrite them to
   point to the PR:
   ```bash
   grep -rn 'NOTES-<deleted>' --include='*.md' --include='*.py' \
     --include='*.tsx' --include='*.ts'
   ```

6. **Show the diff before applying.** Print the proposed punch-list
   diff (index + item files) and any cross-repo edits. Ask the user to
   confirm before writing. This is a guided workflow, not autonomous.

7. **Stage on the existing branch.** No new branch — edits go on the
   feature branch already in flight, either in the same commit as the
   substantive change or as a follow-on commit on the same branch.
   Don't commit on the user's behalf; surface the suggested commit
   message and stop.

# Invariants the skill protects

- **No dedicated punch-list PRs.** Edits ride on substantive PRs.
  Standalone reconciliation is churn.
- **Item numbers are stable.** Never renumber surviving items when one
  is deleted. Old PR descriptions reference NOTES-N by number;
  renumbering breaks that link silently.
- **No archive section.** Don't preserve completed item bodies under a
  "Recently completed" or "History" header.
- **`PR #N` references stay as-is.** Those are GitHub PR autolinks and
  we want them. Only `NOTES-N` references are NOTES.md item numbers.
- **`NOTES-N` is the canonical reference form** in PR bodies, code
  comments, and other docs — never bare `#N` (which GitHub autolinks
  to a PR/issue and confuses readers).
