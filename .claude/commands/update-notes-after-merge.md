---
description: Reconcile NOTES.md after PRs merge — delete completed items, rewrite cross-refs to PR numbers, bump the freshness line. Run this AFTER a PR closing one or more NOTES-N items has merged into main.
---

NOTES.md is a forward-looking punch list. Completed items get deleted,
not archived — git log + PR descriptions are the durable history. Item
numbers (`NOTES-N`) are stable; never renumber. New items take the next
unused number.

# When to run

After a PR that closes one or more NOTES-N items has merged into main.
Don't run pre-merge — pre-merge cleanup risks marking something complete
that gets reverted, hot-fixed, or never lands.

# Steps

1. **Read the freshness line** at the top of NOTES.md ("Last reconciled
   YYYY-MM-DD (through PR #N)") to find the high-water mark.

2. **List candidates.** Enumerate every merge to main since the
   high-water PR:
   ```bash
   gh pr list --state merged --base main --limit 30 \
     --json number,title,mergedAt,body \
     --jq '.[] | select(.number > <high_water_pr>)'
   ```

3. **Match merges to NOTES-N items.** For each merged PR, look in the
   PR body for `closes NOTES-N`, `fixes NOTES-N`, or any `NOTES-N`
   reference that asserts completion. If the PR body is silent on
   NOTES, ask the user before assuming the PR closes anything.

4. **Refuse on thin PR descriptions.** Closing a NOTES-N item deletes
   its content from NOTES.md, so the closing PR's body becomes the
   durable record of *why* the item was scoped that way. If the body
   is one line or empty, refuse to delete and ask the user to either
   expand the PR description or confirm the deletion explicitly.

5. **Verify the feature is actually live.** If the item's section
   names a verification mechanism (e.g., NOTES-6 collector items
   verified by `scripts/collector_status.py` printing `✓ healthy`,
   or NOTES-N table-creation items verified by a `\d <table>` query),
   run it. If verification fails, do NOT delete — flag the regression
   instead.

6. **Apply edits to NOTES.md:**
   - Delete the completed item's section wholesale (header through the
     next `---` separator).
   - Remove the bullet from the relevant priority list in
     "Active priorities".
   - Rewrite cross-references in surviving items: `NOTES-<deleted>`
     becomes a descriptive PR reference, e.g.
     `the route_service_profile rollout (PR #37)` rather than `NOTES-16`.
   - If a priority subsection becomes empty, remove the subsection
     header.
   - Bump the freshness line: `Last reconciled YYYY-MM-DD (through
     PR #N)` where N is the highest merged PR considered.

7. **Sweep the rest of the repo for stale references** to deleted
   items in code comments / CLAUDE.md / etc. and rewrite them to
   point to the merged PR:
   ```bash
   grep -rn 'NOTES-<deleted>' --include='*.md' --include='*.py' \
     --include='*.tsx' --include='*.ts'
   ```

8. **Show the diff before applying.** Always print the proposed
   NOTES.md diff and any cross-repo edits. Ask the user to confirm
   before writing. This is a guided workflow, not autonomous.

9. **Don't commit on the user's behalf.** Per the project's git policy,
   the user opens a feature branch and commits explicitly. Surface the
   suggested commit message, but stop short of running git.

# Invariants the skill protects

- **Item numbers are stable.** Never renumber surviving items when one
  is deleted. Old PR descriptions reference NOTES-N by number;
  renumbering breaks that link silently.
- **No archive section.** Don't preserve completed item bodies under a
  "Recently completed" or "History" header. The freshness line is the
  only nod to history that lives in NOTES.md.
- **`PR #N` references stay as-is.** Those are GitHub PR autolinks and
  we want them. Only `NOTES-N` references are NOTES.md item numbers.
- **`NOTES-N` is the canonical reference form** in PR bodies, code
  comments, and other docs — never bare `#N` (which GitHub autolinks
  to a PR/issue and confuses readers).
