---
name: add-to-notes
description: Add a newly identified improvement idea, feature, bug, refactor, or future-work item to this repo's NOTES.md punch list. Use whenever the user says "add to NOTES", "track this", "punch list", "let's not forget", "log this for later", or surfaces an idea mid-conversation that is worth not losing but is not being implemented right now. Also use proactively when, during a code review or investigation, you yourself identify something non-blocking that the user should know about later — propose adding it to NOTES.md rather than burying it in a comment that will scroll away.
---

The project's forward-looking punch list is an index (`NOTES.md`) plus
one body file per open item (`notes/NOTES-N.md`). New items are added
here so they survive context loss between sessions and PRs. The skill's
job is to take a candidate idea, check it isn't already tracked, and
create a well-formatted item file + index line — leaving the working
tree dirty so the edit rides on the next substantive PR (per the
project's `update-notes-in-pr` workflow; standalone reconciliation PRs
are churn).

# Inputs

A short description of the idea, fix, or feature. May arrive as:
- One item: "add an item to NOTES about X"
- A small list: "add these three things to NOTES"
- A mid-conversation observation Claude raises and the user agrees to track

# Steps

## 1. Read the index and learn the current state

Read `NOTES.md` (the index) end to end. Note:
- The track headings (e.g. the active sprint, "Ops floor",
  "Ops & reliability", "WMATA depth & UX", "Deferred / trigger-based") —
  the new item needs a home under one of these, or a new track if no fit.
- The highest currently-visible `NOTES-N`. Then verify against history:
  ```bash
  git log --all -p -- NOTES.md notes/ | grep -oE "NOTES-[0-9]+" | sort -t- -k2 -n -u | tail -1
  ```
  Numbers are stable forever (closed items are deleted but their numbers
  remain reserved). The new item takes **max(current, historical) + 1**.
- The formatting conventions: open one or two existing `notes/*.md`
  files as templates — `#` title, severity/effort lines, paragraph(s)
  of explanation, optional `## Dependencies`.

## 2. Check for duplicates — semantic match, not keyword

For each candidate idea, scan existing entries (the index lines in
`NOTES.md`, plus `grep -ril <keywords> notes/` into the item bodies).
A duplicate isn't just exact-string overlap — it's the same underlying
work item phrased differently. Examples:

- Candidate "speed up the bunching pipeline" overlaps with an existing
  NOTES-X about "parallelize batch jobs"? → likely duplicate, ask user.
- Candidate "add tooltips to KPI cards" and existing NOTES-Y "Glossary
  page for transit terms"? → related but distinct, OK to add separately.

If you find a duplicate or partial overlap, **stop and report it to the
user**: name the existing NOTES-N, quote one sentence, and ask whether
to (a) skip, (b) add anyway as a related-but-distinct item, or
(c) expand/update the existing item instead. Don't blindly append.

## 3. Decide severity and effort

Add two header lines to the new entry. They mean different things:

- **Severity** — impact if this stays undone. Existing convention is
  `low | medium | high` with an optional parenthetical caveat
  (e.g. `(deferred — needs ≥14 days of data)`, `(data durability — single
  point of failure today)`). Match that style.
- **Effort** — rough work size. Use the same `low | medium | high`
  vocabulary so it reads consistently:
  - **low** — single-file change, < half a day's work, no migration.
  - **medium** — multi-file change or a small new module / endpoint,
    half a day to ~3 days.
  - **high** — new subsystem, schema change, multi-PR sequence, or
    spans the stack (backend + pipelines + frontend).

If genuinely unsure, pick the higher value and add a parenthetical
("Effort: medium (unknown — depends on whether X is already in place)").
Overestimating is cheap; underestimating misleads future planning.

## 4. Pick a track placement

Match the idea to an existing track heading in the index. Loose
mapping:

- Directly advances the north star (see the index's North star
  section) → the active sprint track, sequenced against its existing
  items.
- Ops / infra / reliability → "Ops & reliability" (or "Ops floor" only
  if it prevents a known recurring failure — that bar is high).
- UI / metrics depth on a single agency → "WMATA depth & UX".
- Only matters under a future trigger (public deploy, scale) →
  "Deferred / trigger-based".

If no track is a clean fit:
- **Single item with no home** — propose a new track to the user
  before creating it. A new track just to hold one entry is usually
  wrong.
- **Batch of thematic items (3+) with no home** — go ahead and create
  a clearly-named new track. Call it out explicitly in the
  return message so the user can rename or rehome it if they don't
  like the framing. Asking permission for every batch is friction
  the user has already opted out of by asking for the batch.

## 5. Write the entry

Two writes are needed:

**(a)** An index line under the chosen track in `NOTES.md`, matching
the existing one-line format:
```markdown
- [NOTES-N](notes/NOTES-N.md) Short title — sev low|medium|high / eff low|medium|high — unblocked|blocked by NOTES-M|short caveat
```

**(b)** The body file `notes/NOTES-N.md`:
```markdown
# NOTES-N. Short title

**Severity: low|medium|high** *(optional caveat)*.
**Effort: low|medium|high** *(optional caveat)*.

One to three paragraphs. Lead with what the work is and why it matters.
Include enough specifics (file paths, table names, API surface, concrete
acceptance criteria) that someone returning cold can scope it without
re-deriving the context. If there are known unknowns, name them.

## Dependencies

(Optional — only if there are real blocking deps. Don't fabricate.)
```

Match the prose style of existing entries: full sentences, concrete
references, no marketing voice. If the user described the idea in their
own words, preserve their framing where possible — they know what they
meant.

## 6. No changelog

There is no "Last edited" preamble to update — git history is the
record of when items were added and closed. Do not add one.

## 7. Return — do NOT commit

Leave the working tree dirty. NOTES.md edits ride on the next substantive
PR per the project's `update-notes-in-pr` workflow; standalone "add a
NOTES item" PRs are churn.

Report to the user:
- Which NOTES-N was assigned
- Which track it landed in
- Severity and effort
- One line confirming the files are edited but unstaged

If multiple items were added, list each.

# What not to do

- **Don't renumber existing items.** Numbers are stable forever — even
  closed ones. New items always take max+1.
- **Don't commit or open a PR.** The edit rides on the next substantive
  PR. Mentioning this explicitly in the return message helps the user
  remember.
- **Don't add trivial items.** A two-line cleanup that's faster to do
  than to track shouldn't go in NOTES.md. If the candidate idea is
  smaller than ~30 minutes of work and is genuinely obvious, suggest
  doing it now instead.
- **Don't fabricate dependencies or severity.** If you don't know
  whether something blocks on another item, say "Effort: medium
  (unknown dependency on X)" rather than inventing a dependency chain.
- **Don't reuse a closed item's number for a different idea.** If the
  user asks to "re-open NOTES-44", that's a different operation — it
  means restoring the closed item's text from git history, not creating
  a new entry under the old number. Flag the ambiguity and ask.

# Example

User: "we noticed the system metrics daily upsert silently overwrites
the row when run twice for the same date, with no warning. should
probably error or skip — add to NOTES."

Skill execution:
1. Read the NOTES.md index, find max NOTES-N = 62, confirm via git log.
2. Scan for duplicates — search the index and `grep -ril
   'system_metrics_daily\|upsert' notes/`. None match. OK to add.
3. Severity: low (no data loss, just confusing). Effort: low
   (one-file change in `pipelines/upsert_system_metrics_daily.py`).
4. Track: "Ops & reliability" (data-correctness, not sprint work).
5. Write `notes/NOTES-63.md` + its index line.
6. Return: "Added NOTES-63 (severity: low, effort: low) under
   'Ops & reliability'. notes/NOTES-63.md + the index line are edited
   but unstaged — they'll ride on your next substantive PR."
