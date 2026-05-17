---
name: add-to-notes
description: Add a newly identified improvement idea, feature, bug, refactor, or future-work item to this repo's NOTES.md punch list. Use whenever the user says "add to NOTES", "track this", "punch list", "let's not forget", "log this for later", or surfaces an idea mid-conversation that is worth not losing but is not being implemented right now. Also use proactively when, during a code review or investigation, you yourself identify something non-blocking that the user should know about later — propose adding it to NOTES.md rather than burying it in a comment that will scroll away.
---

NOTES.md is the project's forward-looking punch list. New items are added
here so they survive context loss between sessions and PRs. The skill's
job is to take a candidate idea, check it isn't already tracked, and append
a well-formatted entry — leaving the working tree dirty so the edit rides
on the next substantive PR (per the project's `update-notes-in-pr`
workflow; standalone reconciliation PRs are churn).

# Inputs

A short description of the idea, fix, or feature. May arrive as:
- One item: "add an item to NOTES about X"
- A small list: "add these three things to NOTES"
- A mid-conversation observation Claude raises and the user agrees to track

# Steps

## 1. Read NOTES.md and learn the current state

Read `NOTES.md` end to end. Note:
- The existing subsection structure under `## Active priorities` (e.g.
  "Trend & comparison", "Diagnostic outputs", "Independent of the redesign",
  "P5 — Cleanup") — the new item needs a home in one of these, or a new
  subsection if no fit.
- The highest currently-visible `NOTES-N`. Then verify against history:
  ```bash
  git log --all -p -- NOTES.md | grep -oE "NOTES-[0-9]+" | sort -t- -k2 -n -u | tail -1
  ```
  Numbers are stable forever (closed items are deleted but their numbers
  remain reserved). The new item takes **max(current, historical) + 1**.
- The formatting conventions: severity line, paragraph(s) of explanation,
  optional `### Dependencies`, `---` separator between entries.

## 2. Check for duplicates — semantic match, not keyword

For each candidate idea, scan existing entries (both the bulleted summary
lines under "Active priorities" and the detailed `## NOTES-N` sections).
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

## 4. Pick a subsection placement

Match the idea to an existing subsection under `## Active priorities`.
Loose mapping:

- UI / IA / page-level work → existing "Information architecture & navigation"
  or similar; create one if needed.
- Pipeline / metric / data-correctness work → "Independent of the redesign"
  is a safe default if no themed subsection fits.
- Pure cleanup / dead-code / refactor → "P5 — Cleanup".
- Infra / deployment / cloud → there's a cloud-migration thread
  (NOTES-48/49/50) — slot related work nearby.

If no subsection is a clean fit:
- **Single item with no home** — propose a new subsection to the user
  before creating it. A new subsection just to hold one entry is
  usually wrong; "Independent of the redesign" or "P5 — Cleanup" is
  often a better fit.
- **Batch of thematic items (3+) with no home** — go ahead and create
  a clearly-named new subsection. Call it out explicitly in the
  return message so the user can rename or rehome it if they don't
  like the framing. Asking permission for every batch is friction
  the user has already opted out of by asking for the batch.

## 5. Write the entry

Two writes are needed:

**(a)** A bullet under the chosen subsection in `## Active priorities`,
one or two sentences max:
```markdown
- **NOTES-N Short title.** One-sentence summary of the work and the
  motivating gap.
```

**(b)** A detailed section, appended in NOTES-N order at the bottom of
the file (before any trailing horizontal rules):
```markdown
## NOTES-N. Short title

**Severity: low|medium|high** *(optional caveat)*.
**Effort: low|medium|high** *(optional caveat)*.

One to three paragraphs. Lead with what the work is and why it matters.
Include enough specifics (file paths, table names, API surface, concrete
acceptance criteria) that someone returning cold can scope it without
re-deriving the context. If there are known unknowns, name them.

### Dependencies

(Optional — only if there are real blocking deps. Don't fabricate.)

---
```

Match the prose style of existing entries: full sentences, concrete
references, no marketing voice. If the user described the idea in their
own words, preserve their framing where possible — they know what they
meant.

## 6. Update the "Last edited" preamble

The top of NOTES.md has a paragraph starting `Last edited YYYY-MM-DD.
[Closed|Added|Updated] NOTES-N — …`. Update it to today's date (Eastern;
see `src/timezones.py` if uncertain — usually just today's date is fine)
and a summary of what was added.

Length scales with batch size:
- **One or two items** — 1-2 sentences. Lead with what changed; mention
  concrete file paths / table names where relevant.
- **Batch of 3+ items** — one short clause per item is fine. Name each
  NOTES-N with a few words on what it is so the preamble continues to
  read as a useful changelog. Group with `NOTES-N..M` shorthand if the
  numbers are contiguous; spell out individually otherwise.
- **Highlight surprises** — if any item has unusual severity, effort,
  or dependency relative to the others in the batch, note it (e.g.,
  "NOTES-66 medium severity after PR #115 near-miss").

Use `Added NOTES-N — …` for fresh items. Preserve the existing pattern
of mentioning concrete file paths / table names where relevant, so the
preamble continues to read as a useful changelog.

## 7. Return — do NOT commit

Leave the working tree dirty. NOTES.md edits ride on the next substantive
PR per the project's `update-notes-in-pr` workflow; standalone "add a
NOTES item" PRs are churn.

Report to the user:
- Which NOTES-N was assigned
- Which subsection it landed in
- Severity and effort
- One line confirming the file is edited but unstaged

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
1. Read NOTES.md, find max NOTES-N = 62, confirm via git log.
2. Scan for duplicates — search for "system_metrics_daily", "upsert",
   "idempotency". None match. OK to add.
3. Severity: low (no data loss, just confusing). Effort: low
   (one-file change in `pipelines/upsert_system_metrics_daily.py`).
4. Subsection: "Independent of the redesign" (no themed bucket fits).
5. Write bullet + detailed section as NOTES-63.
6. Update preamble: `Last edited 2026-05-17. Added NOTES-63 —
   upsert_system_metrics_daily silently overwrites on rerun; should
   warn or no-op when row exists.`
7. Return: "Added NOTES-63 (severity: low, effort: low) under
   'Independent of the redesign'. NOTES.md is edited but unstaged —
   it'll ride on your next substantive PR."
