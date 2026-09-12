---
name: file-issue
description: File a newly identified improvement idea, feature, bug, refactor, or future-work item as a GitHub issue in this repo. Use whenever the user says "track this", "file an issue", "punch list", "let's not forget", "log this for later", "add to NOTES" (legacy phrasing), or surfaces an idea mid-conversation that is worth not losing but is not being implemented right now. Also use proactively when, during a code review or investigation, you yourself identify something non-blocking that the user should know about later — propose filing it rather than burying it in a comment that will scroll away.
---

The project's forward-looking punch list is the repo's GitHub Issues
(`gh issue list`). Filing an issue is the whole job: no index file to
edit, no number to mint, nothing to ride on a later PR. The skill's job
is to take a candidate idea, check it isn't already tracked, and file a
well-formed issue with the right labels.

# Inputs

A short description of the idea, fix, or feature. May arrive as:
- One item: "file an issue about X"
- A small list: "track these three things"
- A mid-conversation observation Claude raises and the user agrees to track

# Steps

## 1. Check for duplicates — semantic match, not keyword

```bash
gh issue list --state open --limit 100 --json number,title,labels
gh issue list --state all --search "<keywords>" --limit 10
```

A duplicate isn't just exact-string overlap — it's the same underlying
work item phrased differently. Examples:

- Candidate "speed up the bunching pipeline" vs an open issue
  "parallelize batch jobs"? → likely duplicate, ask user.
- Candidate "add tooltips to KPI cards" vs "Glossary page for transit
  terms"? → related but distinct, OK to file separately.

If you find a duplicate or partial overlap, **stop and report it to the
user**: name the issue number, quote one sentence, and ask whether to
(a) skip, (b) file anyway as a related-but-distinct issue, or
(c) comment on / edit the existing issue instead. Don't blindly file.

Also check closed issues: if the idea was closed as won't-fix or
superseded, say so rather than re-filing.

## 2. Decide severity and effort (labels)

- **`sev:low|medium|high`** — impact if this stays undone. Required.
- **`effort:low|medium|high`** — rough work size. Optional when
  genuinely unknown, but prefer picking one:
  - **low** — single-file change, < half a day's work, no migration.
  - **medium** — multi-file change or a small new module / endpoint,
    half a day to ~3 days.
  - **high** — new subsystem, schema change, multi-PR sequence, or
    spans the stack (backend + pipelines + frontend).

If genuinely unsure, pick the higher value and explain the uncertainty
in the body ("Effort: medium — unknown whether X is already in place").
Overestimating is cheap; underestimating misleads planning.

## 3. Pick a track (label)

- Ops / infra / reliability / data correctness → **`track:ops`**
- UI / metrics depth on a single agency → **`track:ux`**
- Only matters under a future trigger (public deploy, scale, audience
  beyond personal) → **`track:deferred`**

Add **`blocked`** if the work depends on another open issue or an
external trigger, and say what blocks it in the body.

If none fits, file with sev/effort only and say so in the return
message — don't invent a new `track:*` label for one item. (Three or
more thematically related items with no home justify a new label:
`gh label create track:<name>`; call it out so the user can rename.)

## 4. Write the issue

```bash
gh issue create --title "<short title>" \
  --label "track:ops,sev:low,effort:medium" \
  --body-file <tmpfile>
```

Body shape (the repo's issue template mirrors this):

```markdown
**Severity: low|medium|high** *(optional caveat)*.
**Effort: low|medium|high** *(optional caveat)*.

One to three paragraphs. Lead with what the work is and why it matters.
Include enough specifics (file paths, table names, API surface, concrete
acceptance criteria) that someone returning cold can scope it without
re-deriving the context. If there are known unknowns, name them.

## Dependencies

(Optional — only if there are real blocking deps. Reference other
issues as `#N`. Don't fabricate.)
```

Match the prose style of existing issues: full sentences, concrete
references, no marketing voice. If the user described the idea in their
own words, preserve their framing where possible — they know what they
meant.

Write the body to a file in the scratchpad and pass `--body-file`;
inline `--body` mangles backticks and multi-line markdown in zsh.

## 5. Return

Report to the user, per issue filed: the number and URL, the labels
applied, and one line on why it isn't a duplicate of anything open.

# What not to do

- **Don't file trivial items.** A two-line cleanup that's faster to do
  than to track shouldn't be an issue. If the candidate is smaller than
  ~30 minutes of work and genuinely obvious, suggest doing it now.
- **Don't fabricate dependencies or severity.** If you don't know
  whether something blocks on another issue, say so in the body rather
  than inventing a chain.
- **Don't reopen a closed issue for a different idea.** "Reopen #44"
  means `gh issue reopen 44` on the original text, not a new issue with
  old framing. Flag the ambiguity and ask.
- **Don't reference issues as `NOTES-N`.** That numbering is retired
  (2026-09-12); migrated issues carry a "Migrated from NOTES-N" line in
  their body for searchability. New references are `#N` / `issue #N`.

# Example

User: "we noticed the system metrics daily upsert silently overwrites
the row when run twice for the same date, with no warning. should
probably error or skip — track it."

Skill execution:
1. `gh issue list --search "upsert system_metrics_daily"` — nothing
   open or closed matches. OK to file.
2. Severity: low (no data loss, just confusing). Effort: low (one-file
   change in `pipelines/upsert_system_metrics_daily.py`).
3. Track: `track:ops` (data correctness, not UX).
4. `gh issue create --title "system_metrics_daily upsert silently
   overwrites on re-run" --label "track:ops,sev:low,effort:low"
   --body-file …`
5. Return: "Filed #263 (track:ops, sev:low, effort:low):
   <url>. No overlap with open issues."
