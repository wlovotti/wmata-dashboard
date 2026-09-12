---
description: Use when closing a GitHub issue as a PR — the user names an issue number, asks to work the punch list, or runs back-to-back cycles under `/loop`.
---

This command drives one iteration of the punch-list cycle: read the
open issues (`gh issue list`) → confirm next task → dispatch a subagent
to implement → dispatch the `pr-reviewer` agent to review the diff →
watch CI → prompt for merge → cleanup. Task confirmation may be
satisfied by the user naming the issue at invocation (the parent echoes
the resolved title before dispatch instead of asking); merge approval
is never skippable. Everything else autonomous.

The command is composable: run it standalone for one cycle, or under
`/loop /issue-cycle` for autonomous back-to-back cycles. Under `/loop`
the parent context grows by only the subagent's short summary per
iteration — the heavy implementation transcript stays in the subagent
and dies with it. When there are 2-3 unblocked, small/mechanical issues
with disjoint footprints, `/issue-batch` runs them concurrently instead.

# Step 1 — Pre-flight (parent does this directly)

Confirm the working tree is ready:

```bash
git status --porcelain         # see below — empty OR only allowlisted paths
git branch --show-current      # must be `main`
git pull --ff-only
```

Pre-flight rule: the porcelain output may be empty, OR may contain
**only** allowlisted paths — `CLAUDE.md` at the repo root, or any file
under `.claude/commands/`, `.claude/agents/`, or `.claude/skills/`.
Any other dirty path (including a staged file, a new untracked file, or
a deletion) blocks the cycle. Capture the list of "riding-along" files
for later steps — call this `RIDE_ALONG_FILES`.

If the porcelain output, after stripping the leading status bytes,
contains any line outside the allowlist, **STOP**. Tell the user which
path blocked it and let them resolve before re-running. Likewise STOP
on a feature-branch checkout or a pull conflict. Do not auto-stash or
auto-checkout — those are destructive shortcuts for problems we should
investigate.

The riding-along files travel onto the feature branch naturally:
`git checkout -b` from main keeps unstaged changes in the working
tree, so the subagent inherits them and will commit them (see Step 4
prompt). The user gets a confirmation chance in Step 3 before any of
that happens, in case they'd rather stash instead.

# Step 2 — Read state (parent does this directly)

```bash
gh issue list --state open --limit 100 --json number,title,labels
```

Labels carry the triage: `track:ops` / `track:ux` / `track:deferred`,
`sev:*`, `effort:*`, and `blocked`. Open an issue body
(`gh issue view <N>`) only for the candidates you're actually
considering.

Track priority: **`track:ops`** first, then **`track:ux`**, then
unlabeled. **`track:deferred`** issues are eligible only when the user
names one explicitly. An issue is **blocked** if it carries the
`blocked` label or its `## Dependencies` section references a
still-open issue.

# Step 3 — Propose & confirm next task (HUMAN CHECKPOINT #1)

Pick a recommended next task. Selection rule of thumb:

1. Prefer **unblocked** issues
2. Among unblocked, prefer **earlier track** (ops > ux > unlabeled;
   deferred only on explicit user ask)
3. Among same track, prefer **higher severity** (high > medium > low)
4. Tiebreak on **smallest effort** (favor closing issues quickly)

Pick up to 2 alternates with different scope/risk profiles.

Ask via `AskUserQuestion`:

- Question: "Which issue should this cycle close?"
- Header: "Issue"
- Options: recommended (with "(Recommended)" suffix) + 2 alternates
- For each option, the description must include: track, severity,
  dependency status, and your scope estimate (small / medium / large)

The user may pick the recommended, an alternate, or "Other" with a
different issue number. Capture the chosen number for the rest of the
cycle.

If the user already named a specific issue when invoking the command
(check ARGUMENTS), that satisfies this checkpoint — skip the question,
but still echo a one-line confirmation of the resolved issue before
dispatching: "Closing #N: <issue title> — scope: <small/mechanical or
medium+/design-ambiguous>". This is a mistype guard, not a second
question — proceed to the scope gate and Step 4 immediately after
echoing it, without waiting for a reply.

**Scope gate (applies to whichever issue was chosen):** classify the
issue before dispatching.

- **Small / mechanical** — the issue body reads as a spec (named files,
  enumerated work items, no open design questions): proceed to Step 4.
- **Medium+ / design-ambiguous** — new architecture, schema changes,
  cross-cutting surfaces, or a body that says "needs its own spec/plan
  cycle": do NOT dispatch. Tell the user this issue outgrows the cycle
  and route it through **superpowers:brainstorming** then
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
  edit, test, PR). Sonnet is cheaper and faster; genuinely hard issues
  kick back via `STATUS: needs_user` and the parent re-routes.
- a self-contained prompt (template below)

The subagent loads the project's `CLAUDE.md` automatically, so the
prompt does NOT restate repo conventions — it just hands over the task
and the closing-PR checklist.

Subagent prompt template (fill in `{{N}}`, `{{issue_body}}` — the
output of `gh issue view {{N}}` — and `{{ride_along_files}}`, which is
either "none" or a comma-separated list of allowlisted paths from
Step 1):

```
Close GitHub issue #{{N}} in one PR. Issue title and body verbatim:

{{issue_body}}

Pre-existing uncommitted edits in the working tree: {{ride_along_files}}.
The parent already vetted these — they're intentional and should ride
on this PR. They will appear as already-modified files when you start.
Do NOT stash, revert, or `git checkout --` them. Commit them on the
feature branch alongside your substantive change (either folded into
the main commit if scope-related, or as a separate commit on the
same branch with message `chore: roll up doc / tooling drafts`
if unrelated).

Execute this checklist top-to-bottom. Do not deviate.

TURN INVARIANT: never end your turn while any background task or
monitor of yours is still pending — a subagent that ends its turn is
stopped, and nothing auto-resumes it, so a "wait for the notification"
plan just orphans the work until the parent notices and nudges. Either
run commands in the foreground, or background them and keep doing
checklist work in the same turn (e.g. run the full pytest suite in the
background while writing the PR body), collecting every result before
you finish. Practical default: foreground anything expected to finish
within ~5 minutes; background only commands that would outlive the
600 s foreground cap. Arming a Monitor does NOT satisfy this invariant —
a monitor cannot resume a stopped subagent. When you run out of other
checklist work while a background task is still pending, block on it
(e.g. TaskOutput with wait, or poll its status in a loop) until it
completes; never end the turn instead.

1. BRANCH. From `main`:
     git checkout -b <prefix>/issue-{{N}}-<short-slug>
   `<prefix>` ∈ {feature, fix, docs, refactor} per the issue's nature.
   The riding-along files (if any) will travel with the checkout —
   verify with `git status` before proceeding.

2. IMPLEMENT. Follow the issue's work items. Keep scope tight; do NOT
   refactor adjacent code.
   If the change adds or alters logic in src/, api/, or pipelines/:
   REQUIRED SUB-SKILL: superpowers:test-driven-development — write the
   failing test before the implementation. Doc, config, and shell
   plumbing changes are exempt (verify those in step 4 instead).

3. SIDE EFFECTS. If you discover a new issue worth tracking, file it
   with `gh issue create` (labels: one of track:ops / track:ux /
   track:deferred, plus sev:* and effort:*; search open issues first
   so you don't file a duplicate). Never open a second PR.

4. VERIFY (run in order; fix and re-run until each is clean):
     uv run pytest -m smoke
     uv run ruff check src/ scripts/ api/ pipelines/ tests/
     uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
   Match CI exactly — both ruff gates must include `tests/` or test-only
   lint errors will slip through and break the PR after push.
   If the change touches more than one small surface, also run the
   full suite against a scratch DB, never the default `.env` one:
     PG_TEST_DATABASE_URL=postgresql:///wmata_test_local \
       DATABASE_URL=postgresql:///wmata_test_local \
       SFMTA_DATABASE_URL=postgresql:///wmata_test_local uv run pytest
   (all three — DATABASE_URL and SFMTA_DATABASE_URL both need pointing
   at the scratch DB, or the production-DB guard in tests/conftest.py
   aborts the run; see issue #247.) `bin/test-with-pg` does this for
   you and is the preferred one-shot.

5. REFERENCE SWEEP. Prose elsewhere in the repo may describe #{{N}} as
   still open ("tracked as #{{N}}", "see #{{N}}", "deferred to #{{N}}"):
     grep -rn '#{{N}}\b' --include='*.md' --include='*.py' \
       --include='*.tsx' --include='*.ts' --include='*.sh' --include='*.yml'
   Reword those to point at the fix ("fixed in PR #M" — use the PR
   number once known). Leave `docs/superpowers/` and
   `docs/POSTMORTEM_*.md` untouched (frozen artifacts). The issue itself
   stays on GitHub as the durable record — nothing to delete.

6. COMMIT. Format:
     <prefix>: <short summary>

7. OPEN PR with `gh pr create`. Title mirrors the commit. Body MUST:
   - contain the line `Closes #{{N}}` so the merge closes the issue;
   - explain *why* the change was scoped this way — a one-line body is
     not acceptable.

8. RETURN ONLY these four fields (no preamble, no recap):
     PR_NUMBER: <int>
     PR_URL: <url>
     SUMMARY: one paragraph — what changed and what verification ran
     NEW_ISSUES: list of new issue numbers filed, or "none"

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
- prompt containing: the PR number, the PR branch name, the issue
  number, and the issue title + body verbatim (the same
  `{{issue_body}}` used in Step 4).

The agent reads every hunk, checks spec fidelity, confirms the PR body
carries `Closes #N`, sweeps for prose that still describes the issue
as open, runs the `code-review` skill on code-bearing diffs, and
returns a `VERDICT` plus findings tagged cosmetic / substantive /
fundamental.

The parent adjudicates the returned findings (it does NOT re-read the
diff): cosmetic → note them at the merge prompt; substantive → fix
via a follow-up dispatch to the same branch (never silently merge over
them); fundamental → STOP and surface to the user, same as a CI
failure. On `VERDICT: clean`, continue to step 5. If a finding's
severity tag seems miscalibrated or the verdict is ambiguous, the
parent may spot-check the specific hunks in question — that is the
exception, not the routine.

Any follow-up fix dispatch must restate the Step 4 TURN INVARIANT
verbatim in its prompt (never end a turn with pending background work;
foreground anything under ~5 minutes) — ad-hoc fix prompts are where
the stall-and-orphan failure mode has actually bitten.

# Step 5 — Watch CI (parent does this directly)

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

- Question: "PR #M for issue #N is green. Merge?"
- Header: "Merge"
- Options:
  - "Merge (squash + delete branch)" (recommended)
  - "Hold — I'll merge later"
  - "Abort — don't merge this cycle"

On "Merge":

```bash
gh pr merge <PR_NUMBER> --squash --delete-branch
```

GitHub closes the issue automatically via the `Closes #N` line. Verify
with `gh issue view <N> --json state`; if it is still open (the line
was missing or malformed), close it by hand with
`gh issue close <N> --comment "Closed by PR #M"`.

On "Hold": skip step 7's branch cleanup but do switch back to main.
End the cycle with a one-liner reminder of the open PR.

On "Abort": leave the PR open and the branch alone. End the cycle.

# Step 7 — Cleanup (parent does this directly)

After a successful merge:

```bash
git checkout main
git pull --ff-only
git fetch --prune
git branch -vv | awk '/: gone]/{print $1}' | xargs -r git branch -D
```

Confirm `git status` shows clean working tree on `main`.

# Step 8 — End message

Print one line, nothing more:

> Cycle complete: #N closed (PR #M merged). New issues filed: {{...}}.

If running under `/loop`, the driver will fire the next iteration
automatically — the next iteration's `Step 2` will list open issues
fresh (now without the just-closed one) and propose the next task.

If running standalone, suggest the user `/clear` and re-run
`/issue-cycle` for the next cycle, or `/compact` if they want to keep
session history but reduce context size.

# Invariants this command protects

- **The parent thread stays slim.** Heavy file reads, edits, test
  output, and lint logs all live in the subagent and don't bloat the
  parent's context across `/loop` iterations.
- **Task confirmation can be pre-satisfied; merge approval never is.**
  A user-named issue at invocation satisfies task confirmation, but
  the parent still echoes the resolved title before dispatch as a
  mistype guard. Merge approval has no such shortcut — an unintended
  merge is the highest-cost mistake in the cycle.
- **No auto-retry on CI failure.** A red CI is a signal to think, not
  to grind. The user decides whether to fix or abandon.
- **No unreviewed merges.** The dedicated `pr-reviewer` agent (Opus)
  reads the full PR diff — plus the `code-review` skill for
  code-bearing diffs — before the merge prompt, and the parent
  adjudicates its structured findings (step 4.5). Green CI alone does
  not qualify a PR for step 6.
- **Right-size the process.** The scope gate (step 3) keeps big or
  ambiguous issues out of blind dispatch — those route to
  superpowers:brainstorming / superpowers:writing-plans instead.
- **No destructive recovery.** If the working tree has any dirty path
  outside the riding-along allowlist (`CLAUDE.md`, `.claude/commands/`,
  `.claude/agents/`, `.claude/skills/`), refuse to start. If a merge
  conflict appears, surface and stop. Never `git stash` /
  `git reset --hard` / `git checkout -f` as a shortcut.
- **The closing PR closes the issue.** `Closes #N` in the PR body, and
  a body that explains the *why* — the PR is the durable record of the
  decision; the issue is the durable record of the problem.
- **Parallel cycles are allowed on disjoint issues.** Nothing in the
  repo is edited per-issue any more, so two cycles closing different
  issues may run concurrently in separate git worktrees as long as
  their code footprints don't overlap (check before dispatching; if
  they overlap, serialize).
