# NOTES-101. Parallel notes-cycle driver (batch mode)

**Severity: low (workflow efficiency — serial cycles still work).**
**Effort: medium (a `.claude/commands/` change plus dispatch-prompt
edits; no product code).**

The PR #186 split (index + per-item files) makes PRs that close
*different* items file-disjoint, but the `/notes-cycle` command is
still structurally serial. Four gaps block running cycles
concurrently, and each has a known fix:

1. **Pre-flight pins the primary checkout to `main`.** A second
   concurrent cycle can't run in the same working directory. Fix:
   batch mode dispatches each implementer into its own git worktree
   (the `Agent` tool's worktree isolation, or `git worktree add`),
   leaving the primary checkout untouched.
2. **The ride-along convention doesn't survive worktrees.** Dirty
   allowlisted files in the primary checkout do NOT travel into a
   fresh worktree, and two cycles must never both commit the same
   dirty `NOTES.md`. Fix: in batch mode, require a clean tree (or
   assign ride-alongs to exactly one designated cycle).
3. **NEW_NOTES number collisions.** Two concurrent subagents
   discovering side-issues would both compute the same "next unused
   NOTES-N". Fix: in batch mode, subagents do not create item files —
   they return NEW_NOTES as *proposals* in their structured return,
   and the parent files them serially after merges.
4. **Merge approvals and post-merge state.** Human merge approval is
   per-PR and never skippable; batch mode serializes the merge
   prompts, and after each squash-merge re-checks the remaining open
   PRs for mergeability (adjacent index-line deletions in the same
   track can conflict trivially) before prompting the next.

Work: add a batch mode to `/notes-cycle` (or a sibling
`/notes-batch` command): select up to N unblocked items whose
cross-reference sweeps don't overlap (grep each candidate's NOTES-N
across `notes/` + code before dispatch), dispatch implementers in
parallel worktrees, watch all CIs, serialize merge prompts, clean up
worktrees. The serial command remains the single-item path and the
fallback whenever overlap is detected.

## Dependencies

After PR #186 merges (same tooling files). Independent of product
code; suitable to design in a short interactive session since it
changes human-checkpoint ergonomics.
