# NOTES-130. migrate_all.py doesn't strip its own argv before invoking sibling migration scripts

**Severity: low** *(requires an operator to pass a flag to `migrate_all.py`
itself that happens to collide with a flag name one of its auto-discovered
`migrate_*.py` siblings also defines; no automatic/CI path is affected —
CI invokes `migrate_all.py` with no args)*.
**Effort: low** *(argv stripping or per-script namespacing in one file)*.

Surfaced during PR #221 review (NOTES-129). `scripts/migrate_all.py` calls
each auto-discovered `migrate_*.py` sibling's `main()`, and each sibling's
`main()` calls `argparse.ArgumentParser().parse_args()` with no explicit
argv — which defaults to reading `sys.argv[1:]`. `migrate_all.py` never
parses or strips its own argv before invoking the siblings, so any flag
passed to `migrate_all.py` itself passes straight through to *every*
sibling's argparse. `uv run python scripts/migrate_all.py --yes` would
therefore pass `--yes` to every migration script's own parser, not just the
one the operator meant.

This was a latent gap before PR #221; that PR widens the blast radius by
adding a second destructive script (`migrate_drop_vp_redundant_indexes.py`)
that defines the same `--yes` flag name as the existing
`migrate_drop_phase_f.py` — so an operator meaning to confirm one
destructive migration via `migrate_all.py --yes` would silently also
confirm the other's irreversible DROP.

## Fix

In `migrate_all.py`, either strip/reject unexpected argv before invoking
siblings (e.g. `sys.argv[1:] = []` around each `module.main()` call, or
document that `migrate_all.py` accepts no passthrough args and assert
that), or give siblings a way to be invoked programmatically with an
explicit empty argv instead of implicitly reading `sys.argv`. Whatever
lands should keep `migrate_all.py`'s CI usage (no args) working unchanged.

## Dependencies

None — self-contained fix to `scripts/migrate_all.py`.
