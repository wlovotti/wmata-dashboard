# NOTES-137. migrate_all.py doesn't propagate sibling failure/exit signals

**Severity: low (masked today because `init_db()` creates the columns
migrations backfill anyway; would bite a future ALTER-only migration).**
**Effort: low.**

Surfaced during review of PR #230 (the migrate_all argv-isolation fix,
NOTES-130). Pre-existing gaps in how `scripts/migrate_all.py` and its
`_run_sibling` helper handle a sibling's exit/return signals — not
touched by PR #230 and out of scope for it:

1. **A sibling's `sys.exit()` silently truncates the whole run.**
   `migrate_drop_phase_f.run_migration(confirm=False)` calls
   `sys.exit(0)` on its early-return path. `SystemExit` is a
   `BaseException`, not an `Exception`, so it escapes both the sibling's
   own `except Exception` handling and `migrate_all.main()`'s `for path
   in migrations` loop (there is no `except` around `_run_sibling(path)`
   at all). The process exits 0 partway through — observed at sibling
   ~12 of 16 — so every migration sorted after `migrate_drop_phase_f.py`
   silently never runs, and the "All N migration(s) completed." line
   never prints. CI passes only because `init_db()` independently
   creates the columns those later migrations would have backfilled; a
   future ALTER-only migration (no `init_db()` equivalent) sorting after
   `migrate_drop_*` would silently never execute in CI or by hand.

2. **A sibling's non-zero return from `main()` is discarded.**
   `_run_sibling` calls `module.main()` and ignores whatever it returns.
   A sibling that catches its own error and does `return 1` (rather than
   raising) still gets the `<== Finished {path.name}` success line, and
   `migrate_all.main()` still exits 0 for the whole run.

3. **Minor related hardening candidates** (same area, not required for
   the above two, worth doing together):
   - `_run_sibling` resets `sys.argv` around `module.main()`, but
     `spec.loader.exec_module(module)` runs *before* that reset — a
     sibling that reads `sys.argv` at module scope (rather than only
     inside `main()`) still sees migrate_all's original argv. Hoisting
     the reset above `exec_module` would close this.
   - `tests/test_migrate_all_argv.py`'s fake siblings register
     themselves in `sys.modules` under
     `_migrate_all__migrate_fake_*` (via `_run_sibling`'s
     `sys.modules[module_name] = module`) and are never cleaned up,
     leaking test-only module objects into `sys.modules` for the rest of
     the test process.

## Dependencies

None. Items 1 and 2 are the substantive gap (both in
`scripts/migrate_all.py`); item 3's two bullets are independent, smaller
hardening/cleanup follow-ups in the same file and
`tests/test_migrate_all_argv.py`. Good notes-batch candidate.
