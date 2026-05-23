# stop_events_v2 cutover implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch production trip_update derivation from snapshot-based to state-based by routing `derive_stop_events_from_state.py` writes into the canonical `stop_events` table, then retiring the legacy snapshot pipeline + table after a 7-day safety net.

**Architecture:** Three sequenced PRs over ~9 days. PR 1 adds an informational nightly comparison check in the daily batch. PR 2 (cutover, Mon 5/25) swaps the v2 pipeline's target to `stop_events`, removes the legacy step, removes the now-temporary comparison check, and updates the row-count guard. PR 3 (Phase F, Mon 6/01) retires collector dual-write to snapshots, drops `trip_update_snapshots`, and deletes legacy code. Plus discrete operational tasks (smoke tests, manual DROP commands, daily morning checks) interleaved between PRs.

**Tech Stack:** Python 3.12, PostgreSQL, SQLAlchemy, ruff, pytest, launchd

**Reference spec:** `docs/superpowers/specs/2026-05-23-stop-events-v2-cutover-design.md`

---

## Phase 1 — Monitoring PR (Sat 2026-05-23, must merge before 21:00 ET tonight)

### Task 1: Extract a threshold-check helper in `pipelines/run_daily_batch.py`

**Files:**
- Modify: `pipelines/run_daily_batch.py`
- Test: `tests/test_run_daily_batch_compare_check.py` (create new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_run_daily_batch_compare_check.py`:

```python
"""Tests for the comparison-output threshold check used by run_daily_batch."""

from pipelines.run_daily_batch import _check_comparison_thresholds


def test_clean_line_returns_none():
    """A clean 100% line with no diverging routes returns None (no WARN)."""
    line = "2026-05-22: 100.0% agreement (497,730/497,730), 5,058 v2-only rows, 0 routes with >1% disagreement"
    assert _check_comparison_thresholds(line) is None


def test_below_995_returns_reason():
    """Agreement below 99.5% returns a reason string."""
    line = "2026-05-22: 99.4% agreement (490,000/493,000), 5,058 v2-only rows, 0 routes with >1% disagreement"
    reason = _check_comparison_thresholds(line)
    assert reason is not None
    assert "99.4" in reason


def test_v2_only_above_2pct_returns_reason():
    """v2-only fraction above 2% of total returns a reason."""
    line = "2026-05-22: 100.0% agreement (490,000/490,000), 12,000 v2-only rows, 0 routes with >1% disagreement"
    reason = _check_comparison_thresholds(line)
    assert reason is not None
    assert "v2-only" in reason


def test_diverging_routes_returns_reason():
    """Any diverging route returns a reason."""
    line = "2026-05-22: 99.9% agreement (497,000/497,500), 5,000 v2-only rows, 1 routes with >1% disagreement"
    reason = _check_comparison_thresholds(line)
    assert reason is not None
    assert "diverging" in reason or "1" in reason


def test_unparseable_line_returns_reason():
    """A line that doesn't match the expected format returns a reason."""
    line = "something went wrong"
    reason = _check_comparison_thresholds(line)
    assert reason is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_run_daily_batch_compare_check.py -v`
Expected: FAIL with `ImportError: cannot import name '_check_comparison_thresholds'`

- [ ] **Step 3: Add the helper to `pipelines/run_daily_batch.py`**

Add this function near the top of `pipelines/run_daily_batch.py` (after imports, before the existing pipeline definitions):

```python
import re


_COMPARE_LINE_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}): "
    r"(?P<pct>[\d.]+)% agreement "
    r"\((?P<matched>[\d,]+)/(?P<total>[\d,]+)\), "
    r"(?P<v2_only>[\d,]+) v2-only rows, "
    r"(?P<diverging>\d+) routes with >1% disagreement"
)


def _check_comparison_thresholds(line: str) -> str | None:
    """Return a WARN reason string if the comparison line exceeds thresholds, else None.

    Thresholds (from 2026-05-23 cutover design spec):
    - agreement_pct >= 99.5
    - 0 diverging routes
    - v2-only rows <= 2% of total

    Args:
        line: A single output line from compare_old_vs_new_derivation.py.

    Returns:
        A short human-readable reason describing the violated threshold,
        or None if all thresholds pass.
    """
    m = _COMPARE_LINE_RE.match(line.strip())
    if not m:
        return f"unparseable comparison output: {line.strip()[:80]!r}"
    pct = float(m.group("pct"))
    total = int(m.group("total").replace(",", ""))
    v2_only = int(m.group("v2_only").replace(",", ""))
    diverging = int(m.group("diverging"))
    if pct < 99.5:
        return f"agreement {pct}% below 99.5% bar"
    if diverging > 0:
        return f"{diverging} route(s) with >1% disagreement"
    if total > 0 and (v2_only / total) > 0.02:
        return f"v2-only fraction {v2_only / total * 100:.2f}% above 2% bar"
    return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_run_daily_batch_compare_check.py -v`
Expected: PASS — 5 tests pass.

- [ ] **Step 5: Run ruff**

Run: `uv run ruff check pipelines/run_daily_batch.py tests/test_run_daily_batch_compare_check.py && uv run ruff format --check pipelines/run_daily_batch.py tests/test_run_daily_batch_compare_check.py`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git checkout -b feature/notes-72-monitoring-pr
git add pipelines/run_daily_batch.py tests/test_run_daily_batch_compare_check.py
git commit -m "feat(batch): add comparison threshold helper for NOTES-72 monitoring"
```

### Task 2: Wire the comparison step into the batch loop

**Files:**
- Modify: `pipelines/run_daily_batch.py`

- [ ] **Step 1: Locate the per-date loop**

Read lines 320-410 of `pipelines/run_daily_batch.py` to understand the per-(pipeline, date) execution loop and the existing `_v2_row_count_guard` invocation pattern.

- [ ] **Step 2: Add a housekeeping invocation after both derivations complete**

After both `derive_stop_events_*` steps complete for a service_date but BEFORE the next pipeline's iteration (specifically: after the v2 row-count guard succeeds), add an inline subprocess invocation of `compare_old_vs_new_derivation.py`. The exact insertion point and code:

Find this block (around line 388 per the current file):

```python
                # Post-step row-count guard for the v2 derivation. Catches the
                # silent-zero failure mode where the pipeline exits 0 but writes
                # nothing. Reports via log_handle; failure flips failure_count.
                if not _v2_row_count_guard(service_date, log_handle):
                    failure_count += 1
                    ok = False
```

Immediately after that block (still inside the per-date loop), add:

```python
                # NOTES-72 Phase D monitoring: run the comparison and log a
                # WARN line if any threshold is exceeded. Informational only;
                # does NOT fail the batch. Removed at cutover (this file's
                # next PR). See docs/superpowers/specs/2026-05-23-stop-events-v2-cutover-design.md
                if ok:
                    _run_comparison_check(service_date, log_handle)
```

Then add this helper function alongside `_v2_row_count_guard` (so around line 270):

```python
def _run_comparison_check(service_date: date_type, log_handle) -> None:
    """Invoke compare_old_vs_new_derivation.py and write a WARN if thresholds exceeded.

    Informational only — never fails the batch. Output (one line) is captured
    and parsed via _check_comparison_thresholds. A violation writes a
    grep-able ``WARN compare_old_vs_new_derivation: <reason>`` line.

    Args:
        service_date: The service date to compare.
        log_handle: Open file handle for the daily batch log.
    """
    import subprocess as _sp
    cmd = [
        sys.executable,
        "-m",
        "pipelines.compare_old_vs_new_derivation",
        "--date",
        service_date.isoformat(),
    ]
    log_handle.write(f"\n$ {' '.join(cmd)}\n")
    log_handle.flush()
    try:
        proc = _sp.run(cmd, capture_output=True, text=True, check=False, timeout=300)
    except _sp.TimeoutExpired:
        log_handle.write("WARN compare_old_vs_new_derivation: timed out after 300s\n")
        log_handle.flush()
        return
    log_handle.write(proc.stdout)
    if proc.stderr:
        log_handle.write(proc.stderr)
    if proc.returncode != 0:
        log_handle.write(
            f"WARN compare_old_vs_new_derivation: exited {proc.returncode}\n"
        )
        log_handle.flush()
        return
    last_line = (proc.stdout.strip().splitlines() or [""])[-1]
    reason = _check_comparison_thresholds(last_line)
    if reason:
        log_handle.write(f"WARN compare_old_vs_new_derivation: {reason}\n")
    log_handle.flush()
```

- [ ] **Step 3: Run a local smoke**

Run: `uv run python -c "
from pathlib import Path
from datetime import date as date_type
import sys
sys.path.insert(0, '.')
from pipelines.run_daily_batch import _run_comparison_check

with open('/tmp/test_compare.log', 'w') as f:
    _run_comparison_check(date_type(2026, 5, 22), f)

print(open('/tmp/test_compare.log').read())
"`

Expected: the file output should contain the line `2026-05-22: 100.0% agreement (497,730/497,730), 5,058 v2-only rows, 0 routes with >1% disagreement` and NO `WARN` lines (since we're at 100%).

- [ ] **Step 4: Run ruff**

Run: `uv run ruff check pipelines/run_daily_batch.py && uv run ruff format --check pipelines/run_daily_batch.py`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add pipelines/run_daily_batch.py
git commit -m "feat(batch): invoke compare_old_vs_new_derivation as nightly housekeeping (NOTES-72)"
```

### Task 3: Open + merge the monitoring PR

- [ ] **Step 1: Push the branch**

```bash
git push -u origin feature/notes-72-monitoring-pr
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "feat(batch): add stop_events_v2 comparison as nightly housekeeping (NOTES-72)" --body "$(cat <<'EOF'
## Summary
- Adds an informational comparison step in run_daily_batch.py: runs compare_old_vs_new_derivation.py per service date and writes a WARN line to the daily-batch log if thresholds exceeded.
- Thresholds (per 2026-05-23 cutover design spec): agreement < 99.5%, any route > 1% diverging, or v2-only fraction > 2% of total.
- Does NOT fail the batch on threshold breach — the WARN is purely informational and grep-able.
- New helper `_check_comparison_thresholds` is unit-tested.

## Why
Phase E cutover (Mon 2026-05-25) depends on Sat 5/23 + Sun 5/24 nightly comparisons passing the bar. This change makes those comparisons run automatically as part of the existing 03:00 batch and visible in the log.

This step is TEMPORARY — it will be removed in the cutover PR once `stop_events_v2` no longer exists.

## Test plan
- [x] Unit tests for the threshold helper (`tests/test_run_daily_batch_compare_check.py`)
- [x] Local smoke test of `_run_comparison_check` against 2026-05-22 (passes, no WARN)
- [ ] Tonight's 03:00 batch runs; expect a clean comparison line + no WARN in `logs/daily_batch_2026-05-24.log` (covering service date 5/23)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Wait for CI**

Watch: `gh pr checks --watch`
Expected: all checks pass.

- [ ] **Step 4: Merge the PR**

```bash
gh pr merge --squash --delete-branch
```

- [ ] **Step 5: Pull main**

```bash
git checkout main
git pull
```

Phase 1 complete. Next nightly batch (Sun 2026-05-24 03:00 ET) will produce the first comparison output.

---

## Phase 2 — Wait window (Sun 2026-05-24 → Mon 2026-05-25 morning)

### Task 4: Sunday morning check (Sun 2026-05-24, after 04:00 ET)

- [ ] **Step 1: Read the batch log**

Run: `grep -E "compare_old_vs_new|WARN compare" logs/daily_batch_2026-05-24.log`
Expected: one comparison line for service date 2026-05-23 (Sat) with `100.0% agreement` or similar high percentage, no WARN line.

- [ ] **Step 2: Confirm thresholds**

Inspect the comparison line. Confirm:
- agreement_pct ≥ 99.5
- 0 routes with >1% disagreement
- v2-only count / total ≤ 2% (and ideally in 0.7%–1.3% range per spec)

- [ ] **Step 3: Note the result**

If pass: proceed to Task 5.
If fail: stop the cutover. Investigate the divergence. Defer cutover.

### Task 5: Monday morning check + go/no-go (Mon 2026-05-25, after 04:00 ET)

- [ ] **Step 1: Read the batch log**

Run: `grep -E "compare_old_vs_new|WARN compare" logs/daily_batch_2026-05-25.log`
Expected: one comparison line for service date 2026-05-24 (Sun) with high agreement, no WARN line.

- [ ] **Step 2: Confirm both weekend days**

Confirm Sat 5/23 (from Sun batch) AND Sun 5/24 (from Mon batch) both passed the bar.

- [ ] **Step 3: Go/no-go decision**

Both days pass: proceed to Phase 3.
Either day fails: stop. Investigate. Defer Phase 3.

---

## Phase 3 — Cutover PR (Mon 2026-05-25 midday)

### Task 6: Cutover changes to `pipelines/run_daily_batch.py`

**Files:**
- Modify: `pipelines/run_daily_batch.py`
- Test: `tests/test_run_daily_batch_compare_check.py` (delete — the helper is removed)

- [ ] **Step 1: Create the branch**

```bash
git checkout -b feature/notes-72-cutover
```

- [ ] **Step 2: Locate the pipeline definitions**

Read lines 70-100 of `pipelines/run_daily_batch.py` — the pipeline step definitions list.

- [ ] **Step 3: Swap v2 derivation target and remove legacy step**

In the pipeline definitions, locate the v2 derivation step (currently has `"extra_args": ["--target-table", "stop_events_v2"]`). **Remove** the `extra_args` key entirely — `stop_events` is the default target for `derive_stop_events_from_state.py`.

Also locate the legacy `derive_stop_events_trip_updates` step in the same pipeline-definitions list and remove it entirely (the whole step dict).

- [ ] **Step 4: Remove the comparison-check invocation**

Find the block added in Task 2:

```python
                if ok:
                    _run_comparison_check(service_date, log_handle)
```

Delete it.

Also delete the `_run_comparison_check` helper function (added in Task 2) and the `_COMPARE_LINE_RE` regex and `_check_comparison_thresholds` function (added in Task 1). They're no longer used.

- [ ] **Step 5: Update the row-count guard target**

Find `_v2_row_count_guard` (around line 236). Update its SQL and naming:

```python
def _stop_events_trip_update_row_count_guard(service_date: date_type, log_handle) -> bool:
    """Return True if stop_events has trip_update rows for ``service_date``.

    Load-bearing guard from the 2026-05-16→19 silent-failure incident:
    derive_stop_events_from_state.py is allowed to exit 0 with zero rows
    written, which is the failure mode we explicitly need to surface.
    Kept post-cutover (NOTES-72) — same protection, now watching the
    canonical table since stop_events_v2 was retired.

    Args:
        service_date: The service date to check.
        log_handle: Open file handle for the daily batch log.

    Returns:
        True if non-zero trip_update rows present, False otherwise.
    """
    from src.database import get_session
    db = get_session()
    try:
        n = db.execute(
            text(
                "SELECT COUNT(*) FROM stop_events "
                "WHERE service_date = :d AND source = 'trip_update'"
            ),
            {"d": service_date.isoformat()},
        ).scalar() or 0
    finally:
        db.close()
    if n == 0:
        log_handle.write(
            f"GUARD stop_events has 0 trip_update rows for {service_date.isoformat()} "
            "after v2 derivation exited 0 — silent-failure guard tripped\n"
        )
        log_handle.flush()
        return False
    log_handle.write(
        f"GUARD stop_events has {n} trip_update rows for {service_date.isoformat()}\n"
    )
    log_handle.flush()
    return True
```

Then find the call site and rename `_v2_row_count_guard(...)` → `_stop_events_trip_update_row_count_guard(...)`.

- [ ] **Step 6: Delete the obsolete test file**

```bash
rm tests/test_run_daily_batch_compare_check.py
```

- [ ] **Step 7: Run ruff and tests**

```bash
uv run ruff check pipelines/run_daily_batch.py
uv run ruff format --check pipelines/run_daily_batch.py
uv run pytest -m smoke
```

Expected: ruff clean; smoke tests pass.

- [ ] **Step 8: Commit**

```bash
git add pipelines/run_daily_batch.py tests/test_run_daily_batch_compare_check.py
git commit -m "feat(batch): cutover NOTES-72 — v2 writes to stop_events; drop legacy step (Phase E)"
```

### Task 7: Update any tests referencing `stop_events_v2`

**Files:**
- Modify: `tests/test_compare_derivations.py` (review for semantic deps on v2 vs stop_events)
- Modify: `tests/test_migrate_stop_events_v2.py` (decide whether to keep, mark skip, or delete)
- Modify: `tests/test_derive_stop_events_from_state.py` (review for target-table assumptions)

- [ ] **Step 1: Audit test files for stop_events_v2 references**

Run: `grep -rn "stop_events_v2" tests/`
Document the hits.

- [ ] **Step 2: Review and update**

For each hit, decide:
- Test depends on v2 as a *side table* with the old derivation also running → either delete the test (no longer meaningful) or update to test the v2 pipeline's write to `stop_events`.
- Test is about the v2 pipeline's correctness independent of which table → leave as-is if it uses an in-memory test table; update target if it uses a real `stop_events_v2` reference.
- Test is the `test_migrate_stop_events_v2.py` file specifically → mark with `pytest.mark.skip(reason="stop_events_v2 will be dropped at cutover")` for now; delete in Phase F.

- [ ] **Step 3: Run the full test suite**

```bash
bin/test-with-pg
```

Expected: all tests pass (or skipped where annotated).

- [ ] **Step 4: Commit**

```bash
git add tests/
git commit -m "test: update v2-dependent tests for stop_events cutover (NOTES-72)"
```

### Task 8: Open + merge the cutover PR

- [ ] **Step 1: Push**

```bash
git push -u origin feature/notes-72-cutover
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "feat(batch): cutover stop_events_v2 → stop_events (NOTES-72 Phase E)" --body "$(cat <<'EOF'
## Summary
- v2 derivation now writes to canonical `stop_events` (was `stop_events_v2`)
- Legacy `derive_stop_events_trip_updates` step removed from the nightly batch
- Comparison-check step from the previous PR removed (no longer needed)
- Row-count guard updated to watch trip_update rows in `stop_events` (was `stop_events_v2`); load-bearing protection from 5/16→19 silent failure preserved
- `stop_events_v2` table will be DROPPED manually after the next nightly batch confirms clean execution

## Why
3-day pre-cutover comparison + Sat/Sun comparisons all clean (≥99.5%, 0 diverging routes, stable v2-only fraction). Per the 2026-05-23 cutover design spec, this PR executes Phase E.

## Test plan
- [x] Smoke tests pass (`uv run pytest -m smoke`)
- [x] Full PG suite passes (`bin/test-with-pg`)
- [x] Ruff clean
- [ ] Smoke re-derive 5/20-5/22 manually after merge (proves v2 writes to stop_events without error)
- [ ] Tue 5/26 03:00 batch runs cleanly under launchd
- [ ] Tue morning: row-count guard reports non-zero trip_update rows for 5/25

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Wait for CI**

Watch: `gh pr checks --watch`
Expected: all checks pass.

- [ ] **Step 4: Merge**

```bash
gh pr merge --squash --delete-branch
git checkout main
git pull
```

---

## Phase 4 — Cutover-day operations (Mon 2026-05-25 afternoon → Tue 2026-05-26 morning)

### Task 9: Smoke-test re-derive of 5/20–5/22

- [ ] **Step 1: Snapshot row counts BEFORE**

```bash
psql -d wmata_dashboard -c "
SELECT service_date, source, COUNT(*)
FROM stop_events
WHERE service_date IN ('2026-05-20', '2026-05-21', '2026-05-22')
  AND source = 'trip_update'
GROUP BY service_date, source
ORDER BY service_date;
" | tee /tmp/pre_smoke_counts.txt
```

Expected output approximately:
```
 service_date | source      | count
--------------+-------------+--------
 2026-05-20   | trip_update | 504159
 2026-05-21   | trip_update | 505467
 2026-05-22   | trip_update | 497730
```

Save the actual counts.

- [ ] **Step 2: Re-derive 5/20**

Run: `uv run python -m pipelines.derive_stop_events_from_state --all-routes --date 2026-05-20 --target-table stop_events`
Expected: exits 0 with progress logs; no errors.

- [ ] **Step 3: Re-derive 5/21**

Run: `uv run python -m pipelines.derive_stop_events_from_state --all-routes --date 2026-05-21 --target-table stop_events`
Expected: exits 0.

- [ ] **Step 4: Re-derive 5/22**

Run: `uv run python -m pipelines.derive_stop_events_from_state --all-routes --date 2026-05-22 --target-table stop_events`
Expected: exits 0.

- [ ] **Step 5: Snapshot row counts AFTER**

```bash
psql -d wmata_dashboard -c "
SELECT service_date, source, COUNT(*)
FROM stop_events
WHERE service_date IN ('2026-05-20', '2026-05-21', '2026-05-22')
  AND source = 'trip_update'
GROUP BY service_date, source
ORDER BY service_date;
" | tee /tmp/post_smoke_counts.txt
```

- [ ] **Step 6: Diff the snapshots**

```bash
diff /tmp/pre_smoke_counts.txt /tmp/post_smoke_counts.txt
```

Expected: no diff (UPSERT writes byte-identical rows). If counts changed, the cutover has a row-construction bug — STOP and roll back per Tier 1 in the spec.

### Task 10: Wait for Tue 5/26 03:00 nightly batch

- [ ] **Step 1: Wait**

The batch runs unattended at 03:00 Tue under launchd. Output lands in `logs/daily_batch_2026-05-26.log`.

- [ ] **Step 2: Tue morning — read the batch log**

Run: `grep -E "GUARD|done|failure|ERROR" logs/daily_batch_2026-05-26.log`
Expected:
- `GUARD stop_events has <non-zero> trip_update rows for 2026-05-25`
- `========== run_daily_batch done — 0 (pipeline, date) failures ==========`

- [ ] **Step 3: Sanity-check trip_update row count**

```bash
psql -d wmata_dashboard -c "
SELECT COUNT(*) FROM stop_events
WHERE service_date = '2026-05-25' AND source = 'trip_update';
"
```

Expected: a value in the 497k–510k range (matches the recent baseline).

- [ ] **Step 4: Go/no-go decision**

If clean: proceed to Task 11.
If anything looks wrong: STOP. Roll back per Tier 1 in the spec (`git revert` the cutover PR). Do NOT proceed to DROP TABLE.

### Task 11: DROP `stop_events_v2`

- [ ] **Step 1: Confirm no live readers**

Run: `grep -rn "stop_events_v2" pipelines/ src/ api/ scripts/ tests/ 2>/dev/null | grep -v __pycache__ | grep -v migrate_create_stop_events_v2 | grep -v check_schema_drift`
Expected: empty (all live references should have been removed in Phase 3; `migrate_create_stop_events_v2.py` and `check_schema_drift.py` remain but are inert).

- [ ] **Step 2: Final row-count snapshot of v2 (for historical record)**

```bash
psql -d wmata_dashboard -c "
SELECT service_date, COUNT(*) FROM stop_events_v2 GROUP BY service_date ORDER BY service_date;
" | tee /tmp/stop_events_v2_final_counts.txt
```

- [ ] **Step 3: DROP TABLE**

```bash
psql -d wmata_dashboard -c "DROP TABLE stop_events_v2;"
```

Expected: `DROP TABLE`.

- [ ] **Step 4: Confirm**

```bash
psql -d wmata_dashboard -c "\d stop_events_v2"
```

Expected: `Did not find any relation named "stop_events_v2".`

Phase 4 complete. Cutover is irreversible from a side-table perspective; rollback now requires Tier 2 procedure (re-derive via legacy pipeline reading from `trip_update_snapshots`).

---

## Phase 5 — Phase F countdown (Tue 2026-05-26 → Mon 2026-06-01)

### Task 12: Daily morning checks (7 mornings: Tue 5/26 → Mon 6/01)

For EACH of the 7 mornings, perform this check. Mark one checkbox per morning.

- [ ] **Tue 2026-05-26 morning check**
- [ ] **Wed 2026-05-27 morning check**
- [ ] **Thu 2026-05-28 morning check**
- [ ] **Fri 2026-05-29 morning check**
- [ ] **Sat 2026-05-30 morning check**
- [ ] **Sun 2026-05-31 morning check**
- [ ] **Mon 2026-06-01 morning check**

**Procedure (each morning, ~30 sec):**

1. Tail the batch log:
   ```
   grep -E "GUARD|done|failure|ERROR" logs/daily_batch_<YYYY-MM-DD>.log
   ```
   Expected: GUARD non-zero, `done — 0 (pipeline, date) failures`.

2. Sanity-check the trip_update row count for yesterday's service date:
   ```
   psql -d wmata_dashboard -c "
   SELECT COUNT(*) FROM stop_events
   WHERE service_date = '<yesterday>' AND source = 'trip_update';
   "
   ```
   Expected: 497k–510k range.

3. Spot-check `system_metrics_daily` against the prior same-DoW value:
   ```
   psql -d wmata_dashboard -c "
   SELECT service_date, agency_otp_on_time_pct, ewt_minutes
   FROM system_metrics_daily
   WHERE service_date >= '<yesterday minus 28 days>'
   ORDER BY service_date DESC LIMIT 8;
   "
   ```
   Expected: yesterday's row within ±2pp of the same-DoW value 7 days prior.

4. If any of (1)/(2)/(3) is off: stop the countdown, investigate, roll back per Tier 2 if needed.

### Task 13: Phase F go/no-go (Mon 2026-06-01)

- [ ] **Step 1: Verify all 7 morning checks were clean**

If yes: proceed to Phase 6.
If any morning showed a signal: investigate root cause first. Phase F can be deferred without cost (the dual-write keeps trip_update_snapshots populated and the legacy code is still present).

---

## Phase 6 — Phase F PR (Mon 2026-06-01)

### Task 14: Remove collector dual-write to `trip_update_snapshots`

**Files:**
- Modify: `src/wmata_collector.py` (around line 620, `_save_trip_updates`)
- Modify: `tests/test_collector_dual_write.py` (update to reflect new single-write reality)

- [ ] **Step 1: Create the branch**

```bash
git checkout -b feature/notes-72-phase-f
```

- [ ] **Step 2: Audit the existing `_save_trip_updates` code**

Read `src/wmata_collector.py:620-720` to understand the dual-write structure. The method should currently write to both `trip_update_snapshots` and `trip_update_state` (via the UPSERT path).

- [ ] **Step 3: Update the existing test to define the new contract**

Update `tests/test_collector_dual_write.py` to assert that `_save_trip_updates` writes ONLY to `trip_update_state` (not `trip_update_snapshots`). Document any tests that depend on snapshots existing — those need their assertion flipped to "trip_update_snapshots is NOT written."

Specific change to make: any test that currently does `db.execute(text("SELECT COUNT(*) FROM trip_update_snapshots WHERE ..."))` and asserts > 0 should be flipped to assert == 0 (or removed entirely if the test was specifically about snapshots).

- [ ] **Step 4: Run the test to verify it fails**

Run: `uv run pytest tests/test_collector_dual_write.py -v`
Expected: FAIL — current code still writes to snapshots.

- [ ] **Step 5: Modify `_save_trip_updates`**

In `src/wmata_collector.py:_save_trip_updates`, remove the INSERT-to-`trip_update_snapshots` code path. Keep:
- The UPSERT to `trip_update_state`
- The JSONL archive append
- The dedup state management

The method should retain its signature and return value semantics (count of trips processed); only the snapshots write goes away.

- [ ] **Step 6: Run the test to verify it passes**

Run: `uv run pytest tests/test_collector_dual_write.py -v`
Expected: PASS.

- [ ] **Step 7: Ruff**

Run: `uv run ruff check src/wmata_collector.py tests/test_collector_dual_write.py && uv run ruff format --check src/wmata_collector.py tests/test_collector_dual_write.py`
Expected: no errors.

- [ ] **Step 8: Commit**

```bash
git add src/wmata_collector.py tests/test_collector_dual_write.py
git commit -m "refactor(collector): remove trip_update_snapshots write path (NOTES-72 Phase F)"
```

### Task 15: Delete legacy pipeline files

**Files:**
- Delete: `pipelines/derive_stop_events_trip_updates.py`
- Delete: `pipelines/compare_old_vs_new_derivation.py`
- Delete: `pipelines/archive_trip_update_snapshots.py`
- Delete: `scripts/migrate_create_stop_events_v2.py`
- Delete: `tests/test_compare_derivations.py`
- Delete: `tests/test_migrate_stop_events_v2.py`

- [ ] **Step 1: Confirm no remaining references**

```bash
grep -rn -E "derive_stop_events_trip_updates|compare_old_vs_new_derivation|archive_trip_update_snapshots|migrate_create_stop_events_v2" pipelines/ src/ api/ scripts/ tests/ docs/ 2>/dev/null | grep -v __pycache__ | grep -v "^docs/superpowers/specs/"
```

Expected: empty (references in old specs are OK; references in live code are not).

- [ ] **Step 2: Delete the files**

```bash
rm pipelines/derive_stop_events_trip_updates.py
rm pipelines/compare_old_vs_new_derivation.py
rm pipelines/archive_trip_update_snapshots.py
rm scripts/migrate_create_stop_events_v2.py
rm tests/test_compare_derivations.py
rm tests/test_migrate_stop_events_v2.py
```

- [ ] **Step 3: Run the full suite**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
bin/test-with-pg
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: delete legacy snapshot pipeline + comparison + v2 migration files (NOTES-72 Phase F)"
```

### Task 16: Update CLAUDE.md and NOTES.md

**Files:**
- Modify: `CLAUDE.md` (remove dual-derivation paragraph and silent-failure-guard exposition that no longer applies)
- Modify: `NOTES.md` (close NOTES-72 or mark Phase E/F complete; link this spec + plan)

- [ ] **Step 1: Identify the obsolete CLAUDE.md content**

Read lines 79-90 of `CLAUDE.md` and the silent-failure-guard exposition (around line 240+ per the file at the time the spec was written). These describe a dual-derivation architecture that no longer exists.

- [ ] **Step 2: Remove or rewrite**

The `stop_events.source` dual-source paragraph itself still applies (proximity + trip_update is still the model). What changes is:
- No more "old" trip_update derivation reading from `trip_update_snapshots`
- No more `stop_events_v2` side table
- The silent-failure guard exposition can stay (the guard is still load-bearing), but should reference the canonical table.

Edit `CLAUDE.md` to remove the parts that describe the old dual-pipeline + side-table architecture. Keep the proximity-vs-trip_update source filter advice.

- [ ] **Step 3: Update NOTES.md**

In NOTES.md, find the NOTES-72 entry (around line 124). Remove the long "Phase D/E/F" exposition and replace with a one-liner like:

```
- **NOTES-72 Trip-update state refactor — complete.** Migration finished
  2026-06-01. Specs:
  `docs/superpowers/specs/2026-05-17-trip-update-state-refactor-design.md`,
  `docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md`,
  `docs/superpowers/specs/2026-05-23-stop-events-v2-cutover-design.md`.
```

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md NOTES.md
git commit -m "docs: update CLAUDE.md + NOTES.md after NOTES-72 Phase F completion"
```

### Task 17: Stop the collector, deploy, restart

This step exists OUTSIDE the PR — it's a real-world deployment action that must happen on the user's machine, not by an agent.

- [ ] **Step 1: Stop the current collector**

The collector is a long-running daemon. Identify its PID:

```bash
cat /Users/wlovotti/repos/wmata-dashboard/logs/collector.pid
```

Stop it with SIGINT:
```bash
kill -INT $(cat /Users/wlovotti/repos/wmata-dashboard/logs/collector.pid)
```

- [ ] **Step 2: Wait for clean shutdown**

Confirm the collector exits cleanly (look for the "graceful shutdown" line in `logs/collector.log`).

- [ ] **Step 3: Restart with the new code**

After the PR is merged and main is pulled:

```bash
git checkout main
git pull
nohup uv run python scripts/continuous_combined_collector.py > /dev/null 2>&1 &
echo $! > logs/collector.pid
```

- [ ] **Step 4: Verify collector is healthy**

Wait 2 minutes, then:
```bash
uv run python scripts/collector_status.py
```

Expected: collector running, recent activity, no errors.

### Task 18: DROP `trip_update_snapshots`

- [ ] **Step 1: Confirm new collector is not writing to snapshots**

```bash
psql -d wmata_dashboard -c "
SELECT collected_at, COUNT(*)
FROM trip_update_snapshots
WHERE collected_at > NOW() - INTERVAL '5 minutes'
GROUP BY collected_at
ORDER BY collected_at DESC LIMIT 5;
"
```

Expected: empty result (no new rows in the last 5 minutes, because the new collector doesn't write to this table anymore).

- [ ] **Step 2: Final row-count snapshot (historical record)**

```bash
psql -d wmata_dashboard -c "
SELECT
  COUNT(*) AS total_rows,
  pg_size_pretty(pg_total_relation_size('trip_update_snapshots')) AS total_size
FROM trip_update_snapshots;
" | tee /tmp/trip_update_snapshots_final.txt
```

- [ ] **Step 3: DROP TABLE**

```bash
psql -d wmata_dashboard -c "DROP TABLE trip_update_snapshots;"
```

Expected: `DROP TABLE`. The freed space won't be returned to the OS by `DROP TABLE` alone (it does free for reuse though). If you want to return space to the OS, run `VACUUM FULL` on the database after — but the table being dropped means there's no `VACUUM FULL` target. The freed space is reclaimed by Postgres automatically.

- [ ] **Step 4: Confirm**

```bash
psql -d wmata_dashboard -c "\d trip_update_snapshots"
```

Expected: relation does not exist.

### Task 19: Open + merge the Phase F PR

- [ ] **Step 1: Push**

```bash
git push -u origin feature/notes-72-phase-f
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "feat: NOTES-72 Phase F — retire snapshot pipeline + table" --body "$(cat <<'EOF'
## Summary
- Collector no longer writes to `trip_update_snapshots`
- Legacy pipeline files deleted (`derive_stop_events_trip_updates.py`, `compare_old_vs_new_derivation.py`, `archive_trip_update_snapshots.py`)
- `scripts/migrate_create_stop_events_v2.py` deleted
- v2-specific test files deleted
- `trip_update_snapshots` table DROPped manually
- `CLAUDE.md` + `NOTES.md` updated to reflect single-derivation architecture

## Why
7 nightly batches under v2 since cutover (Mon 2026-05-25) all clean per the morning checks. Per the 2026-05-23 cutover design spec, this PR executes Phase F (collapsed 7-day variant, was 14 in original spec).

## Test plan
- [x] `uv run pytest -m smoke` passes
- [x] `bin/test-with-pg` passes
- [x] Ruff clean
- [x] Collector restarted on new code; `scripts/collector_status.py` healthy
- [x] `trip_update_snapshots` table dropped; no live writers remain
- [ ] Next nightly batch runs cleanly under launchd

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Wait for CI**

Watch: `gh pr checks --watch`
Expected: all pass.

- [ ] **Step 4: Merge**

```bash
gh pr merge --squash --delete-branch
git checkout main
git pull
```

Migration complete.

---

## Acceptance criteria

The migration is considered complete when all of the following are true:

- [ ] `stop_events.source='trip_update'` rows are produced by `derive_stop_events_from_state.py` writing to the canonical `stop_events` table.
- [ ] `stop_events_v2` table dropped.
- [ ] `trip_update_snapshots` table dropped.
- [ ] Collector no longer dual-writes to `trip_update_snapshots`.
- [ ] Legacy `derive_stop_events_trip_updates.py` and `compare_old_vs_new_derivation.py` removed from the repo.
- [ ] `pipelines/run_daily_batch.py` runs cleanly with the v2-only lineup, including the row-count guard against `stop_events` trip_update rows.
- [ ] All existing tests pass; `cd frontend && npm test` passes.
- [ ] `system_metrics_daily` for the 7 days post-cutover shows no metric anomalies (per Section 3 of the spec).
- [ ] `CLAUDE.md` and `NOTES.md` updated.
