# NOTES-133. Stateless-collector hardening follow-ups (deferred review minors)

**Severity: low (none of these have caused an incident; each is a small
robustness gap surfaced during PR review).**
**Effort: low each, several independent items — good notes-batch
candidates.**

Minor findings deferred out of the stateless-collector rewrite (PRs
#222, #223, and the cutover PR that closed NOTES-95/94/81) during
review, none blocking:

1. **Interval clock uses `time.time()`, not monotonic.**
   `scripts/stateless_collector.py`'s tick loop measures its own
   sleep-drift with `time.monotonic()`, but `run_upload_cycle(...,
   now=time.time())` and therefore `PingGate`'s freshness/rate-limit
   windows (`src/stateless_poller.py`) are driven by wall-clock time. An
   NTP step or manual clock change mid-run could make the gate briefly
   think a feed is fresher or stalier than it really is. Low risk (the
   box syncs time normally) but an easy fix: thread a monotonic clock
   through `PingGate` instead.

2. **Zero-byte pending files are never pruned.** `S3Uploader.upload_closed_files`
   (`src/s3_uploader.py`) skips a closed file with `size == 0` (`if size
   == 0: continue`) rather than uploading it — correct, nothing to ship —
   but a skipped file is also never moved into `uploaded/`, so
   `prune_uploaded`'s 48-hour window never reaches it either. A zero-byte
   file (e.g. a rotation that raced an empty tick) sits in the archive
   dir forever.

3. **Per-stream upload error isolation is missing.** `run_upload_cycle`
   (`src/stateless_poller.py`) iterates the `(feed, archive_dir,
   key_prefix, writer)` stream list in a plain `for` loop with no
   per-stream try/except — an exception uploading the "tu" stream (e.g.
   `UploadVerificationError`) propagates out and skips "vp" for that
   cycle entirely, even though the two streams are otherwise independent.

4. **`freshness_sec=1200` vs. `ROTATE_INTERVAL_SEC=900` slack note.**
   The 20-minute `PingGate` freshness window against a 15-minute rotation
   cadence leaves ~5 minutes of slack before a feed reads as stale, which
   `docs/DEPLOYMENT.md` §13.5's grace-period math already accounts for —
   but the slack itself isn't named anywhere as a tunable. Worth a
   one-line comment near `ROTATE_INTERVAL_SEC` / `PingGate`'s default so
   a future cadence change doesn't silently erode (or balloon) that
   margin.

5. **Truncated-file rows are silently final in the load manifest.**
   `pipelines/load_vp_archive.py:load_vp_file` marks a file loaded (row
   count + all) in `vp_archive_loaded_files` as soon as it finishes
   reading — including a file whose zstd stream was crash-truncated
   mid-upload (the stream_reader tolerates a missing frame footer by
   design, per its own docstring). If a corrected/complete copy of that
   file ever became available in S3 under the same name, the manifest's
   idempotency guard would skip it as "already loaded," permanently
   capping that file's rows at whatever the truncated read produced. In
   practice files are immutable once uploaded (verify-then-buffer in
   `S3Uploader`), so this is a theoretical gap, not an observed one.

## Dependencies

None — all five are independent, self-contained fixes across
`src/stateless_poller.py`, `src/s3_uploader.py`,
`scripts/stateless_collector.py`, and `pipelines/load_vp_archive.py`.
Good candidates for a single notes-batch cycle.
