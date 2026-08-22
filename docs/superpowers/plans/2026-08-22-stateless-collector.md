# Stateless Collector Rewrite (NOTES-95) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the stateful VM (Postgres + timers) with a dumb poller that archives both agencies' GTFS-RT feeds as zstd JSONL and uploads to S3 every 15 minutes; rework the laptop pull to `aws s3 sync` + a new VP loader.

**Architecture:** One new entry point `scripts/stateless_collector.py --agency {wmata,sfmta}` reuses `WMATADataCollector`'s fetch/parse methods but never touches a database. `JsonlArchiveWriter` gains 15-minute rotation; a new `S3Uploader` ships closed files and a `PingGate` pings healthchecks.io only when both feeds shipped recently. Laptop side: new `pipelines/load_vp_archive.py` (manifest-table idempotency + NOTES-81 phantom-timestamp guard) and a reworked `bin/pull-and-derive.sh`.

**Tech Stack:** Python 3 (requests, zstandard, boto3 — all existing deps), SQLAlchemy, systemd templated units, healthchecks.io.

**Spec:** `docs/superpowers/specs/2026-08-22-stateless-collector-design.md` (read it first; this plan implements it section by section).

## Global Constraints

- Datetime storage is **naive UTC** everywhere (`src/timezones.py`: `utcnow_naive`, `from_epoch_naive_utc`). Never `datetime.now()` / `datetime.fromtimestamp()`.
- Docstrings on every function, class, and method (user's global CLAUDE.md).
- Before every push: `uv run ruff check src/ scripts/ api/ pipelines/ tests/` AND `uv run ruff format --check src/ scripts/ api/ pipelines/ tests/` AND `uv run pytest -m smoke` (full `uv run pytest` before each PR). Both ruff gates must include `tests/`.
- Never commit to `main`. Three PRs, sequential (wait for each merge before opening the next — squash-merge makes stacked PRs conflict):
  - **PR A** (Tasks 1–5, branch `feature/notes-95-stateless-collector`): VM-side collector.
  - **PR B** (Tasks 6–8, branch `feature/notes-95-vp-loader`): laptop-side loader.
  - **PR C** (Task 9, branch `feature/notes-95-cutover`): pull-and-derive rework + docs + NOTES closure. Opens only after the user-run cutover ops (Tasks O1–O3) verify the overlap.
- Any PR touching `deployment/systemd/` must cite `docs/DEPLOYMENT.md` §2 (cp to `/etc/systemd/system/` + `daemon-reload` + restart) in its deploy section — `git pull` does not update installed units.
- New `main()` functions take `argv=None` and pass it to `parse_args(argv)` — never read `sys.argv` implicitly (the NOTES-130 lesson).
- Heavy ops (provisioning, unit installs, parity psql runs, decommission) are **user-run**; the plan supplies exact commands but subagents never SSH to the VM or run backfills.
- S3 bucket: `s3://wmata-dashboard-backups`, permanent prefix `raw-jsonl-archive/`. Sub-prefixes: root = WMATA TU (existing), `sfmta/` = SFMTA TU (existing), `vp/` = WMATA VP (new), `sfmta_vp/` = SFMTA VP (new).

---

### Task 1: 15-minute rotation in `JsonlArchiveWriter`

**Files:**
- Modify: `src/archive_writer.py`
- Test: `tests/test_archive_writer.py`

**Interfaces:**
- Consumes: existing `JsonlArchiveWriter(archive_dir)` / `.append(row, snapshot_ts)` / `.close()`.
- Produces (later tasks rely on these exact signatures):
  - `JsonlArchiveWriter(archive_dir, rotate_interval_sec: int | None = None)` — `None` keeps today's midnight-only rotation.
  - `.close() -> Path | None` — returns the path of the file it just closed (None if none open).
  - `.open_path -> Path | None` property — the currently-open file (uploader must skip it).
  - Filenames become `YYYY-MM-DD.<pid>.<open_unix_ts>.jsonl.zst` (open-time, not startup-time — still 3 dot-tokens, still matched by replay's `{date}.*.jsonl.zst` glob).

- [ ] **Step 1: Write the failing tests** (append to `tests/test_archive_writer.py`, matching its existing fixture style):

```python
def _read_rows(path):
    """Decode every JSON line from a .jsonl.zst file (tolerates no footer)."""
    import io, json
    import zstandard as zstd

    with open(path, "rb") as fh:
        reader = zstd.ZstdDecompressor().stream_reader(fh)
        text = io.TextIOWrapper(reader, encoding="utf-8")
        return [json.loads(line) for line in text]


def test_interval_rotation_opens_new_file(tmp_path):
    """Crossing rotate_interval_sec closes the open file and starts a new one."""
    from datetime import datetime

    from src.archive_writer import JsonlArchiveWriter

    w = JsonlArchiveWriter(tmp_path, rotate_interval_sec=900)
    ts = datetime(2026, 8, 22, 12, 0, 0)
    w.append({"a": 1}, snapshot_ts=ts)
    # Simulate 15 minutes of wall clock passing.
    w._open_wall_ts -= 901
    w.append({"a": 2}, snapshot_ts=ts)
    w.close()

    files = sorted(tmp_path.glob("2026-08-22.*.jsonl.zst"))
    assert len(files) == 2
    assert [_read_rows(f)[0]["a"] for f in files] == [1, 2]


def test_no_interval_keeps_single_file(tmp_path):
    """Default rotate_interval_sec=None preserves the current one-file behavior."""
    from datetime import datetime

    from src.archive_writer import JsonlArchiveWriter

    w = JsonlArchiveWriter(tmp_path)
    ts = datetime(2026, 8, 22, 12, 0, 0)
    w.append({"a": 1}, snapshot_ts=ts)
    w._open_wall_ts -= 10_000
    w.append({"a": 2}, snapshot_ts=ts)
    w.close()
    assert len(list(tmp_path.glob("*.jsonl.zst"))) == 1


def test_close_returns_closed_path(tmp_path):
    """close() reports which file it closed so the uploader can ship it."""
    from datetime import datetime

    from src.archive_writer import JsonlArchiveWriter

    w = JsonlArchiveWriter(tmp_path, rotate_interval_sec=900)
    assert w.close() is None  # nothing open yet
    w.append({"a": 1}, snapshot_ts=datetime(2026, 8, 22, 12, 0, 0))
    open_path = w.open_path
    assert open_path is not None and open_path.exists()
    assert w.close() == open_path
    assert w.open_path is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_archive_writer.py -v -k "interval or single_file or closed_path"`
Expected: FAIL (`TypeError: unexpected keyword argument 'rotate_interval_sec'`, `AttributeError: open_path`).

- [ ] **Step 3: Implement** in `src/archive_writer.py`:
  - `__init__` gains `rotate_interval_sec: int | None = None`; store as `self._rotate_interval_sec`; add `self._open_wall_ts: float | None = None` and `self._open_path: Path | None = None`.
  - `_filename_for(target_date)` uses `int(time.time())` as the third token (per-open, replacing the per-process `self._startup_ts`); loop `while path.exists(): ts += 1` to guarantee uniqueness (only reachable at a date-boundary + interval race). Update the class/module docstrings: per-open filenames keep the single-frame-per-file property the per-process scheme introduced.
  - `append()` rotates when `self._open_date != target_date` **or** (`self._rotate_interval_sec is not None and self._open_wall_ts is not None and time.time() - self._open_wall_ts >= self._rotate_interval_sec`).
  - `_rotate_to()` records `self._open_wall_ts = time.time()` and `self._open_path = path`.
  - `close()` returns the previously-open `Path` (or `None`), and resets `_open_path`/`_open_wall_ts` to `None`.
  - Add `@property def open_path(self) -> Path | None`.

- [ ] **Step 4: Run the whole file's tests** (existing filename-format tests may assert the startup-ts token — update any that break to the new per-open semantics, keeping their intent)

Run: `uv run pytest tests/test_archive_writer.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git checkout -b feature/notes-95-stateless-collector
git add src/archive_writer.py tests/test_archive_writer.py
git commit -m "feature: interval rotation + closed-path reporting in JsonlArchiveWriter (NOTES-95)"
```

---

### Task 2: Agency config gains VP-archive and S3 fields

**Files:**
- Modify: `src/agency_config.py`, `config/agencies/wmata.yaml`, `config/agencies/sfmta.yaml`
- Test: `tests/test_agency_config.py`

**Interfaces:**
- Produces: `AgencyConfig` gains four fields consumed by Tasks 4 and 7:
  - `vp_archive_dir: str` — wmata: `archive/vp_snapshots`, sfmta: `archive/sfmta_vp_snapshots`
  - `s3_bucket: str` — both: `wmata-dashboard-backups`
  - `s3_tu_prefix: str` — wmata: `raw-jsonl-archive/`, sfmta: `raw-jsonl-archive/sfmta/`
  - `s3_vp_prefix: str` — wmata: `raw-jsonl-archive/vp/`, sfmta: `raw-jsonl-archive/sfmta_vp/`

- [ ] **Step 1: Write the failing test** (append to `tests/test_agency_config.py`):

```python
def test_vp_and_s3_fields_load_for_both_agencies():
    """The stateless collector's VP-archive dirs and S3 prefixes come from yaml."""
    from src.agency_config import load_agency_config

    wmata = load_agency_config("wmata")
    assert wmata.vp_archive_dir == "archive/vp_snapshots"
    assert wmata.s3_bucket == "wmata-dashboard-backups"
    assert wmata.s3_tu_prefix == "raw-jsonl-archive/"
    assert wmata.s3_vp_prefix == "raw-jsonl-archive/vp/"

    sfmta = load_agency_config("sfmta")
    assert sfmta.vp_archive_dir == "archive/sfmta_vp_snapshots"
    assert sfmta.s3_tu_prefix == "raw-jsonl-archive/sfmta/"
    assert sfmta.s3_vp_prefix == "raw-jsonl-archive/sfmta_vp/"
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_agency_config.py -v` → FAIL (`TypeError` / `KeyError`).

- [ ] **Step 3: Implement** — add the four dataclass fields; in `load_agency_config` read `raw["collector"]["vp_archive_dir"]` and `raw["s3"]["bucket"] / ["tu_prefix"] / ["vp_prefix"]`. Add to both yamls, e.g. `wmata.yaml`:

```yaml
collector:
  # ... existing keys unchanged ...
  vp_archive_dir: archive/vp_snapshots
s3:
  bucket: wmata-dashboard-backups
  tu_prefix: raw-jsonl-archive/
  vp_prefix: raw-jsonl-archive/vp/
```

and `sfmta.yaml`:

```yaml
collector:
  # ... existing keys unchanged ...
  vp_archive_dir: archive/sfmta_vp_snapshots
s3:
  bucket: wmata-dashboard-backups
  tu_prefix: raw-jsonl-archive/sfmta/
  vp_prefix: raw-jsonl-archive/sfmta_vp/
```

Also update `wmata.yaml`'s header comment (it says the config is "NOT yet consumed" pending NOTES-95 — after Task 4 it is consumed by `scripts/stateless_collector.py`; the legacy `continuous_combined_collector.py` still doesn't read it).

- [ ] **Step 4: Run** `uv run pytest tests/test_agency_config.py tests/test_sfmta_collector_tick.py -v` → PASS (existing constructors use keyword lookup, so new fields are additive).

- [ ] **Step 5: Commit**

```bash
git add src/agency_config.py config/agencies/ tests/test_agency_config.py
git commit -m "feature: agency-config VP-archive dirs + S3 prefixes (NOTES-95)"
```

---

### Task 3: `S3Uploader` — ship closed files, verify, buffer 48 h

**Files:**
- Create: `src/s3_uploader.py`
- Test: `tests/test_s3_uploader.py`

**Interfaces:**
- Produces (Task 4 consumes exactly these):
  - `S3Uploader(bucket: str, s3_client=None)` — `s3_client=None` lazily builds `boto3.client("s3")`; tests inject a fake.
  - `.upload_closed_files(archive_dir: Path, key_prefix: str, skip: set[Path]) -> list[str]` — uploads every `*.jsonl.zst` directly under `archive_dir` (not in `skip`, not in `uploaded/`), verifies `head_object` ContentLength == local size, moves each verified file to `archive_dir/uploaded/`, returns uploaded filenames. Raises `UploadVerificationError` on a size mismatch (file stays in place → retried next cycle).
  - `.prune_uploaded(archive_dir: Path, max_age_sec: int = 172800) -> int` — deletes files in `archive_dir/uploaded/` older than `max_age_sec` by mtime; returns count.
  - `class UploadVerificationError(RuntimeError)`.

Crash-safety notes to encode in docstrings: a leftover file from a dead process is uploaded on the next scan because it is closed (not in `skip`); a crash between upload and rename re-uploads the same key with identical content — idempotent overwrite.

- [ ] **Step 1: Write the failing tests** (`tests/test_s3_uploader.py`):

```python
"""S3Uploader unit tests — a fake boto3 client, no network."""

from pathlib import Path

import pytest

from src.s3_uploader import S3Uploader, UploadVerificationError


class FakeS3:
    """Records upload_file calls; head_object reports the stored size."""

    def __init__(self, corrupt: bool = False):
        self.uploads: list[tuple[str, str, str]] = []  # (local, bucket, key)
        self._sizes: dict[str, int] = {}
        self._corrupt = corrupt

    def upload_file(self, filename, bucket, key):
        size = Path(filename).stat().st_size
        self._sizes[key] = size - 1 if self._corrupt else size
        self.uploads.append((filename, bucket, key))

    def head_object(self, Bucket, Key):
        return {"ContentLength": self._sizes[Key]}


def _mk(dirpath: Path, name: str, content: bytes = b"x" * 64) -> Path:
    """Create a fake closed archive file."""
    p = dirpath / name
    p.write_bytes(content)
    return p


def test_uploads_closed_files_and_moves_to_uploaded(tmp_path):
    fake = FakeS3()
    up = S3Uploader("bkt", s3_client=fake)
    closed = _mk(tmp_path, "2026-08-22.1.100.jsonl.zst")
    open_file = _mk(tmp_path, "2026-08-22.1.200.jsonl.zst")

    shipped = up.upload_closed_files(tmp_path, "raw-jsonl-archive/vp/", skip={open_file})

    assert shipped == ["2026-08-22.1.100.jsonl.zst"]
    assert fake.uploads[0][2] == "raw-jsonl-archive/vp/2026-08-22.1.100.jsonl.zst"
    assert not closed.exists()
    assert (tmp_path / "uploaded" / closed.name).exists()
    assert open_file.exists()  # skipped


def test_verification_failure_leaves_file_in_place(tmp_path):
    up = S3Uploader("bkt", s3_client=FakeS3(corrupt=True))
    f = _mk(tmp_path, "2026-08-22.1.100.jsonl.zst")
    with pytest.raises(UploadVerificationError):
        up.upload_closed_files(tmp_path, "p/", skip=set())
    assert f.exists()  # still pending — retried next cycle


def test_prune_uploaded_deletes_only_old_files(tmp_path):
    import os
    import time

    up = S3Uploader("bkt", s3_client=FakeS3())
    updir = tmp_path / "uploaded"
    updir.mkdir()
    old = _mk(updir, "2026-08-20.1.100.jsonl.zst")
    fresh = _mk(updir, "2026-08-22.1.100.jsonl.zst")
    two_days_ago = time.time() - 172_801
    os.utime(old, (two_days_ago, two_days_ago))

    assert up.prune_uploaded(tmp_path) == 1
    assert not old.exists() and fresh.exists()
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_s3_uploader.py -v` → FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `src/s3_uploader.py`:**

```python
"""Upload closed JSONL archive files to S3 with verify-then-buffer semantics.

Lifecycle per file (spec 2026-08-22 §1): the collector's writer closes a
file every 15 minutes; this uploader ships every closed file it finds,
verifies the stored byte count, then moves the file into an ``uploaded/``
subdirectory that is pruned after 48 hours. Idempotency is filesystem
state: pending files sit next to the writer's open file; a crash between
upload and the rename re-uploads identical bytes to the same key (a
harmless overwrite); a leftover file from a crashed process is shipped on
the next scan because it is closed and therefore not in ``skip``.
"""

import time
from pathlib import Path

import boto3


class UploadVerificationError(RuntimeError):
    """Raised when S3 reports a different byte count than the local file."""


class S3Uploader:
    """Ship closed ``*.jsonl.zst`` files from an archive dir to one S3 prefix."""

    def __init__(self, bucket: str, s3_client=None):
        """``s3_client=None`` builds a real boto3 client; tests inject a fake."""
        self.bucket = bucket
        self._s3 = s3_client if s3_client is not None else boto3.client("s3")

    def upload_closed_files(
        self, archive_dir: Path, key_prefix: str, skip: set[Path]
    ) -> list[str]:
        """Upload every closed archive file directly under ``archive_dir``.

        ``skip`` holds the writer's currently-open path(s). Each verified
        file moves to ``archive_dir/uploaded/``; a verification mismatch
        raises and leaves the file pending so the next cycle retries it.
        Returns the uploaded filenames (sorted, oldest first).
        """
        uploaded_dir = archive_dir / "uploaded"
        shipped: list[str] = []
        for path in sorted(archive_dir.glob("*.jsonl.zst")):
            if path in skip:
                continue
            size = path.stat().st_size
            if size == 0:
                continue
            key = f"{key_prefix}{path.name}"
            self._s3.upload_file(str(path), self.bucket, key)
            head = self._s3.head_object(Bucket=self.bucket, Key=key)
            if head["ContentLength"] != size:
                raise UploadVerificationError(
                    f"{key}: S3 has {head['ContentLength']} bytes, local file has {size}"
                )
            uploaded_dir.mkdir(exist_ok=True)
            path.rename(uploaded_dir / path.name)
            shipped.append(path.name)
        return shipped

    def prune_uploaded(self, archive_dir: Path, max_age_sec: int = 48 * 3600) -> int:
        """Delete verified-uploaded files older than ``max_age_sec`` (mtime)."""
        uploaded_dir = archive_dir / "uploaded"
        if not uploaded_dir.exists():
            return 0
        cutoff = time.time() - max_age_sec
        pruned = 0
        for path in uploaded_dir.glob("*.jsonl.zst"):
            if path.stat().st_mtime < cutoff:
                path.unlink()
                pruned += 1
        return pruned
```

- [ ] **Step 4: Run** `uv run pytest tests/test_s3_uploader.py -v` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/s3_uploader.py tests/test_s3_uploader.py
git commit -m "feature: S3Uploader with verify-then-buffer lifecycle (NOTES-95)"
```

---

### Task 4: Poller loop, ping gate, and the `stateless_collector.py` entry point

**Files:**
- Create: `src/stateless_poller.py`, `scripts/stateless_collector.py`
- Test: `tests/test_stateless_poller.py`

**Interfaces:**
- Consumes: Task 1's writer (`rotate_interval_sec`, `.open_path`, `.close() -> Path|None`), Task 2's config fields, Task 3's `S3Uploader`; existing `WMATADataCollector.get_realtime_trip_updates() -> (snapshot_ts, rows)` and `.get_realtime_vehicle_positions() -> list[dict]`; `src/deadman.py:ping_healthcheck(url)`; `src/pidfile.py:acquire_pid_file/release_pid_file`; `src/timezones.py:utcnow_naive`.
- Produces:
  - `PingGate(url, freshness_sec=1200, min_gap_sec=300)` with `.record_ship(feed: str, now: float)` and `.maybe_ping(now: float) -> bool` (feeds are the strings `"tu"` and `"vp"`).
  - `archive_vp_rows(writer, vehicles: list[dict], collected_at: datetime) -> int` — appends one line per vehicle with a `collected_at` ISO field added; returns row count.
  - `run_upload_cycle(uploader, streams, gate, now) -> list[str]` where `streams` is `list[tuple[str, Path, str, JsonlArchiveWriter]]` = `(feed, archive_dir, key_prefix, writer)`.

- [ ] **Step 1: Write the failing tests** (`tests/test_stateless_poller.py`):

```python
"""Unit tests for the stateless poller's gate, VP serialization, and upload cycle."""

from datetime import datetime
from pathlib import Path

from src.archive_writer import JsonlArchiveWriter
from src.stateless_poller import PingGate, archive_vp_rows, run_upload_cycle


class RecordingUploader:
    """Stands in for S3Uploader; reports the pending files as shipped."""

    def __init__(self):
        self.calls = []

    def upload_closed_files(self, archive_dir, key_prefix, skip):
        shipped = [p.name for p in sorted(Path(archive_dir).glob("*.jsonl.zst")) if p not in skip]
        self.calls.append((archive_dir, key_prefix))
        return shipped

    def prune_uploaded(self, archive_dir, max_age_sec=48 * 3600):
        return 0


def test_ping_gate_requires_both_feeds_fresh(monkeypatch):
    pings = []
    monkeypatch.setattr("src.stateless_poller.ping_healthcheck", lambda url: pings.append(url))
    gate = PingGate("http://hc/x", freshness_sec=1200, min_gap_sec=300)

    gate.record_ship("tu", now=1000.0)
    assert gate.maybe_ping(now=1001.0) is False  # vp never shipped
    gate.record_ship("vp", now=1002.0)
    assert gate.maybe_ping(now=1003.0) is True
    assert pings == ["http://hc/x"]
    assert gate.maybe_ping(now=1100.0) is False  # inside min_gap_sec
    assert gate.maybe_ping(now=1400.0) is True   # gap passed, both still fresh
    assert gate.maybe_ping(now=9999.0) is False  # both feeds stale now


def test_archive_vp_rows_round_trip(tmp_path):
    """VP dicts serialize with collected_at and read back losslessly."""
    import io, json
    import zstandard as zstd

    w = JsonlArchiveWriter(tmp_path)
    vehicles = [{"vehicle_id": "42", "route_id": "D72", "trip_id": "t1",
                 "direction_id": 0, "trip_start_date": "20260822",
                 "latitude": 38.9, "longitude": -77.0, "speed": 5.5,
                 "current_stop_sequence": 3, "stop_id": "1001",
                 "current_status": 2, "timestamp": 1787740800}]
    n = archive_vp_rows(w, vehicles, collected_at=datetime(2026, 8, 22, 12, 0, 5))
    closed = w.close()
    assert n == 1

    with open(closed, "rb") as fh:
        text = io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(fh), encoding="utf-8")
        rows = [json.loads(line) for line in text]
    assert rows[0]["vehicle_id"] == "42"
    assert rows[0]["timestamp"] == 1787740800
    assert rows[0]["collected_at"] == "2026-08-22T12:00:05"


def test_run_upload_cycle_records_ships_per_feed(tmp_path, monkeypatch):
    monkeypatch.setattr("src.stateless_poller.ping_healthcheck", lambda url: True)
    tu_dir, vp_dir = tmp_path / "tu", tmp_path / "vp"
    tu_dir.mkdir(); vp_dir.mkdir()
    (tu_dir / "2026-08-22.1.100.jsonl.zst").write_bytes(b"x")
    tu_w, vp_w = JsonlArchiveWriter(tu_dir), JsonlArchiveWriter(vp_dir)
    gate = PingGate("http://hc/x")
    streams = [("tu", tu_dir, "raw-jsonl-archive/", tu_w),
               ("vp", vp_dir, "raw-jsonl-archive/vp/", vp_w)]

    shipped = run_upload_cycle(RecordingUploader(), streams, gate, now=1000.0)

    assert shipped == ["2026-08-22.1.100.jsonl.zst"]
    assert gate._last_ship["tu"] == 1000.0
    assert "vp" not in gate._last_ship  # nothing shipped for vp
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_stateless_poller.py -v` → FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `src/stateless_poller.py`:**

```python
"""Pure logic for the stateless collector: ping gating, VP archiving, upload cycles.

Kept import-safe (no boto3 client construction, no network at import) so
`scripts/stateless_collector.py` stays a thin loop and everything here is
unit-testable. Spec: docs/superpowers/specs/2026-08-22-stateless-collector-design.md §1–2.
"""

from datetime import datetime
from pathlib import Path

from src.archive_writer import JsonlArchiveWriter
from src.deadman import ping_healthcheck


class PingGate:
    """Dead-man gate: ping only while BOTH feeds are shipping fresh data.

    ``record_ship(feed, now)`` marks a feed as having shipped an S3 object.
    ``maybe_ping(now)`` pings when every recorded feed shipped within
    ``freshness_sec`` AND both "tu" and "vp" have shipped at least once,
    rate-limited to one ping per ``min_gap_sec``. A wedged feed therefore
    silences the check within ~freshness_sec — this single signal subsumes
    the VP-path dead-man coverage item (NOTES-94; see spec §2).
    """

    def __init__(self, url: str | None, freshness_sec: int = 1200, min_gap_sec: int = 300):
        self._url = url
        self._freshness_sec = freshness_sec
        self._min_gap_sec = min_gap_sec
        self._last_ship: dict[str, float] = {}
        self._last_ping: float | None = None

    def record_ship(self, feed: str, now: float) -> None:
        """Mark ``feed`` ("tu" | "vp") as having shipped an object at ``now``."""
        self._last_ship[feed] = now

    def maybe_ping(self, now: float) -> bool:
        """Ping the healthcheck if both feeds are fresh; return whether we pinged."""
        both_fresh = all(
            now - self._last_ship.get(feed, float("-inf")) <= self._freshness_sec
            for feed in ("tu", "vp")
        )
        gap_ok = self._last_ping is None or now - self._last_ping >= self._min_gap_sec
        if both_fresh and gap_ok:
            ping_healthcheck(self._url)
            self._last_ping = now
            return True
        return False


def archive_tu_rows(writer: JsonlArchiveWriter, rows: list[dict]) -> int:
    """Append TU rows in the exact format `_save_trip_updates` archives them.

    One line per (trip, stop) row, keyed by the row's own ``snapshot_ts`` —
    byte-format compatibility with pipelines/replay_archive_to_state.py is
    an acceptance test (tests/test_stateless_replay_compat.py).
    """
    for row in rows:
        writer.append(row, snapshot_ts=row["snapshot_ts"])
    return len(rows)


def archive_vp_rows(
    writer: JsonlArchiveWriter, vehicles: list[dict], collected_at: datetime
) -> int:
    """Append one line per vehicle, adding the poll's ``collected_at`` (naive UTC).

    The raw GTFS-RT epoch ``timestamp`` is preserved untouched — the
    NOTES-81 phantom-timestamp guard is applied by the laptop-side loader,
    never at collection (raw stays raw; spec §1).
    """
    for vehicle in vehicles:
        row = dict(vehicle)
        row["collected_at"] = collected_at.isoformat()
        writer.append(row, snapshot_ts=collected_at)
    return len(vehicles)


def run_upload_cycle(uploader, streams, gate: PingGate, now: float) -> list[str]:
    """Ship every closed file across all feed streams, then maybe ping.

    ``streams`` is a list of ``(feed, archive_dir, key_prefix, writer)``.
    The writer's currently-open file is skipped; everything else directly
    under the dir is closed by construction. Prunes the 48-hour uploaded/
    buffer as it goes.
    """
    shipped_all: list[str] = []
    for feed, archive_dir, key_prefix, writer in streams:
        skip = {writer.open_path} if writer.open_path is not None else set()
        shipped = uploader.upload_closed_files(Path(archive_dir), key_prefix, skip)
        if shipped:
            gate.record_ship(feed, now)
            shipped_all.extend(shipped)
        uploader.prune_uploaded(Path(archive_dir))
    gate.maybe_ping(now)
    return shipped_all
```

- [ ] **Step 4: Run** `uv run pytest tests/test_stateless_poller.py -v` → PASS.

- [ ] **Step 5: Write `scripts/stateless_collector.py`** (thin loop; no test beyond import — the pieces are covered above):

```python
"""Stateless GTFS-RT collector — poll → zstd JSONL → S3 every 15 min → hc ping.

The Path 2a endpoint (NOTES-95): no database anywhere in this process.
Reuses WMATADataCollector purely as a fetch/parse client (its internal
archive writer and DB methods are never used); archives TU and VP rows to
per-feed JSONL streams rotating every 15 minutes; ships closed files to S3
and pings healthchecks.io only while both feeds are shipping (spec §1–2).

Run with:
    uv run python scripts/stateless_collector.py --agency wmata
    uv run python scripts/stateless_collector.py --agency sfmta
"""

import argparse
import os
import signal
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from src.agency_config import load_agency_config, request_kwargs
from src.archive_writer import JsonlArchiveWriter
from src.pidfile import acquire_pid_file, release_pid_file
from src.s3_uploader import S3Uploader
from src.stateless_poller import PingGate, archive_tu_rows, archive_vp_rows, run_upload_cycle
from src.timezones import utcnow_naive
from src.wmata_collector import WMATADataCollector

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
ROTATE_INTERVAL_SEC = 900  # 15-minute upload cadence (spec decision 1)


def now_str() -> str:
    """Local-time stamp prefix used in console logs."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_fetcher(cfg) -> WMATADataCollector:
    """Construct the collector as a pure fetch client for ``cfg``'s feeds.

    The instance's internal archive writer targets the TU dir but is never
    appended to (we archive via our own rotating writers), and no DB
    session is ever attached.
    """
    api_key = os.getenv(cfg.api_key_env)
    if not api_key:
        raise ValueError(f"{cfg.api_key_env} not found in environment variables")
    auth = request_kwargs(cfg, api_key)
    return WMATADataCollector(
        api_key,
        archive_root=REPO_ROOT / cfg.archive_dir,
        tu_feed_url=cfg.trip_updates_url,
        vp_feed_url=cfg.vehicle_positions_url,
        request_params=auth.get("params"),
        service_date_tz=cfg.timezone,
    )


def main(argv=None) -> None:
    """Run the stateless polling loop for one agency until interrupted."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", required=True, choices=("wmata", "sfmta"))
    args = parser.parse_args(argv)

    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    cfg = load_agency_config(args.agency)
    acquire_pid_file(REPO_ROOT / cfg.pid_file)

    tu_dir = REPO_ROOT / cfg.archive_dir
    vp_dir = REPO_ROOT / cfg.vp_archive_dir
    tu_writer = JsonlArchiveWriter(tu_dir, rotate_interval_sec=ROTATE_INTERVAL_SEC)
    vp_writer = JsonlArchiveWriter(vp_dir, rotate_interval_sec=ROTATE_INTERVAL_SEC)
    uploader = S3Uploader(cfg.s3_bucket)
    gate = PingGate(os.getenv(cfg.healthcheck_url_env))
    streams = [
        ("tu", tu_dir, cfg.s3_tu_prefix, tu_writer),
        ("vp", vp_dir, cfg.s3_vp_prefix, vp_writer),
    ]
    fetcher = build_fetcher(cfg)

    print(f"{cfg.display_name} Stateless Collector (no DB)")
    print(f"TU every {cfg.tick_sec * cfg.trip_updates_every_ticks}s -> s3://{cfg.s3_bucket}/{cfg.s3_tu_prefix}")
    print(f"VP every {cfg.tick_sec * cfg.vehicle_positions_every_ticks}s -> s3://{cfg.s3_bucket}/{cfg.s3_vp_prefix}")

    tick_idx = 0
    try:
        while True:
            start = time.monotonic()

            if tick_idx % cfg.trip_updates_every_ticks == 0:
                try:
                    _, rows = fetcher.get_realtime_trip_updates()
                    n = archive_tu_rows(tu_writer, rows)
                    print(f"[{now_str()}] tick={tick_idx} trip_updates rows={n}")
                except Exception as e:
                    print(f"[{now_str()}] tick={tick_idx} trip_updates ERROR: {e}")

            if tick_idx % cfg.vehicle_positions_every_ticks == 0:
                try:
                    vehicles = fetcher.get_realtime_vehicle_positions()
                    n = archive_vp_rows(vp_writer, vehicles, collected_at=utcnow_naive())
                    print(f"[{now_str()}] tick={tick_idx} vehicle_positions rows={n}")
                except Exception as e:
                    print(f"[{now_str()}] tick={tick_idx} vehicle_positions ERROR: {e}")

            try:
                shipped = run_upload_cycle(uploader, streams, gate, now=time.time())
                if shipped:
                    print(f"[{now_str()}] tick={tick_idx} uploaded: {', '.join(shipped)}")
            except Exception as e:
                print(f"[{now_str()}] tick={tick_idx} upload ERROR: {e}")

            elapsed = time.monotonic() - start
            sleep_for = cfg.tick_sec - elapsed
            if sleep_for < 0:
                print(f"[{now_str()}] tick={tick_idx} WARNING: tick took {elapsed:.1f}s")
            else:
                time.sleep(sleep_for)
            tick_idx += 1

    except KeyboardInterrupt:
        print("\nStopping stateless collection...")
    finally:
        # Close both writers, then ship the final partial files (no ping —
        # a shutdown must not look like health).
        tu_writer.close()
        vp_writer.close()
        try:
            for _feed, archive_dir, key_prefix, _w in streams:
                uploader.upload_closed_files(Path(archive_dir), key_prefix, set())
        except Exception as e:
            print(f"final upload ERROR: {e}")
        fetcher.close()
        release_pid_file(REPO_ROOT / cfg.pid_file)


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Add the replay-compatibility golden test** (`tests/test_stateless_replay_compat.py`) — proves spec §5's byte-format requirement without network, by comparing against what `_save_trip_updates` archives:

```python
"""Spec §5: TU files written by the stateless path replay identically.

Builds one poll's rows, archives them via archive_tu_rows, and asserts the
decoded lines match what WMATADataCollector's own archive writer produces
for the same rows (same keys, same JSON encoding of timestamps).
"""

import io
import json
from datetime import datetime

import zstandard as zstd

from src.archive_writer import JsonlArchiveWriter
from src.stateless_poller import archive_tu_rows

SNAPSHOT_TS = datetime(2026, 8, 22, 12, 0, 0)
ROWS = [
    {
        "snapshot_ts": SNAPSHOT_TS,
        "trip_id": "t1",
        "route_id": "D72",
        "vehicle_id": "42",
        "stop_id": "1001",
        "stop_sequence": 3,
        "predicted_arrival_ts": datetime(2026, 8, 22, 12, 5, 0),
        "predicted_departure_ts": None,
        "schedule_relationship": "SCHEDULED",
        "trip_start_date": "20260822",
    }
]


def _decode(path):
    with open(path, "rb") as fh:
        text = io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(fh), encoding="utf-8")
        return [json.loads(line) for line in text]


def test_stateless_tu_lines_match_legacy_archive_format(tmp_path):
    legacy_dir, new_dir = tmp_path / "legacy", tmp_path / "new"
    legacy = JsonlArchiveWriter(legacy_dir)
    for row in ROWS:  # exactly what _save_trip_updates does before its DB work
        legacy.append(row, snapshot_ts=row["snapshot_ts"])
    legacy.close()

    new = JsonlArchiveWriter(new_dir)
    archive_tu_rows(new, ROWS)
    new.close()

    legacy_lines = _decode(next(legacy_dir.glob("*.jsonl.zst")))
    new_lines = _decode(next(new_dir.glob("*.jsonl.zst")))
    assert new_lines == legacy_lines
```

- [ ] **Step 7: Run everything** — `uv run pytest tests/test_stateless_poller.py tests/test_stateless_replay_compat.py -v` → PASS; then `uv run pytest -m smoke` → PASS.

- [ ] **Step 8: Commit**

```bash
git add src/stateless_poller.py scripts/stateless_collector.py tests/test_stateless_poller.py tests/test_stateless_replay_compat.py
git commit -m "feature: stateless collector entry point + ping gate (NOTES-95)"
```

---

### Task 5: Templated systemd unit + deployment docs; open PR A

**Files:**
- Create: `deployment/systemd/collector@.service`
- Modify: `docs/DEPLOYMENT.md` (new "Stateless collector (nano)" section — do NOT remove the interim banner yet; that happens in PR C after cutover)

**Interfaces:** none downstream; the unit invokes Task 4's script with `--agency %i`.

- [ ] **Step 1: Write `deployment/systemd/collector@.service`:**

```ini
[Unit]
Description=Stateless GTFS-RT collector (%i)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=wmata
Group=wmata
WorkingDirectory=/home/wmata/wmata-dashboard
Environment="PATH=/home/wmata/.local/bin:/usr/local/bin:/usr/bin:/bin"
# Per-agency secrets: API key, healthcheck URL, AWS credentials for the
# S3 upload (Lightsail has no instance roles — keys live here).
EnvironmentFile=/home/wmata/wmata-dashboard/.env.%i

ExecStart=/home/wmata/wmata-dashboard/.venv/bin/python3 scripts/stateless_collector.py --agency %i

Restart=always
RestartSec=5s

StandardOutput=append:/var/log/wmata/collector-%i.log
StandardError=append:/var/log/wmata/collector-%i-error.log
SyslogIdentifier=collector-%i

# nano_3_0 is a 512 MB box running two instances; the loop's baseline is
# tens of MB (no DB session, no SQLAlchemy engine).
MemoryMax=180M

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=/var/log/wmata /home/wmata/wmata-dashboard/logs /home/wmata/wmata-dashboard/archive

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Document in `docs/DEPLOYMENT.md`** — add a "Stateless collector (nano) — NOTES-95" section covering: provisioning a `nano_3_0`, repo deploy, `.env.wmata` / `.env.sfmta` contents (`WMATA_API_KEY` or `SFMTA_API_KEY`+`SFMTA_COLLECTOR_HEALTHCHECK_URL`, `COLLECTOR_HEALTHCHECK_URL`, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` scoped to `raw-jsonl-archive/*` puts — extend `deployment/aws/s3-backup-policy.json`'s pattern), unit install per §2 (`sudo cp deployment/systemd/collector@.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable --now collector@wmata collector@sfmta`), and the two new healthchecks.io checks (period 5 min, grace 45 min — the gate pings every ~5 min when healthy; full-outage alert ≈50 min, single-feed wedge ≈80 min worst case).

- [ ] **Step 3: Full verification**

```bash
uv run pytest
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
```
Expected: all clean. Fix anything that isn't before proceeding.

- [ ] **Step 4: Commit and open PR A**

```bash
git add deployment/systemd/collector@.service docs/DEPLOYMENT.md
git commit -m "feature: templated collector@ unit + nano deployment docs (NOTES-95)"
git push -u origin feature/notes-95-stateless-collector
gh pr create --title "feature: stateless collector — VM side (NOTES-95, 1/3)" --body "..."
```
PR body must: explain this is 1 of 3 for NOTES-95 (spec link), state that NOTES.md edits land in PR C with the cutover, and cite `docs/DEPLOYMENT.md` §2 for the unit install (units are user-installed; `git pull` alone no-ops).

---

### Task 6: `vp_archive_loaded_files` manifest model + migration

**Files:**
- Modify: `src/models.py`
- Create: `scripts/migrate_create_vp_archive_loaded_files.py`
- Test: `tests/test_load_vp_archive.py` (started here, extended in Task 7)

**Interfaces:**
- Produces (Task 7 consumes):

```python
class VpArchiveLoadedFile(Base):
    """One row per raw VP archive file already loaded into vehicle_positions.

    The loader's idempotency key: each immutable S3-synced file loads
    exactly once (spec §3); re-runs and overlapping syncs skip rows here.
    """

    __tablename__ = "vp_archive_loaded_files"

    filename = Column(String, primary_key=True)  # basename, e.g. 2026-08-22.612.1787740800.jsonl.zst
    row_count = Column(Integer, nullable=False)
    dropped_count = Column(Integer, nullable=False, default=0)
    loaded_at = Column(DateTime, nullable=False, default=utcnow_naive)
```

- [ ] **Step 1: Failing test** (new `tests/test_load_vp_archive.py`; use the repo's standard in-memory-SQLite session fixture from `tests/conftest.py` — read how sibling tests like `tests/test_load_vp_from_parquet.py` get a session and copy that pattern):

```python
def test_manifest_model_round_trip(session):
    """VpArchiveLoadedFile persists filename/counts/loaded_at."""
    from src.models import VpArchiveLoadedFile

    session.add(VpArchiveLoadedFile(filename="2026-08-22.1.100.jsonl.zst", row_count=10, dropped_count=1))
    session.commit()
    row = session.query(VpArchiveLoadedFile).one()
    assert row.row_count == 10 and row.loaded_at is not None
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_load_vp_archive.py -v` → FAIL (ImportError).

- [ ] **Step 3: Add the model** to `src/models.py` (exact class above, placed near `VehiclePosition`) → test PASSES.

- [ ] **Step 4: Write the migration script** `scripts/migrate_create_vp_archive_loaded_files.py`, mirroring `scripts/migrate_create_collector_heartbeats.py` but agency-aware (the table must exist in BOTH the WMATA and SFMTA databases):

```python
"""Create the ``vp_archive_loaded_files`` manifest table (NOTES-95 VP loader).

Idempotent (CREATE TABLE IF NOT EXISTS). Run once per database:

    uv run python scripts/migrate_create_vp_archive_loaded_files.py
    uv run python scripts/migrate_create_vp_archive_loaded_files.py --agency sfmta
"""

import argparse
import sys

from dotenv import load_dotenv
from sqlalchemy import text

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_engine

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vp_archive_loaded_files (
    filename       VARCHAR    PRIMARY KEY,
    row_count      INTEGER    NOT NULL,
    dropped_count  INTEGER    NOT NULL DEFAULT 0,
    loaded_at      TIMESTAMP  NOT NULL
);
"""


def run_migration(engine) -> None:
    """Apply the migration. Safe to re-run."""
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))


def main(argv=None) -> int:
    """CLI entry point; ``argv`` is explicit so migrate_all.py can pass []."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", default="wmata", choices=("wmata", "sfmta"))
    args = parser.parse_args(argv)
    load_dotenv()
    cfg = load_agency_config(args.agency)
    engine = get_engine(resolve_agency_db_url(cfg))
    print(f"Creating vp_archive_loaded_files in the {args.agency} database...")
    run_migration(engine)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 5: Run** `uv run pytest tests/test_load_vp_archive.py -m smoke -v; uv run pytest tests/test_load_vp_archive.py -v` → PASS. (Do NOT run the migration against the real databases — document it in the PR body for the user to run, per MIGRATIONS.md.)

- [ ] **Step 6: Commit**

```bash
git checkout main && git pull --ff-only   # only after PR A merges
git checkout -b feature/notes-95-vp-loader
git add src/models.py scripts/migrate_create_vp_archive_loaded_files.py tests/test_load_vp_archive.py
git commit -m "feature: vp_archive_loaded_files manifest table (NOTES-95)"
```

---

### Task 7: `pipelines/load_vp_archive.py` — VP JSONL → `vehicle_positions`

**Files:**
- Create: `pipelines/load_vp_archive.py`
- Test: `tests/test_load_vp_archive.py` (extend)

**Interfaces:**
- Consumes: Task 6's `VpArchiveLoadedFile`; VP line format from Task 4's `archive_vp_rows` (vehicle dict fields + `collected_at` ISO string, `timestamp` as raw epoch int or null); `src/timezones.py:from_epoch_naive_utc`; `src/agency_config.py:load_agency_config, resolve_agency_db_url`; `src/database.py:get_session`.
- Produces:
  - `GUARD_SEC = 1800` — module constant; a row is dropped when `abs(vehicle timestamp − collected_at) > GUARD_SEC`. (This is the spec's "±15 min outside the 15-min rotation window" implemented with row-level precision: 15 min window + 15 min tolerance = 30 min from the row's own poll time. Catches the +20–24 h NOTES-81 phantoms; tolerates AVL lag.)
  - `parse_vp_line(obj: dict) -> dict | None` — returns kwargs for a `VehiclePosition` insert, or `None` when the guard drops the row. Missing/null `timestamp` falls back to `collected_at` (matching the legacy `_save_vehicle_positions` behavior).
  - `load_vp_file(session, path: Path) -> tuple[int, int]` — `(inserted, dropped)`; inserts rows + manifest row in ONE transaction (crash → rollback → file stays unloaded); skips (returns `(0, 0)` and logs) if the basename is already in the manifest.
  - `main(argv=None)` — args: `--agency {wmata,sfmta}` (default wmata), `--archive-root PATH` (default `REPO_ROOT / cfg.vp_archive_dir`). Iterates `sorted(archive_root.glob("*.jsonl.zst"))`, prints a per-file and final summary including total dropped.

- [ ] **Step 1: Write the failing tests** (extend `tests/test_load_vp_archive.py`):

```python
def _write_vp_file(dirpath, vehicles, collected_at):
    """Produce a VP archive file exactly as the stateless collector would."""
    from src.archive_writer import JsonlArchiveWriter
    from src.stateless_poller import archive_vp_rows

    w = JsonlArchiveWriter(dirpath)
    archive_vp_rows(w, vehicles, collected_at=collected_at)
    return w.close()


VEHICLE = {"vehicle_id": "42", "route_id": "D72", "trip_id": "t1", "direction_id": 0,
           "trip_start_date": "20260822", "latitude": 38.9, "longitude": -77.0,
           "speed": 5.5, "current_stop_sequence": 3, "stop_id": "1001",
           "current_status": 2, "timestamp": 1787740805}
COLLECTED = datetime(2026, 8, 22, 12, 0, 10)  # ~5 s after the fix timestamp


def test_load_vp_file_inserts_rows_and_manifest(session, tmp_path):
    from src.models import VehiclePosition, VpArchiveLoadedFile
    from pipelines.load_vp_archive import load_vp_file

    path = _write_vp_file(tmp_path, [VEHICLE], COLLECTED)
    inserted, dropped = load_vp_file(session, path)

    assert (inserted, dropped) == (1, 0)
    vp = session.query(VehiclePosition).one()
    assert vp.vehicle_id == "42" and vp.route_id == "D72"
    from src.timezones import from_epoch_naive_utc

    assert vp.timestamp == from_epoch_naive_utc(1787740805)
    assert session.query(VpArchiveLoadedFile).one().filename == path.name


def test_double_load_is_idempotent(session, tmp_path):
    from src.models import VehiclePosition
    from pipelines.load_vp_archive import load_vp_file

    path = _write_vp_file(tmp_path, [VEHICLE], COLLECTED)
    load_vp_file(session, path)
    assert load_vp_file(session, path) == (0, 0)
    assert session.query(VehiclePosition).count() == 1


def test_phantom_timestamp_dropped_and_counted(session, tmp_path):
    """NOTES-81: a +24h vehicle-reported timestamp never reaches the table."""
    from src.models import VehiclePosition, VpArchiveLoadedFile
    from pipelines.load_vp_archive import load_vp_file

    phantom = dict(VEHICLE, vehicle_id="99", timestamp=1787740805 + 86_400)
    path = _write_vp_file(tmp_path, [VEHICLE, phantom], COLLECTED)
    inserted, dropped = load_vp_file(session, path)

    assert (inserted, dropped) == (1, 1)
    assert session.query(VehiclePosition).one().vehicle_id == "42"
    assert session.query(VpArchiveLoadedFile).one().dropped_count == 1


def test_missing_timestamp_falls_back_to_collected_at(session, tmp_path):
    from src.models import VehiclePosition
    from pipelines.load_vp_archive import load_vp_file

    no_ts = dict(VEHICLE, timestamp=None)
    path = _write_vp_file(tmp_path, [no_ts], COLLECTED)
    assert load_vp_file(session, path) == (1, 0)
    assert session.query(VehiclePosition).one().timestamp == COLLECTED
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_load_vp_archive.py -v` → FAIL (ImportError on `pipelines.load_vp_archive`).

- [ ] **Step 3: Implement `pipelines/load_vp_archive.py`:**

```python
"""Load raw VP JSONL archives into vehicle_positions (NOTES-95, spec §3).

Replaces the retired \\copy-over-tunnel VP delta: the stateless collector
archives VehiclePositions as jsonl.zst in S3; this loader parses each
synced file once (manifest-table idempotency) and applies the NOTES-81
phantom-timestamp guard at load time — raw files stay raw.

    uv run python pipelines/load_vp_archive.py --agency wmata
    uv run python pipelines/load_vp_archive.py --agency sfmta
"""

import argparse
import io
import json
import sys
from datetime import datetime
from pathlib import Path

import zstandard as zstd
from dotenv import load_dotenv
from sqlalchemy import insert

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_session
from src.models import VehiclePosition, VpArchiveLoadedFile
from src.timezones import from_epoch_naive_utc, utcnow_naive

REPO_ROOT = Path(__file__).resolve().parent.parent

# Drop rows whose vehicle-reported timestamp is >30 min from the row's own
# poll time: the spec's "±15 min outside the 15-min rotation window" with
# row-level precision. Catches NOTES-81's +20-24h phantoms; tolerates AVL lag.
GUARD_SEC = 1800
INSERT_CHUNK = 5000


def parse_vp_line(obj: dict) -> dict | None:
    """Map one archived VP JSON line to VehiclePosition insert kwargs.

    Returns None when the NOTES-81 guard drops the row. A missing/null
    ``timestamp`` falls back to ``collected_at`` (legacy collector
    behavior for un-timestamped vehicles).
    """
    collected_at = datetime.fromisoformat(obj["collected_at"])
    epoch = obj.get("timestamp")
    if epoch:
        ts = from_epoch_naive_utc(epoch)
        if abs((ts - collected_at).total_seconds()) > GUARD_SEC:
            return None
    else:
        ts = collected_at
    return {
        "vehicle_id": obj.get("vehicle_id"),
        "route_id": obj.get("route_id"),
        "trip_id": obj.get("trip_id"),
        "direction_id": obj.get("direction_id"),
        "trip_start_date": obj.get("trip_start_date"),
        "latitude": obj.get("latitude"),
        "longitude": obj.get("longitude"),
        "speed": obj.get("speed"),
        "current_stop_sequence": obj.get("current_stop_sequence"),
        "stop_id": obj.get("stop_id"),
        "current_status": obj.get("current_status"),
        "timestamp": ts,
        "collected_at": collected_at,
    }


def load_vp_file(session, path: Path) -> tuple[int, int]:
    """Load one archive file exactly once; returns (inserted, dropped).

    Rows + the manifest row commit in a single transaction, so a crash
    mid-file rolls back cleanly and the file re-loads next run. The zstd
    stream_reader tolerates a missing frame footer (crash-cut files).
    """
    already = session.get(VpArchiveLoadedFile, path.name)
    if already is not None:
        print(f"  {path.name}: already loaded ({already.row_count} rows), skipping")
        return 0, 0

    inserted = dropped = 0
    batch: list[dict] = []
    with open(path, "rb") as fh:
        text_stream = io.TextIOWrapper(
            zstd.ZstdDecompressor().stream_reader(fh), encoding="utf-8"
        )
        for line in text_stream:
            row = parse_vp_line(json.loads(line))
            if row is None:
                dropped += 1
                continue
            batch.append(row)
            if len(batch) >= INSERT_CHUNK:
                session.execute(insert(VehiclePosition), batch)
                inserted += len(batch)
                batch = []
    if batch:
        session.execute(insert(VehiclePosition), batch)
        inserted += len(batch)

    session.add(
        VpArchiveLoadedFile(
            filename=path.name, row_count=inserted, dropped_count=dropped,
            loaded_at=utcnow_naive(),
        )
    )
    session.commit()
    print(f"  {path.name}: inserted {inserted}, dropped {dropped}")
    return inserted, dropped


def main(argv=None) -> int:
    """Load every not-yet-loaded VP archive file for one agency."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", default="wmata", choices=("wmata", "sfmta"))
    parser.add_argument("--archive-root", type=Path, default=None)
    args = parser.parse_args(argv)
    load_dotenv()

    cfg = load_agency_config(args.agency)
    archive_root = args.archive_root or REPO_ROOT / cfg.vp_archive_dir
    session = get_session(db_url=resolve_agency_db_url(cfg))
    try:
        total_ins = total_drop = 0
        for path in sorted(archive_root.glob("*.jsonl.zst")):
            ins, drop = load_vp_file(session, path)
            total_ins += ins
            total_drop += drop
        print(f"Done: {total_ins} rows inserted, {total_drop} dropped (NOTES-81 guard).")
    finally:
        session.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run** `uv run pytest tests/test_load_vp_archive.py -v` → PASS. Then the full suite: `uv run pytest` → PASS.

- [ ] **Step 5: Commit**

```bash
git add pipelines/load_vp_archive.py tests/test_load_vp_archive.py
git commit -m "feature: VP archive loader with manifest idempotency + NOTES-81 guard"
```

---

### Task 8: GTFS-reload staleness gate; open PR B

**Files:**
- Modify: `scripts/run_gtfs_reload.py`
- Test: `tests/test_run_gtfs_reload_staleness.py` (create)

**Interfaces:**
- Produces: `reload_due(session, max_age_days: int) -> bool` in `scripts/run_gtfs_reload.py` — True when `MAX(gtfs_snapshots.snapshot_date)` is older than `max_age_days` (or no snapshot exists). New CLI flag `--max-age-days N` (default: absent = always reload, preserving current behavior). Task 9's `pull-and-derive.sh` calls `uv run python scripts/run_gtfs_reload.py --max-age-days 7`.

- [ ] **Step 1: Failing test** (`tests/test_run_gtfs_reload_staleness.py`):

```python
"""The --max-age-days gate: reload only when the newest snapshot is stale."""

from datetime import timedelta

from src.models import GTFSSnapshot
from src.timezones import utcnow_naive


def test_reload_due_when_no_snapshot(session):
    from scripts.run_gtfs_reload import reload_due

    assert reload_due(session, max_age_days=7) is True


def test_reload_not_due_for_fresh_snapshot(session):
    from scripts.run_gtfs_reload import reload_due

    session.add(GTFSSnapshot(snapshot_date=utcnow_naive() - timedelta(days=2)))
    session.commit()
    assert reload_due(session, max_age_days=7) is False


def test_reload_due_for_stale_snapshot(session):
    from scripts.run_gtfs_reload import reload_due

    session.add(GTFSSnapshot(snapshot_date=utcnow_naive() - timedelta(days=8)))
    session.commit()
    assert reload_due(session, max_age_days=7) is True
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_run_gtfs_reload_staleness.py -v` → FAIL (ImportError on `reload_due`).

- [ ] **Step 3: Implement** in `scripts/run_gtfs_reload.py`:

```python
def reload_due(session, max_age_days: int) -> bool:
    """True when the newest gtfs_snapshots row is older than ``max_age_days``.

    The pull-and-derive flow (spec 2026-08-22 §3 step 1) calls the wrapper
    with --max-age-days 7 so a reload happens at most weekly instead of on
    every pull; no snapshot at all always means due.
    """
    from datetime import timedelta

    from sqlalchemy import func

    from src.models import GTFSSnapshot
    from src.timezones import utcnow_naive

    newest = session.query(func.max(GTFSSnapshot.snapshot_date)).scalar()
    return newest is None or newest < utcnow_naive() - timedelta(days=max_age_days)
```

Wire into `main()`: add `parser.add_argument("--max-age-days", type=int, default=None)`; when set, open a session (`from src.database import get_session`), check `reload_due`, and on False print `GTFS snapshot is fresh (<= N days); skipping reload.` and exit 0 before spawning the subprocess.

- [ ] **Step 4: Run** `uv run pytest tests/test_run_gtfs_reload_staleness.py -v && uv run pytest -m smoke` → PASS. Full gates:

```bash
uv run pytest
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
```

- [ ] **Step 5: Commit and open PR B**

```bash
git add scripts/run_gtfs_reload.py tests/test_run_gtfs_reload_staleness.py
git commit -m "feature: --max-age-days staleness gate for GTFS reload (NOTES-95)"
git push -u origin feature/notes-95-vp-loader
gh pr create --title "feature: VP archive loader + manifest + reload gate (NOTES-95, 2/3)" --body "..."
```
PR body must: link the spec, state the migration commands the USER runs after merge (`migrate_create_vp_archive_loaded_files.py` for both agencies — do not run them yourself), and note NOTES.md closure lands in PR C.

---

### Task O1 (USER-RUN OPS): Provision the nano and start the parallel run

Not a coding task — the implementer prepares nothing here; the user executes after PR A merges, following the new DEPLOYMENT.md section. Summary of the commands the docs must contain (verify Task 5 wrote them):

```bash
aws lightsail create-instances --region us-east-1 --instance-names wmata-poller \
  --availability-zone us-east-1a --blueprint-id ubuntu_24_04 --bundle-id nano_3_0
# then per DEPLOYMENT.md: create wmata user, clone repo, uv sync, write
# .env.wmata/.env.sfmta, install collector@.service per §2, enable both instances,
# create the two healthchecks.io checks (period 5 min, grace 45 min)
```

Verification: new objects appearing under all four S3 prefixes within 20 minutes; both hc checks green.

### Task O2 (USER-RUN OPS): ≥7-day overlap verification

After ~7 days, the user runs (commands to be included in PR C's body):

```bash
# S3-side VP rows per day (WMATA) vs old VM DB:
aws s3 ls s3://wmata-dashboard-backups/raw-jsonl-archive/vp/ | wc -l   # files landing
# row-count parity for one full UTC day D (laptop, after a test sync+load into a scratch DB):
ssh ubuntu@52.54.130.186 "sudo -u wmata psql -d wmata_dashboard -Atc \
  \"SELECT count(*) FROM vehicle_positions WHERE collected_at >= 'D' AND collected_at < 'D+1'\""
# TU parity: replay one sample day from the new files into a scratch DB and
# compare stop_events counts (NOTES-96-style; bin/refresh-dev-db.sh gives the scratch copy)
```

Acceptance: VP daily counts within ~1% (the guard drops a handful of phantoms — count them via the loader summary); TU replay counts match exactly.

### Task O3 (USER-RUN OPS): Flip + decommission

Final VP delta pull + final `pg_dump` from the old VM, then delete the instance, the `wmata-pgdata` disk, and auto-snapshots. Only after this does PR C merge (its docs describe the end state).

---

### Task 9: pull-and-derive rework, docs, punch-list closure; open PR C

**Files:**
- Modify: `bin/pull-and-derive.sh` (rewrite), `docs/DEPLOYMENT.md` (remove interim banner; end-state topology), `CLAUDE.md` (freshness line), `NOTES.md` (remove 3 index lines)
- Delete: `notes/NOTES-95.md`, `notes/NOTES-94.md`, `notes/NOTES-81.md`, `bin/db-tunnel.sh`, `bin/prune-vm-archive.sh`, `scripts/launchd/com.wmata-dashboard.gtfs-reload.plist` (+ its README section), `deployment/systemd/wmata-collector.service`, `deployment/systemd/sfmta-collector.service`, `deployment/systemd/wmata-backup.{service,timer}`, `deployment/systemd/wmata-archive-positions.{service,timer}`, `deployment/systemd/wmata-metrics.{service,timer}`, `deployment/systemd/wmata-window-derived.{service,timer}`, `deployment/scripts/backup_db.sh`
- Test: `bin/pull-and-derive.sh` is shell plumbing — exempt from TDD; verify by running it end-to-end (step 4).

**Interfaces:** consumes Task 7's loader CLI and Task 8's `--max-age-days` flag. Nothing downstream.

- [ ] **Step 1: Rewrite `bin/pull-and-derive.sh`:**

```bash
#!/usr/bin/env bash
# bin/pull-and-derive.sh — Path 2a ingest: sync fresh raw data from S3,
# load + replay locally, derive. Run on demand (manual cadence).
#
#   bin/pull-and-derive.sh          # replay+derive lookback of 14 days
#   bin/pull-and-derive.sh 35       # wider catch-up
#
# LOOKBACK_DAYS note: widening past 2026-06-13 is unsafe — June dates need
# snapshot 12 and must go through scripts/local_recovery_2026_07.sh.
set -euo pipefail

S3_BASE="s3://wmata-dashboard-backups/raw-jsonl-archive"
LOCAL_ARCHIVE="${LOCAL_ARCHIVE:-archive/raw_snapshots}"
LOCAL_ARCHIVE_SFMTA="${LOCAL_ARCHIVE_SFMTA:-archive/sfmta_raw_snapshots}"
LOCAL_ARCHIVE_VP="${LOCAL_ARCHIVE_VP:-archive/vp_snapshots}"
LOCAL_ARCHIVE_SFMTA_VP="${LOCAL_ARCHIVE_SFMTA_VP:-archive/sfmta_vp_snapshots}"
LOOKBACK_DAYS="${1:-14}"
mkdir -p "$LOCAL_ARCHIVE" "$LOCAL_ARCHIVE_SFMTA" "$LOCAL_ARCHIVE_VP" "$LOCAL_ARCHIVE_SFMTA_VP"

echo "== GTFS reload if stale (>7 days) =="
PYTHONUNBUFFERED=1 uv run python scripts/run_gtfs_reload.py --max-age-days 7

echo "== s3 sync raw archives =="
# Root prefix holds WMATA TU directly; exclude the sibling sub-prefixes.
aws s3 sync "$S3_BASE/" "$LOCAL_ARCHIVE/" \
  --exclude "sfmta/*" --exclude "vp/*" --exclude "sfmta_vp/*"
aws s3 sync "$S3_BASE/sfmta/" "$LOCAL_ARCHIVE_SFMTA/"
aws s3 sync "$S3_BASE/vp/" "$LOCAL_ARCHIVE_VP/"
aws s3 sync "$S3_BASE/sfmta_vp/" "$LOCAL_ARCHIVE_SFMTA_VP/"

echo "== load VP archives (manifest-idempotent; NOTES-81 guard at load) =="
PYTHONUNBUFFERED=1 uv run python pipelines/load_vp_archive.py --agency wmata --archive-root "$LOCAL_ARCHIVE_VP"
PYTHONUNBUFFERED=1 uv run python pipelines/load_vp_archive.py --agency sfmta --archive-root "$LOCAL_ARCHIVE_SFMTA_VP"

echo "== replay TU archive for the lookback window (idempotent) =="
# replay_archive_to_state.py fails loudly on a zero-file match; derivation
# must never run past a replay failure (the NOTES-93 incident).
failed=()
for i in $(seq "$LOOKBACK_DAYS" -1 0); do
  d=$(date -v -"${i}"d +%F)   # macOS date; this script is laptop-only
  if ! PYTHONUNBUFFERED=1 uv run python pipelines/replay_archive_to_state.py --date "$d" --archive-root "$LOCAL_ARCHIVE"; then
    failed+=("$d")
  fi
done
if [ "${#failed[@]}" -gt 0 ]; then
  echo "Replay failed for: ${failed[*]} — aborting before derive." >&2
  exit 1
fi

echo "== derive (self-targets zero-run dates) =="
PYTHONUNBUFFERED=1 uv run python pipelines/run_daily_batch.py --lookback-days "$LOOKBACK_DAYS"
echo "Done."
```

(SFMTA replay/derive keeps its existing separate flow — this script's derive scope is unchanged from today.)

- [ ] **Step 2: Docs.** `docs/DEPLOYMENT.md`: remove the 2026-07-18 interim banner; describe the end state (nano runs `collector@{wmata,sfmta}`; laptop = system of record; S3 = permanent raw store fed by the collector; freshness = `bin/pull-and-derive.sh`, no tunnel). `CLAUDE.md`: update the freshness sentence (drop `bin/db-tunnel.sh` / `VM_HOST` requirement). Keep DEPLOYMENT.md §2's unit-install procedure section (it now covers `collector@.service`).

- [ ] **Step 3: Punch-list closure** (the `update-notes-in-pr` flow, in THIS branch):
  - `git rm notes/NOTES-95.md notes/NOTES-94.md notes/NOTES-81.md`
  - Remove their three index lines from `NOTES.md`; the "Ops & reliability (parked pending NOTES-95)" heading loses its parenthetical (the track un-parks).
  - Sweep: `grep -rn 'NOTES-95\|NOTES-94\|NOTES-81' --include='*.md' --include='*.py' --include='*.tsx' --include='*.ts' --include='*.sh' --include='*.yaml' .` — rewrite survivors into PR-anchored phrases (e.g. "the stateless-collector rewrite (PR #A/#B/#C)"). Expect hits in: `NOTES.md` blocked-by notes on other items (95 appears in NOTES-94/81 lines being deleted, but check 102/104/112/113/116/118/130 bodies), `src/agency_config.py` module docstring, `config/agencies/wmata.yaml` header, `docs/POSTMORTEM_2026-07.md` (historical — leave postmortem references intact, they describe the past), `bin/pull-and-derive.sh` (rewritten), spec/plan docs under `docs/superpowers/` (leave intact — they are dated records), and this plan's own PR-A/B code comments (`stateless_poller.py` mentions NOTES-94/81 — rewrite to descriptive phrases now that the items are gone).

- [ ] **Step 4: Verify end-to-end** — run `bin/pull-and-derive.sh` once (user may need to run it if AWS creds/DB access make it user-territory; otherwise run it directly — it is idempotent). Expected: sync pulls, loader reports, replay+derive complete. Then full gates:

```bash
uv run pytest
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
```

- [ ] **Step 5: Commit and open PR C**

```bash
git checkout main && git pull --ff-only   # only after PR B merges AND O1-O3 complete
git checkout -b feature/notes-95-cutover
# ... stage all of the above ...
git commit -m "feature: cut over to stateless collector — retire tunnel path (NOTES-95)"
git push -u origin feature/notes-95-cutover
gh pr create --title "feature: stateless-collector cutover (NOTES-95, 3/3 — closes NOTES-95/94/81)" --body "..."
```
PR body must: link the spec; record the O1–O3 verification results (get them from the user); list the decommissioned resources; cite DEPLOYMENT.md §2 for the unit changes; explain why NOTES-94 and NOTES-81 close with it (subsumed by the ping gate / folded into the loader guard).

---

## Self-Review (completed at write time)

- **Spec coverage:** §1 collector → Tasks 1–4; §2 layout+alarm → Tasks 2–5; §3 laptop → Tasks 6–8 + Task 9 step 1; §4 cutover inventory/sequence → Tasks 5 (docs), O1–O3, 9; §5 testing → each task's tests + `test_stateless_replay_compat.py` + O2. No gaps found.
- **Type consistency:** `close() -> Path|None` (Task 1) matches Task 3's `skip: set[Path]` usage via Task 4's `open_path`; `archive_vp_rows` line format matches Task 7's `parse_vp_line` fields; config field names identical across Tasks 2/4/7.
- **Known judgment calls encoded above:** per-open filename token (replay-glob-compatible); ping gate numbers (5-min hc period / 45-min grace); GUARD_SEC=1800 row-level guard implementing the spec's window rule; SFMTA derive scope unchanged.
