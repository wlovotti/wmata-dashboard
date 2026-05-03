---
description: Health check for the continuous combined collector — process, sleep state, disk, log errors, row counts, cadence, freshness.
---

Run the collector status script and surface the report verbatim.

```bash
uv run python scripts/collector_status.py
```

If the script prints `✓ healthy`, just confirm that the collector is healthy in one short line — no need to repeat the whole report unless the user asks. If it prints `✗ N issue(s)`, surface the issues and propose next steps. Don't query the database directly; the script is the source of truth here.
