# NOTES-112. Proximity fallback matcher emits +20–24h false matches

**Severity: low** *(≈3.5 rows/day on SFMTA, and most land on routes the
comparison excludes anyway — see below. But they pollute every outlier
scan and would survive any future |deviation|-based data-quality
audit.)*
**Effort: low-medium** *(likely a plausibility clamp in the proximity
matching path of `pipelines/derive_stop_events.py`; the investigation
of why the candidate window admits day-distant stop_times is the
unknown.)*

The NOTES-105/110 repave (2026-08-10→11) eliminated the −24h
misattribution cluster (287,904 → 0 rows), but left a small, distinct
residue: **65 rows across 7/21–8/7, all `source='proximity'`, all with
*positive* `deviation_sec` between +71,817 and +86,364** (vehicle
observed ~20–24 h *after* the scheduled instant). These predate the
repave — the same rows appeared in the pre-delete baseline — so they
are not service-date misattribution (that signature was −86,400 and
trip_update-dominated). They are position/time-based fallback matches
pairing a vehicle (probably laid over or parked near a terminal) with
a stop_time from the wrong side of the service day.

Affected routes: K, N, T (Muni Metro LRV), PM, PH, CA (cable car),
48, 5R, PM — note most are rail/cable modes that the comparison's
bus-only symmetric definition already excludes, so the bus-comparison
blast radius is roughly routes 48/5R at ~1 row/day. WMATA should be
checked for the same signature before assuming it's SFMTA-specific.

Plausible fix: reject proximity matches whose implied |deviation|
exceeds a sanity bound (e.g. 3 h), or restrict the candidate stop_time
pool to a window around the observation instant. Guard belongs in the
proximity path only — `trip_update` shows zero rows with this
signature.
