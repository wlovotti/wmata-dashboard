# NOTES-115. Scheduled wait time / service-level KPI on the comparison page

**Severity: medium** *(the comparison page currently hides the largest
term of rider wait — every shipped KPI is measured relative to schedule,
so the page can't show that one agency simply runs more service)*.
**Effort: medium** *(research + aggregation + one new tile; the SWT
machinery exists in `src/ewt.py` but a system-level SWT rollup may not —
check `system_metrics_daily` before scoping a pipeline change vs
on-the-fly compute)*.

Motivating observation (2026-08-11): the shipped comparison page
(PR #198) shows WMATA and SFMTA as roughly comparable, but all four
KPIs (OTP, EWT, service delivered, bunching) are
performance-against-promise metrics that normalize away the promise
itself. Measured from current GTFS for Thursday 2026-08-06, daytime
(7:00–19:00): WMATA median scheduled headway per route-direction is
**24 min** with 27% of route-directions at ≤15 min; SFMTA is **12 min**
with 60% at ≤15 min. Total rider wait = SWT + EWT, and the page shows
only the small term (EWT: 183 s vs 148 s) while hiding the big one
(median-headway SWT: ~12 min vs ~6 min). The user's qualitative rider
experience ("Muni just runs more service") is real and currently
invisible.

Preferred shape: surface **SWT alongside EWT** on the comparison page
so the wait tiles read as "scheduled wait X + excess wait Y", sourced
from the same EWT machinery (`src/ewt.py` computes both halves:
EWT = AWT − SWT) for internal consistency — rather than a raw
trips-per-day count, which doesn't compare across network sizes.

**Research first, per standing practice:** scan how agencies and the
TRB literature report service levels before locking the formula —
TCRP's wait-assessment framing (SWT/AWT), NTD's vehicle revenue hours,
and pick the aggregation weighting deliberately (route-weighted vs
trip-weighted vs rider-weighted medians differ materially here; the
2026-08-06 numbers above are route-direction-weighted). Also decide
whether the KPI covers all routes or only the frequent-gate subset that
EWT uses today, and say so in the caveat footnote.

Acceptance: the comparison page shows a scheduled-service KPI per
agency (SWT or equivalent), computed identically for both, with a
caveat line stating the weighting choice.

## Dependencies

None blocking. `route_service_profile` would be a natural input for a
headway-based framing, but its SFMTA rows are weekend-only until the
weekday classification gap (NOTES-116) is fixed — the EWT-machinery
path does not depend on it.
