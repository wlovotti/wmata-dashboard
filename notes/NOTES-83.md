# NOTES-83. Blank RouteDetail visual-regression baselines

**Severity: medium (the CI visual gate for RouteDetail asserts nothing —
visual regressions on that page ship silently).**
**Effort: low** *(medium if the root cause is a real fixture-path crash
rather than a stale capture)*.

Both checked-in baselines
(`frontend/tests/e2e/routedetail.spec.js-snapshots/routedetail-d72-chromium-darwin.png`
and `-linux.png`) are entirely blank white 1280×720 images. The spec
(`routedetail.spec.js`) waits for the "30-Day Trend" text to be visible
and then takes a `fullPage: true` screenshot, yet the baseline is an
empty viewport-sized frame — so at capture time the page was blank and
≤720px tall. Most likely something (RouteMap/leaflet under fixtures, or
a crash after the visibility check) blanks the page, and a
`--update-snapshots` run enshrined it; CI stays green because the page
consistently re-blanks the same way. Overview, RouteList, and Segments
baselines are all healthy, so the harness itself works.

Work: (1) run the spec headed/traced locally to see what the page
actually looks like at capture time; (2) fix the crash or add the
missing fixture; (3) regenerate BOTH baseline sets per the
`frontend/README.md` procedure (macOS local + Linux via Docker).

**Subagent note:** diagnosis and the code fix are subagent-suitable;
the baseline regeneration (macOS + Docker Playwright runs) is user-run —
the subagent should document the regen commands in the PR body instead
of running them.
