# NOTES-120. Playwright visual-regression gate is insensitive to copy-sized diffs

**Severity: low** *(a real UI regression can land undetected if it's
small in pixel-area terms — but the underlying screenshots are still
captured on failure and the miss is copy-only so far, not layout)*.
**Effort: low** *(likely a threshold tune plus maybe a targeted
higher-sensitivity region/mask, not a redesign of the harness)*.

Surfaced 2026-08-13 during review of PR #204 (the frontend-chrome
honesty fixes): that PR changed the header subtitle text on all four
baselined pages (Overview, RouteList, RouteDetail-D72, Segments), which
should invalidate every Playwright visual-regression baseline — but CI
stayed green. `frontend/playwright.config.js`'s
`expect.toHaveScreenshot.maxDiffPixelRatio: 0.01` tolerates up to 1% of
pixels differing, which a single-line subtitle text swap comfortably
stays under against a full-page screenshot. The baselines are therefore
"stale but green": they depict text that no longer matches the app, and
the gate will keep passing until someone notices and manually
regenerates them.

Investigate before fixing: whether tightening
`maxDiffPixelRatio` system-wide reintroduces flakiness from
anti-aliasing (the reason it was set to 0.01 in the first place, per
`playwright.config.js`'s own comment), whether a lower threshold
specifically for the header region (a masked/cropped comparison) is
more targeted, or whether some other mechanism (e.g. a text-content
snapshot alongside the pixel one) better catches copy changes without
chasing pixel tolerances.

Acceptance: a documented decision (in the closing PR) on the chosen
mechanism, and a test that a header copy change (or similar
small-area text diff) is caught by CI — either by demonstrating the
new threshold/mask fails against a stale baseline, or an equivalent
regression check.
