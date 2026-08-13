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

**2026-08-13 addendum (from the PR #205 baseline regen):** the
insensitivity also breaks the *regen* workflow, not just the gate.
Playwright's `--update-snapshots` defaults to mode `changed`, which
only rewrites snapshots that **fail** comparison — so for a copy-sized
diff inside the 1% tolerance, the documented regen commands in
`frontend/README.md` silently write nothing (this bit the first regen
pass on PR #205; `git status` came back clean after both platforms
"passed"). `--update-snapshots=all` forces the rewrite. The closing PR
should update `frontend/README.md`'s regen commands to use `=all` (or
explain when it's needed) regardless of which gate mechanism is chosen.

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
