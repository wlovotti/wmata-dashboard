// Shared header-copy assertion for visual-regression specs (closes the
// visual-regression copy-sensitivity gap, PR #214).
//
// The pixel-diff gate (`playwright.config.js`'s
// `expect.toHaveScreenshot.maxDiffPixelRatio: 0.01`) tolerates up to 1% of
// a full-page screenshot differing, to absorb sub-pixel anti-aliasing noise
// across runs/platforms. That tolerance is generous enough that a one-line
// header subtitle swap comfortably stays under it against a full-page
// screenshot — PR #204 changed the header subtitle on all four baselined
// pages and the visual-regression job stayed green, because the diff was
// real but pixel-small. Tightening the ratio system-wide risks reintroducing
// the anti-aliasing flakiness the 1% tolerance was chosen to absorb, and a
// masked/cropped comparison just moves the same brittleness onto a second,
// harder-to-maintain screenshot fixture.
//
// Instead: assert the exact header text content as its own DOM-level check,
// run as a separate `<Page>: header copy is exact` test in each spec. Since
// 2026-11-24 the pixel snapshots are advisory in CI, so this lives outside
// the visual-regression tests and stays in the blocking functional step.
// This doesn't touch the pixel tolerance or the committed baselines at all
// (so it doesn't force a baseline regen), but a copy change anywhere in the
// header — however small in pixel-area terms — fails this assertion
// deterministically.
//
// Centralized here (rather than duplicating the literal strings in all four
// specs) so a legitimate, intentional header copy change only needs
// updating in one place — mirrors the `gtfsFreshnessStub.js` /
// `tileStub.js` shared-helper pattern already used by these specs.

import { expect } from '@playwright/test'

export const EXPECTED_HEADER_TITLE = 'WMATA Performance Dashboard'
export const EXPECTED_HEADER_SUBTITLE = 'Daily bus network performance metrics'

/**
 * Assert the app header renders the current, exact title + subtitle copy.
 *
 * Called from each spec's blocking `header copy is exact` test, so a header
 * copy regression fails CI even though the pixel snapshots are advisory and
 * would not catch a diff too small to trip `maxDiffPixelRatio` anyway.
 *
 * @param {import('@playwright/test').Page} page - the Playwright page,
 *   already navigated to a page that renders the shared `App.jsx` header.
 * @returns {Promise<void>}
 */
export async function assertHeaderCopy(page) {
  // getByRole's `name` option is a case-insensitive substring match by
  // default, so a title like "WMATA Performance Dashboard (Beta)" would
  // still pass — mirror the subtitle's exact toHaveText check instead.
  await expect(page.locator('header h1')).toHaveText(EXPECTED_HEADER_TITLE)
  await expect(page.locator('header .subtitle')).toHaveText(EXPECTED_HEADER_SUBTITLE)
}
