// Shared header-copy assertion for visual-regression specs (closes the
// visual-regression copy-sensitivity gap, TODO: PR#).
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
// run immediately before the pixel snapshot in each visual-regression test.
// This doesn't touch the pixel tolerance or the committed baselines at all
// (so it doesn't force a baseline regen), but a copy change anywhere in the
// header — however small in pixel-area terms — fails this assertion
// deterministically. It runs as a normal Playwright assertion, so it's part
// of the same CI job as the pixel gate and needs no separate wiring.
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
 * Intended to run right before `expect(page).toHaveScreenshot(...)` in each
 * visual-regression test, so a header copy regression fails CI even when
 * the pixel diff it produces is too small to trip `maxDiffPixelRatio`.
 *
 * @param {import('@playwright/test').Page} page - the Playwright page,
 *   already navigated to a page that renders the shared `App.jsx` header.
 * @returns {Promise<void>}
 */
export async function assertHeaderCopy(page) {
  await expect(page.getByRole('heading', { name: EXPECTED_HEADER_TITLE, level: 1 })).toBeVisible()
  await expect(page.locator('header .subtitle')).toHaveText(EXPECTED_HEADER_SUBTITLE)
}
