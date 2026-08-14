// Deterministic tile stub for map screenshots (NOTES-84 system map).
//
// OSM raster tiles are a live network dependency with nondeterministic
// content — fatal for visual baselines. Every tile request is fulfilled
// with the same solid light-grey 1x1 PNG (browsers scale it to the 256x256
// slot) so polylines render over a flat, stable background.
//
// PNG bytes: a 1x1 opaque #eef0f2 PNG, verified by decoding its IDAT chunk
// (colortype 2 / RGB, raw pixel bytes ee f0 f2 — no alpha, no transparency).
const TILE_PNG_BASE64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR42mN49+ETAAWgAtEM3pLTAAAAAElFTkSuQmCC'

/**
 * Register a route intercept that answers all OSM tile requests with the
 * stub PNG. Call in beforeEach BEFORE page.goto.
 *
 * @param {import('@playwright/test').Page} page - the Playwright page to
 *   register the intercept on.
 * @returns {Promise<void>}
 */
export async function stubMapTiles(page) {
  await page.route('**/*.tile.openstreetmap.org/**', (route) =>
    route.fulfill({
      contentType: 'image/png',
      body: Buffer.from(TILE_PNG_BASE64, 'base64'),
    }),
  )
}
