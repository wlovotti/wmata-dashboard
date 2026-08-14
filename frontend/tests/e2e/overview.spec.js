// Visual regression for the Overview page (/).
//
// All /api/** calls are intercepted and served from committed fixture JSON
// so the test never touches Postgres or the FastAPI backend. The API mocking
// strategy is page.route() — deterministic, no backend process required.
//
// Fixture coverage:
//   /api/system/trend?metric=otp&...    → system_trend_otp.json
//   /api/system/trend?metric=service_delivered&... → system_trend_service_delivered.json
//   /api/system/trend?metric=ewt&...    → system_trend_ewt.json
//   /api/system/trend?metric=bunching&... → system_trend_bunching.json
//   /api/routes                          → routes_scorecard.json
//   /api/routes/contributors?...         → routes_contributors.json
//   /api/targets                         → targets.json
//   /api/agency-comparison               → agency_comparison.json
//   /api/shapes                          → api_shapes.json
//
// OSM raster tiles are stubbed with a solid PNG (tileStub.js) so the system
// map's background is deterministic too — no live-network pixels anywhere
// in the baseline.

import { test, expect } from '@playwright/test'
import { readFileSync } from 'fs'
import { fileURLToPath } from 'url'
import { join, dirname } from 'path'
import { fulfillGtfsFreshness } from './helpers/gtfsFreshnessStub'
import { stubMapTiles } from './helpers/tileStub'

const __dirname = dirname(fileURLToPath(import.meta.url))
const fixturesDir = join(__dirname, '../fixtures')

function fixture(name) {
  return JSON.parse(readFileSync(join(fixturesDir, name), 'utf8'))
}

test.beforeEach(async ({ page }) => {
  // Stub OSM tile requests before navigation so the map background is
  // deterministic (register before the API route below, per NOTES-84).
  await stubMapTiles(page)

  // Intercept all /api/** requests before navigation.
  await page.route('**/api/**', async (route) => {
    const url = route.request().url()

    if (url.includes('/api/shapes')) {
      return route.fulfill({ json: fixture('api_shapes.json') })
    }
    if (url.includes('/api/system/trend') && url.includes('metric=otp')) {
      return route.fulfill({ json: fixture('system_trend_otp.json') })
    }
    if (url.includes('/api/system/trend') && url.includes('metric=service_delivered')) {
      return route.fulfill({ json: fixture('system_trend_service_delivered.json') })
    }
    if (url.includes('/api/system/trend') && url.includes('metric=ewt')) {
      return route.fulfill({ json: fixture('system_trend_ewt.json') })
    }
    if (url.includes('/api/system/trend') && url.includes('metric=bunching')) {
      return route.fulfill({ json: fixture('system_trend_bunching.json') })
    }
    if (url.includes('/api/agency-comparison')) {
      return route.fulfill({ json: fixture('agency_comparison.json') })
    }
    if (url.includes('/api/routes/contributors')) {
      return route.fulfill({ json: fixture('routes_contributors.json') })
    }
    if (url.includes('/api/routes') && !url.includes('/api/routes/')) {
      return route.fulfill({ json: fixture('routes_scorecard.json') })
    }
    if (url.includes('/api/targets')) {
      return route.fulfill({ json: fixture('targets.json') })
    }
    if (url.includes('/api/gtfs/freshness')) {
      return fulfillGtfsFreshness(route)
    }

    // Fall through to actual network for anything unmatched.
    await route.continue()
  })
})

test('Overview: smoke — nav link visible', async ({ page }) => {
  await page.goto('/')
  // The "Overview" nav link must be present regardless of data loading state.
  await expect(page.getByRole('link', { name: 'Overview' })).toBeVisible()
})

test('Overview: hero verdict renders', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText(/on time this week/i)).toBeVisible()
  // CompareStrip renders inside the hero from an async /api/agency-comparison
  // fetch — wait for it too so this test's timing matches the visual
  // regression test below.
  await expect(page.getByRole('link', { name: /full comparison/i })).toBeVisible()
})

test('Overview: movers panel renders', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('heading', { name: 'Getting worse' })).toBeVisible()
})

test('Overview: biggest drags renders', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('heading', { name: 'Biggest drags' })).toBeVisible()
})

test('Overview: system map renders polylines', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('heading', { name: 'System map' })).toBeVisible()
  // Leaflet renders polylines as SVG paths inside the map pane.
  await expect(page.locator('.leaflet-overlay-pane path').first()).toBeVisible()
})

test('Overview: visual regression', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText(/on time this week/i)).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Getting worse' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Biggest drags' })).toBeVisible()
  // The CompareStrip inside the hero renders from an async
  // /api/agency-comparison fetch and can land after the settle below on a
  // cold CI server, reflowing the whole page underneath it — wait for it
  // explicitly so the screenshot is deterministic.
  await expect(page.getByRole('link', { name: /full comparison/i })).toBeVisible()
  // The system map's polylines render async off the /api/shapes fetch —
  // wait for at least one so the screenshot doesn't catch a half-drawn map.
  await expect(page.locator('.leaflet-overlay-pane path').first()).toBeVisible()
  await page.waitForTimeout(500)
  await expect(page).toHaveScreenshot('overview.png', { fullPage: true })
})
