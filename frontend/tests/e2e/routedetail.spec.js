// Visual regression for RouteDetail page (/route/D72).
//
// The RouteDetail path in App.jsx is /route/:routeId (not /routes/:routeId).
// Fixtures cover /api/routes/D72 (the detail payload), the three trend
// endpoints (/api/routes/D72/trend?metric=...), period-drilldown, and
// bunching-causes (both consumed by the PeriodDrilldown sub-component).
// All other /api/** calls return empty-safe defaults so the page renders
// without crashing.
//
// bunching-causes needs its own fixture (rather than falling through to
// the generic EMPTY_OBJECT catch-all below): the real endpoint always
// returns `n_bunched_pairs` + `breakdown` together (see
// src/bunching.py:compute_bunching_cause_breakdown), but `{}` has
// neither, which used to crash BunchingCauseBar (NOTES-83) and blank the
// whole page before the fullPage screenshot.
//
// period-drilldown also needs its own fixture: `{}` is a *valid* shape
// there (PeriodDrilldown.jsx defaults `data.ewt`/`data.bunching` to `[]`),
// so it doesn't crash — but it silently renders both "Performance by Time
// of Day" charts as their empty states, which would enshrine an
// under-populated baseline. See
// api/aggregations.py:get_route_period_drilldown for the real shape.

import { test, expect } from '@playwright/test'
import { readFileSync } from 'fs'
import { fileURLToPath } from 'url'
import { join, dirname } from 'path'
import { fulfillGtfsFreshness } from './helpers/gtfsFreshnessStub'
import { assertHeaderCopy } from './helpers/headerCopy'

const __dirname = dirname(fileURLToPath(import.meta.url))
const fixturesDir = join(__dirname, '../fixtures')

function fixture(name) {
  return JSON.parse(readFileSync(join(fixturesDir, name), 'utf8'))
}

// Minimal empty payloads for endpoints RouteDetail's sub-components might call
// (RecentRuns, BlockList, StopDiagnostic, RouteDiagnosisPanel).
const EMPTY_LIST = []
const EMPTY_OBJECT = {}

test.beforeEach(async ({ page }) => {
  await page.route('**/api/**', async (route) => {
    const url = route.request().url()

    // Main route detail payload.
    if (url.match(/\/api\/routes\/D72(\?|$)/)) {
      return route.fulfill({ json: fixture('route_d72_detail.json') })
    }

    // Trend payloads.
    if (url.includes('/api/routes/D72/trend') && url.includes('metric=otp')) {
      return route.fulfill({ json: fixture('route_d72_trend_otp.json') })
    }
    if (url.includes('/api/routes/D72/trend') && url.includes('metric=service_delivered')) {
      return route.fulfill({ json: fixture('route_d72_trend_service_delivered.json') })
    }
    if (url.includes('/api/routes/D72/trend') && url.includes('metric=excess_trip_time')) {
      return route.fulfill({ json: fixture('route_d72_trend_excess_trip_time.json') })
    }

    // Per-time-period EWT + bunching charts (PeriodDrilldown).
    if (url.includes('/api/routes/D72/period-drilldown')) {
      return route.fulfill({ json: fixture('route_d72_period_drilldown.json') })
    }

    // Bunching-cause decomposition (PeriodDrilldown's BunchingCauseBar).
    if (url.includes('/api/routes/D72/bunching-causes')) {
      return route.fulfill({ json: fixture('route_d72_bunching_causes.json') })
    }

    // Sub-component endpoints — return safe empty defaults so their
    // loading/error states don't interfere with the screenshot.
    if (url.includes('/api/routes/D72/runs')) {
      return route.fulfill({ json: { runs: [] } })
    }
    if (url.includes('/api/routes/D72/blocks')) {
      return route.fulfill({ json: { blocks: [] } })
    }
    if (url.includes('/api/routes/D72/stops')) {
      return route.fulfill({ json: { stops: [] } })
    }
    // Narrative endpoint: 404 simulates "not yet generated" — the component
    // renders a clean "no narrative" message rather than an empty/broken state.
    if (url.includes('/api/routes/D72/diagnosis')) {
      return route.fulfill({ status: 404, json: { detail: 'No narrative cached' } })
    }
    if (url.includes('/api/routes/D72')) {
      // Catch-all for any other D72 sub-routes.
      return route.fulfill({ json: EMPTY_OBJECT })
    }
    if (url.includes('/api/gtfs/freshness')) {
      return fulfillGtfsFreshness(route)
    }

    await route.continue()
  })
})

test('RouteDetail: smoke — route name D72 visible', async ({ page }) => {
  await page.goto('/route/D72')
  // The route name should appear in the header area.
  await expect(page.getByText('D72').first()).toBeVisible()
})

test('RouteDetail: 30-Day Trend section renders', async ({ page }) => {
  await page.goto('/route/D72')
  await expect(page.getByText('30-Day Trend')).toBeVisible()
})

test('RouteDetail: visual regression', async ({ page }) => {
  await page.goto('/route/D72')
  // Wait for the trend section and sparklines to render.
  await expect(page.getByText('30-Day Trend')).toBeVisible()
  await page.waitForTimeout(500)
  // DOM-level header copy check alongside the pixel snapshot (PR #214) —
  // see helpers/headerCopy.js for why.
  await assertHeaderCopy(page)
  await expect(page).toHaveScreenshot('routedetail-d72.png', { fullPage: true })
})
