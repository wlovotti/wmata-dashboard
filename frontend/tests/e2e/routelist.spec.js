// Visual regression for the RouteList page (/routes).
//
// API mocked via page.route() — no backend required.
// Fixtures cover /api/routes (scorecard), /api/routes/contributors, /api/targets,
// and the system trend endpoints (used by the SystemTrend component rendered at
// the top of RouteList).

import { test, expect } from '@playwright/test'
import { readFileSync } from 'fs'
import { fileURLToPath } from 'url'
import { join, dirname } from 'path'

const __dirname = dirname(fileURLToPath(import.meta.url))
const fixturesDir = join(__dirname, '../fixtures')

function fixture(name) {
  return JSON.parse(readFileSync(join(fixturesDir, name), 'utf8'))
}

test.beforeEach(async ({ page }) => {
  await page.route('**/api/**', async (route) => {
    const url = route.request().url()

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
      return route.fulfill({ json: { loaded_at: '2026-05-15T10:00:00', feed_version: '2026-05-15' } })
    }

    await route.continue()
  })
})

test('RouteList: smoke — Routes nav link visible', async ({ page }) => {
  await page.goto('/routes')
  await expect(page.getByRole('link', { name: 'Routes' })).toBeVisible()
})

test('RouteList: contributors table renders with route data', async ({ page }) => {
  await page.goto('/routes')
  // The contributors table should show "Biggest contributors" button as active
  // and the route D72 should appear in the table. Wait for the contributors
  // table heading row to be visible.
  await expect(page.getByText('Biggest contributors')).toBeVisible()
  // Wait for the contributor row to load — the route short name appears in the badge.
  await expect(page.getByRole('cell', { name: 'D72' })).toBeVisible()
})

test('RouteList: visual regression', async ({ page }) => {
  await page.goto('/routes')
  // Wait for contributors to load.
  await expect(page.getByRole('cell', { name: 'D72' })).toBeVisible()
  await page.waitForTimeout(500)
  await expect(page).toHaveScreenshot('routelist.png', { fullPage: true })
})
