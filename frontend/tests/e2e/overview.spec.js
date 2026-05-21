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
  // Intercept all /api/** requests before navigation.
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

    // Fall through to actual network for anything unmatched.
    await route.continue()
  })
})

test('Overview: smoke — nav link visible', async ({ page }) => {
  await page.goto('/')
  // The "Overview" nav link must be present regardless of data loading state.
  await expect(page.getByRole('link', { name: 'Overview' })).toBeVisible()
})

test('Overview: "Where to look" heading renders', async ({ page }) => {
  await page.goto('/')
  // Use role heading to be specific — the text also appears in a paragraph.
  await expect(page.getByRole('heading', { name: 'Where to look' })).toBeVisible()
})

test('Overview: "What changed" panel renders', async ({ page }) => {
  await page.goto('/')
  // Wait for the panel heading to appear (scorecard fetch must complete).
  await expect(page.getByRole('heading', { name: 'What changed' })).toBeVisible()
})

test('Overview: visual regression', async ({ page }) => {
  await page.goto('/')
  // Wait for async content: contributors heading and "What changed" panel.
  await expect(page.getByRole('heading', { name: 'Where to look' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'What changed' })).toBeVisible()
  // Additional settle: sparklines are async recharts renders; a short wait
  // reduces flicker without making the test brittle.
  await page.waitForTimeout(500)
  await expect(page).toHaveScreenshot('overview.png', { fullPage: true })
})
