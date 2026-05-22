// Visual regression and smoke tests for the Segments page (/segments).
//
// API mocked via page.route() — no backend required.
// Fixture: /api/segments → segments.json (3 stop-pair rows).

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
    if (url.includes('/api/segments')) {
      return route.fulfill({ json: fixture('segments.json') })
    }
    await route.continue()
  })
})

test('Segments: nav link visible', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('link', { name: 'Segments' })).toBeVisible()
})

test('Segments: page heading renders', async ({ page }) => {
  await page.goto('/segments')
  await expect(page.getByRole('heading', { name: 'Cross-route segment diagnostic' })).toBeVisible()
})

test('Segments: ranked rows appear', async ({ page }) => {
  await page.goto('/segments')
  // First row stop names from fixture
  await expect(page.getByText('Wisconsin Ave NW + M St NW')).toBeVisible()
  await expect(page.getByText('Wisconsin Ave NW + N St NW')).toBeVisible()
})

test('Segments: click to expand drilldown', async ({ page }) => {
  await page.goto('/segments')
  // Before click: no "Mean slip" header (that's inside the drilldown table)
  await expect(page.getByText('Mean slip')).not.toBeVisible()
  // Click the first row to expand it
  const firstRow = page.locator('tbody tr').first()
  await firstRow.click()
  // After click: the per-route drilldown table header "Mean slip" should appear
  await expect(page.getByText('Mean slip')).toBeVisible()
})

test('Segments: visual regression', async ({ page }) => {
  await page.goto('/segments')
  // Wait for the table to be fully rendered
  await page.waitForSelector('tbody tr')
  await expect(page).toHaveScreenshot('segments-chromium.png', { fullPage: false })
})
