// Playwright configuration for WMATA dashboard visual regression tests.
//
// Chromium-only for the initial scaffold. Adding Firefox / WebKit is a
// follow-up task once baselines stabilize — see NOTES for tracking.
//
// Visual regression baselines are platform-specific. Baselines committed
// in this repo were generated on macOS. CI (Linux) must regenerate them
// via:
//   npx playwright test --update-snapshots
// and commit the result in a follow-up PR (see PR body for the exact steps).

import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './tests/e2e',
  timeout: 30_000,
  retries: 0,
  workers: 1,

  expect: {
    // Small tolerance for sub-pixel anti-aliasing differences across runs.
    toHaveScreenshot: { maxDiffPixelRatio: 0.01 },
  },

  use: {
    baseURL: 'http://localhost:5173',
    // Capture a screenshot on failure for easier debugging.
    screenshot: 'only-on-failure',
    // No video recording — keeps artifact size small for the initial scaffold.
  },

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],

  // Start the Vite dev server automatically when running locally.
  // In CI (process.env.CI is set) the server must already be running, OR
  // Playwright's webServer block starts it — reuseExistingServer lets both
  // paths work without a hard error when the port is already taken.
  webServer: {
    command: 'npm run dev',
    port: 5173,
    reuseExistingServer: !process.env.CI,
    timeout: 30_000,
  },
})
