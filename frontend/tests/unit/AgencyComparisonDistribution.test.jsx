/**
 * Route-level distribution rows + histogram on the agency comparison page
 * (NOTES-141). New file per the item's TDD instruction -- the existing
 * AgencyComparison.test.jsx (PR #198/#211) stays untouched and keeps
 * covering the pre-existing table shape with a fixture payload that has no
 * `route_distribution` block at all, proving this feature is additive.
 */
import { render, screen, waitFor, within } from '@testing-library/react'
import { vi } from 'vitest'
import AgencyComparison from '../../src/components/AgencyComparison'
import {
  ROUTE_DISTRIBUTION_METRICS,
  agencySeriesColor,
  buildDistributionHistogramData,
  formatDistributionStats,
} from '../../src/utils/agencyComparison'

// jsdom (the vitest test environment here) doesn't implement
// ResizeObserver, which recharts' <ResponsiveContainer> requires to
// measure its parent. Polyfill it directly on `globalThis` -- the same
// pattern `Sparkline.test.jsx` already uses for its own recharts-backed
// component -- rather than `vi.stubGlobal` in a `beforeEach`: a stub
// registered through vitest's mock registry can be torn down by
// `vi.unstubAllGlobals()` before an in-flight async re-render (e.g. this
// file's error-state retry test, which re-renders `ResponsiveContainer`
// after a `waitFor`) gets to mount its chart, which is what made CI flaky
// while local runs passed "by environment luck." A permanent assignment
// has no teardown to race. The chart itself renders at 0x0 under the
// polyfill -- these tests assert the surrounding table structure (labels,
// stats, sub-rows), not pixel output.
class ResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}
globalThis.ResizeObserver = ResizeObserver

const HISTOGRAM = [
  { label: '<60', count: 3 },
  { label: '60-70', count: 5 },
  { label: '70-80', count: 10 },
  { label: '80-90', count: 8 },
  { label: '90+', count: 2 },
]

const payloadWithDistribution = {
  window_start: '2026-07-23',
  window_end: '2026-08-12',
  agencies: [
    {
      agency: 'wmata',
      display_name: 'WMATA',
      metrics: {
        otp: { window_mean: 75.2, wow_delta: -2.1, days_included: 7, partial_days: 1 },
        service_delivered: { window_mean: 0.94, wow_delta: 0.01, days_included: 7, partial_days: 0 },
        swt: { window_mean: 300, wow_delta: -15, days_included: 7, partial_days: 0 },
        ewt: { window_mean: 90, wow_delta: 5, days_included: 7, partial_days: 0 },
        bunching: { window_mean: 0.04, wow_delta: -0.005, days_included: 7, partial_days: 0 },
      },
      service_level: { median_headway_seconds: 720, pct_at_most_15min: 0.6, n_headways: 100 },
      route_distribution: {
        otp: {
          route_count: 28,
          median: 74.0,
          p25: 65.0,
          p75: 82.0,
          histogram: HISTOGRAM,
          threshold: 75.0,
          share_at_or_above_threshold: 0.4,
        },
        service_delivered: {
          route_count: 28,
          median: 0.93,
          p25: 0.88,
          p75: 0.97,
          histogram: HISTOGRAM,
          threshold: 0.95,
          share_at_or_above_threshold: 0.36,
        },
      },
    },
    {
      agency: 'sfmta',
      display_name: 'SFMTA (Muni)',
      metrics: {
        otp: { window_mean: 71.0, wow_delta: 0.8, days_included: 7, partial_days: 0 },
        service_delivered: { window_mean: 0.89, wow_delta: -0.02, days_included: 7, partial_days: 0 },
        swt: { window_mean: 360, wow_delta: 10, days_included: 7, partial_days: 0 },
        ewt: { window_mean: 120, wow_delta: -8, days_included: 7, partial_days: 0 },
        bunching: { window_mean: 0.06, wow_delta: 0.01, days_included: 7, partial_days: 0 },
      },
      service_level: { median_headway_seconds: 600, pct_at_most_15min: 0.72, n_headways: 80 },
      route_distribution: {
        otp: {
          route_count: 0,
          median: null,
          p25: null,
          p75: null,
          histogram: HISTOGRAM.map((b) => ({ ...b, count: 0 })),
          threshold: 75.0,
          share_at_or_above_threshold: null,
        },
        service_delivered: {
          route_count: 14,
          median: 0.85,
          p25: 0.8,
          p75: 0.9,
          histogram: HISTOGRAM,
          threshold: 0.95,
          share_at_or_above_threshold: 0.1,
        },
      },
    },
  ],
  caveats: [
    'WMATA and SFMTA measure on-time performance windows differently.',
    'Route counts differ between agencies and a per-route mean weights every route equally.',
  ],
}

function mockFetch(impl) {
  vi.stubGlobal('fetch', vi.fn(impl))
}

afterEach(() => vi.unstubAllGlobals())

describe('utils/agencyComparison — distribution helpers', () => {
  test('formatDistributionStats returns null for a zero-route distribution', () => {
    expect(
      formatDistributionStats('otp', {
        route_count: 0,
        median: null,
        p25: null,
        p75: null,
        threshold: 75,
        share_at_or_above_threshold: null,
      }),
    ).toBeNull()
  })

  test('formatDistributionStats returns null for a missing distribution', () => {
    expect(formatDistributionStats('otp', undefined)).toBeNull()
  })

  test('formatDistributionStats formats median/IQR/share in canonical units', () => {
    const stats = formatDistributionStats('otp', {
      route_count: 28,
      median: 74.0,
      p25: 65.0,
      p75: 82.0,
      threshold: 75.0,
      share_at_or_above_threshold: 0.4,
    })
    expect(stats.median).toBe('74.0%')
    expect(stats.iqr).toBe('65.0% – 82.0%')
    expect(stats.share).toBe('40%')
    expect(stats.thresholdLabel).toBe('75.0%')
    expect(stats.routeCount).toBe(28)
  })

  test('formatDistributionStats scales service_delivered ratio to percent', () => {
    const stats = formatDistributionStats('service_delivered', {
      route_count: 14,
      median: 0.85,
      p25: 0.8,
      p75: 0.9,
      threshold: 0.95,
      share_at_or_above_threshold: 0.1,
    })
    expect(stats.median).toBe('85.0%')
    expect(stats.thresholdLabel).toBe('95.0%')
    expect(stats.share).toBe('10%')
  })

  test('buildDistributionHistogramData builds one row per bucket keyed by agency', () => {
    const data = buildDistributionHistogramData(payloadWithDistribution.agencies, 'otp')
    expect(data).toHaveLength(5)
    expect(data[0]).toEqual({ label: '<60', wmata: 3, sfmta: 0 })
    expect(data[2]).toEqual({ label: '70-80', wmata: 10, sfmta: 0 })
  })

  test('buildDistributionHistogramData returns an empty array when no agency has a histogram', () => {
    expect(buildDistributionHistogramData([{ agency: 'wmata' }], 'otp')).toEqual([])
  })

  test('agencySeriesColor is fixed per agency key and falls back for unknown agencies', () => {
    expect(agencySeriesColor('wmata')).toBe('#2a78d6')
    expect(agencySeriesColor('sfmta')).toBe('#eb6834')
    expect(agencySeriesColor('some-future-agency')).toBe('#64748b')
  })

  test('ROUTE_DISTRIBUTION_METRICS covers exactly otp and service_delivered', () => {
    expect(ROUTE_DISTRIBUTION_METRICS).toEqual(['otp', 'service_delivered'])
  })
})

describe('AgencyComparison — route distribution rendering', () => {
  test('renders a "Route distribution" sub-row with stats for OTP and service_delivered only', async () => {
    mockFetch(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve(payloadWithDistribution) }),
    )
    render(<AgencyComparison />)
    await waitFor(() => expect(screen.getByRole('table')).toBeVisible())

    const distributionLabels = screen.getAllByText('Route distribution')
    // One sub-row for OTP, one for service_delivered -- not for swt/ewt/bunching.
    expect(distributionLabels).toHaveLength(2)

    const otpDistributionRow = screen.getByText('On-time performance').closest('tr').nextSibling
    expect(within(otpDistributionRow).getByText('74.0%')).toBeVisible() // median
    expect(within(otpDistributionRow).getByText('65.0% – 82.0%')).toBeVisible() // IQR
    expect(within(otpDistributionRow).getByText('28')).toBeVisible() // route count
    expect(within(otpDistributionRow).getByText('40% of routes')).toBeVisible()
  })

  test('shows a no-data message for an agency with zero scored routes', async () => {
    mockFetch(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve(payloadWithDistribution) }),
    )
    render(<AgencyComparison />)
    await waitFor(() => expect(screen.getByRole('table')).toBeVisible())

    const otpDistributionRow = screen.getByText('On-time performance').closest('tr').nextSibling
    expect(within(otpDistributionRow).getByText(/No routes scored/)).toBeVisible()
  })

  test('renders a "Distribution shape" sub-row for the metrics with data', async () => {
    mockFetch(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve(payloadWithDistribution) }),
    )
    render(<AgencyComparison />)
    await waitFor(() => expect(screen.getByRole('table')).toBeVisible())

    expect(screen.getAllByText('Distribution shape')).toHaveLength(2)
  })

  test('does not render distribution sub-rows for swt/ewt/bunching', async () => {
    mockFetch(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve(payloadWithDistribution) }),
    )
    render(<AgencyComparison />)
    await waitFor(() => expect(screen.getByRole('table')).toBeVisible())

    const ewtRow = screen.getByText('Excess wait time').closest('tr')
    expect(ewtRow.nextSibling.querySelector('.agency-distribution-sublabel')).toBeNull()
  })

  test('legacy payload with no route_distribution renders the table with no distribution rows (additive)', async () => {
    const legacyPayload = {
      ...payloadWithDistribution,
      agencies: payloadWithDistribution.agencies.map(({ route_distribution: _drop, ...rest }) => rest),
    }
    mockFetch(() => Promise.resolve({ ok: true, json: () => Promise.resolve(legacyPayload) }))
    render(<AgencyComparison />)
    await waitFor(() => expect(screen.getByRole('table')).toBeVisible())

    expect(screen.queryByText('Route distribution')).toBeNull()
    expect(screen.queryByText('Distribution shape')).toBeNull()
    // The pre-existing headline cell still renders correctly.
    expect(screen.getByText('75.2%')).toBeVisible()
  })

  test('error state shows a retry button that re-fetches the endpoint', async () => {
    let callCount = 0
    mockFetch(() => {
      callCount += 1
      if (callCount === 1) return Promise.reject(new Error('network down'))
      return Promise.resolve({ ok: true, json: () => Promise.resolve(payloadWithDistribution) })
    })
    render(<AgencyComparison />)

    await waitFor(() =>
      expect(screen.getByText(/Unable to load agency comparison/i)).toBeVisible(),
    )
    const retryButton = screen.getByRole('button', { name: /try again/i })
    expect(retryButton).toBeVisible()

    retryButton.click()

    await waitFor(() => expect(screen.getByRole('table')).toBeVisible())
    expect(callCount).toBe(2)
  })
})
