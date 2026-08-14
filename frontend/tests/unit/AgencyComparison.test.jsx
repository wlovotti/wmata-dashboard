/**
 * AgencyComparison (PR #198; reformatted to a metric-row table by
 * the comparison-table reformat, PR TODO). Verifies the table shape — one row per metric, one column
 * per agency — plus the loading/error/empty states and the caveats list
 * that the reformat had to preserve unchanged.
 */
import { render, screen, waitFor, within } from '@testing-library/react'
import { vi } from 'vitest'
import AgencyComparison from '../../src/components/AgencyComparison'

const payload = {
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
    },
  ],
  caveats: ['WMATA and SFMTA measure on-time performance windows differently.'],
}

function mockFetch(impl) {
  vi.stubGlobal('fetch', vi.fn(impl))
}

afterEach(() => vi.unstubAllGlobals())

describe('AgencyComparison', () => {
  test('shows a loading state before the fetch resolves', () => {
    mockFetch(() => new Promise(() => {})) // never resolves
    render(<AgencyComparison />)
    expect(screen.getByText(/Loading agency comparison/i)).toBeVisible()
  })

  test('shows an error state on fetch failure', async () => {
    mockFetch(() => Promise.reject(new Error('down')))
    render(<AgencyComparison />)
    await waitFor(() =>
      expect(screen.getByText(/Unable to load agency comparison/i)).toBeVisible(),
    )
  })

  test('shows the no-agency-configured message when the payload has none', async () => {
    mockFetch(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({ agencies: [], caveats: [] }) }),
    )
    render(<AgencyComparison />)
    await waitFor(() => expect(screen.getByText(/No agency database/i)).toBeVisible())
  })

  test('renders one table row per metric with both agencies as columns', async () => {
    mockFetch(() => Promise.resolve({ ok: true, json: () => Promise.resolve(payload) }))
    render(<AgencyComparison />)
    await waitFor(() => expect(screen.getByRole('table')).toBeVisible())

    // Header row: one column per agency, in payload order.
    const headerCells = screen.getAllByRole('columnheader')
    expect(headerCells.map((th) => th.textContent)).toEqual(['Metric', 'WMATA', 'SFMTA (Muni)'])

    // OTP row: label + both agencies' values sit on the same <tr>.
    const otpRow = screen.getByText('On-time performance').closest('tr')
    expect(within(otpRow).getByText('75.2%')).toBeVisible()
    expect(within(otpRow).getByText('71.0%')).toBeVisible()
    // Delta pill survives the reformat.
    expect(within(otpRow).getByText(/−2.1 pts vs prior week/)).toBeVisible()
    expect(within(otpRow).getByText(/\+0.8 pts vs prior week/)).toBeVisible()
    // Partial-day disclosure survives the reformat.
    expect(within(otpRow).getByText(/1 of 7 days flagged partial/)).toBeVisible()

    // Every other headline metric also gets its own row.
    for (const label of [
      'Service delivered',
      'Scheduled wait (frequent svc)',
      'Excess wait time',
      'Bunching rate',
    ]) {
      expect(screen.getByText(label).closest('tr')).not.toBeNull()
    }

    // Daytime service-level row, including the ≤15-min share context line.
    const serviceLevelRow = screen.getByText('Daytime service level').closest('tr')
    expect(within(serviceLevelRow).getByText('12.0 min')).toBeVisible()
    expect(within(serviceLevelRow).getByText('10.0 min')).toBeVisible()
    expect(
      within(serviceLevelRow).getByText(/60% of scheduled service every ≤15 min/),
    ).toBeVisible()

    // Window framing line and caveats list are unchanged by the reformat.
    expect(screen.getByText(/matched window since 2026-07-23/)).toBeVisible()
    expect(screen.getByText('Comparability caveats')).toBeVisible()
    expect(
      screen.getByText('WMATA and SFMTA measure on-time performance windows differently.'),
    ).toBeVisible()
  })
})
