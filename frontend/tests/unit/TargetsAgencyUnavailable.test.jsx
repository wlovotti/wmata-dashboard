/**
 * Targets.jsx "not available for this agency" state (NOTES-143).
 *
 * `/api/targets` doesn't even accept an `agency` query param — it always
 * reads WMATA's `config/route_targets.yaml`. Rendering it under a Muni
 * header would show WMATA's own targets as if they were configured for
 * Muni, so the page must skip both fetches entirely for `agency=sfmta`
 * and show a short unavailable card instead of a (silently wrong) table.
 */
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import Targets from '../../src/components/Targets'

function mockFetch(impl) {
  vi.stubGlobal('fetch', vi.fn(impl))
}

afterEach(() => vi.unstubAllGlobals())

function renderTargets(initialEntries) {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <Targets />
    </MemoryRouter>,
  )
}

describe('Targets agency availability', () => {
  test('agency=sfmta never fetches and shows an unavailable card', async () => {
    mockFetch(() => Promise.reject(new Error('should not be called')))
    renderTargets(['/targets?agency=sfmta'])

    await waitFor(() => expect(screen.getByText(/not available for muni/i)).toBeVisible())
    expect(fetch).not.toHaveBeenCalled()
    expect(screen.queryByText('Performance targets')).toBeVisible()
    expect(screen.queryByText('System defaults')).not.toBeInTheDocument()
  })

  test('the default agency (wmata) still fetches and renders the table', async () => {
    mockFetch((url) => {
      const u = String(url)
      if (u.startsWith('/api/targets')) {
        return Promise.resolve({
          ok: true,
          json: () =>
            Promise.resolve({
              system_default: { otp: 75.0, service_delivered: 0.95, ewt: 180.0, bunching: 0.04 },
              routes: {},
              metrics: ['otp', 'service_delivered', 'ewt', 'bunching'],
            }),
        })
      }
      if (u.startsWith('/api/routes')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ routes: [] }) })
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({}) })
    })
    renderTargets(['/targets'])

    await waitFor(() => expect(screen.getByText('System defaults')).toBeVisible())
    expect(screen.queryByText(/not available for/i)).not.toBeInTheDocument()
  })

  test('the "← Diagnostics" link carries the agency selection back', async () => {
    mockFetch(() => Promise.reject(new Error('should not be called')))
    renderTargets(['/targets?agency=sfmta'])

    await waitFor(() => expect(screen.getByText(/not available for muni/i)).toBeVisible())
    expect(screen.getByRole('link', { name: /diagnostics/i })).toHaveAttribute(
      'href',
      '/diagnostics?agency=sfmta',
    )
  })
})
