/**
 * CompareStrip (NOTES-84): one-row WMATA-vs-Muni OTP teaser inside the hero,
 * linking to /compare. Never load-bearing: any fetch problem renders nothing.
 *
 * PR #TBD finding 2: CompareStrip was the one Overview-hero fetch left
 * uncached after PR #217 — it fetched raw instead of routing through
 * useMultiFetch, so it popped in on every return visit. The last test below
 * pins the fix: a second mount serves the cached value instantly (no
 * network round-trip needed before the strip renders).
 */
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import CompareStrip from '../../src/components/CompareStrip'

const payload = {
  window_start: '2026-07-23',
  agencies: [
    {
      agency: 'wmata',
      display_name: 'WMATA',
      metrics: { otp: { window_mean: 75.2, wow_delta: -2.1 } },
    },
    {
      agency: 'sfmta',
      display_name: 'SFMTA (Muni)',
      metrics: { otp: { window_mean: 71.0, wow_delta: 0.8 } },
    },
  ],
}

function mockFetch(impl) {
  vi.stubGlobal('fetch', vi.fn(impl))
}

// CompareStrip now shares the module-level fetchCache with every other
// useMultiFetch caller (PR #TBD finding 2); tests/setup.js's global
// afterEach already clears it between tests, so an earlier test's cached
// payload never bleeds into a later one here.
afterEach(() => vi.unstubAllGlobals())

describe('CompareStrip', () => {
  test('renders both agencies OTP and links to /compare', async () => {
    mockFetch(() => Promise.resolve({ ok: true, json: () => Promise.resolve(payload) }))
    render(
      <MemoryRouter>
        <CompareStrip />
      </MemoryRouter>,
    )
    await waitFor(() => expect(screen.getByText(/WMATA/)).toBeVisible())
    expect(screen.getByText(/SFMTA \(Muni\)/)).toBeVisible()
    expect(screen.getByRole('link', { name: /full comparison/i })).toHaveAttribute(
      'href',
      '/compare',
    )
    // Window disclosure (final-review wave): the strip's means cover the
    // whole matched window, not the hero's 7-day figure above it — the
    // trailing label makes that explicit.
    expect(screen.getByText(/since 2026-07-23/)).toBeVisible()
  })

  test('renders nothing on fetch failure', async () => {
    mockFetch(() => Promise.reject(new Error('down')))
    const { container } = render(
      <MemoryRouter>
        <CompareStrip />
      </MemoryRouter>,
    )
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })

  test('renders nothing when fewer than two agencies report', async () => {
    mockFetch(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ agencies: [payload.agencies[0]] }),
      }),
    )
    const { container } = render(
      <MemoryRouter>
        <CompareStrip />
      </MemoryRouter>,
    )
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })

  test('a second mount serves the cached payload instantly, without waiting on the network (PR #TBD finding 2)', async () => {
    mockFetch(() => Promise.resolve({ ok: true, json: () => Promise.resolve(payload) }))
    const { unmount } = render(
      <MemoryRouter>
        <CompareStrip />
      </MemoryRouter>,
    )
    await waitFor(() => expect(screen.getByText(/WMATA/)).toBeVisible())
    unmount()

    // Second mount: even though the mock fetch would resolve fine again,
    // the assertion below runs synchronously (no `waitFor`) — it only
    // passes if the cached value from the first mount painted immediately.
    render(
      <MemoryRouter>
        <CompareStrip />
      </MemoryRouter>,
    )
    expect(screen.getByText(/WMATA/)).toBeVisible()
    expect(screen.getByText(/SFMTA \(Muni\)/)).toBeVisible()
  })
})
