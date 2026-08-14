/**
 * CompareStrip (NOTES-84): one-row WMATA-vs-Muni OTP teaser inside the hero,
 * linking to /compare. Never load-bearing: any fetch problem renders nothing.
 */
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import CompareStrip from '../../src/components/CompareStrip'

const payload = {
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
})
