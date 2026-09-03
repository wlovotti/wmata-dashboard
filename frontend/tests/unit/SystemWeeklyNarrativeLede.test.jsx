/**
 * SystemWeeklyNarrativeLede (system weekly narrative, PR #219): the
 * Overview page's editorial lede, fed by GET /api/system/weekly-narrative.
 * Never load-bearing: renders nothing until a narrative row exists.
 *
 * PR #219 review finding 3: the component previously guarded only on
 * `!data`, so a cached row with an empty/whitespace-only `narrative` (e.g.
 * an exit-0-but-empty `claude` run that slipped past the writer's guard)
 * rendered an empty bordered paragraph with just the meta line. It must
 * guard on `data.narrative` being non-empty instead.
 */
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import SystemWeeklyNarrativeLede from '../../src/components/SystemWeeklyNarrativeLede'

// NOTES-143: the component now reads the current agency (`useAgency`,
// which needs Router context for `useSearchParams`) to build its fetch
// URL, so every render below needs a MemoryRouter ancestor.
function renderLede() {
  return render(
    <MemoryRouter>
      <SystemWeeklyNarrativeLede />
    </MemoryRouter>,
  )
}

const payload = {
  as_of_date: '2026-08-16',
  narrative: 'Riders on frequent routes waited about a minute longer than scheduled this week.',
  generated_at: '2026-08-17T12:00:00Z',
  model_id: 'claude-sonnet-4-6',
  prompt_version: 'v1',
  is_stale: false,
}

function mockFetch(impl) {
  vi.stubGlobal('fetch', vi.fn(impl))
}

afterEach(() => vi.unstubAllGlobals())

describe('SystemWeeklyNarrativeLede', () => {
  test('renders nothing on 404 / no data', async () => {
    mockFetch(() => Promise.resolve({ ok: false, json: () => Promise.resolve(null) }))
    const { container } = renderLede()
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })

  test('renders nothing on fetch failure', async () => {
    mockFetch(() => Promise.reject(new Error('down')))
    const { container } = renderLede()
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })

  test('renders nothing when the cached narrative is empty', async () => {
    mockFetch(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({ ...payload, narrative: '' }) }),
    )
    const { container } = renderLede()
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })

  test('renders nothing when the cached narrative is whitespace-only', async () => {
    mockFetch(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ ...payload, narrative: '   \n  ' }),
      }),
    )
    const { container } = renderLede()
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })

  test('renders the narrative and meta line when present', async () => {
    mockFetch(() => Promise.resolve({ ok: true, json: () => Promise.resolve(payload) }))
    renderLede()
    await waitFor(() => expect(screen.getByText(payload.narrative)).toBeVisible())
    expect(screen.getByText(/week ending 2026-08-16/i)).toBeVisible()
    expect(screen.getByText(/claude-sonnet-4-6/)).toBeVisible()
    expect(screen.queryByText(/may be out of date/i)).not.toBeInTheDocument()
  })

  test('shows the staleness banner when is_stale is true', async () => {
    mockFetch(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({ ...payload, is_stale: true }) }),
    )
    renderLede()
    await waitFor(() => expect(screen.getByText(/may be out of date/i)).toBeVisible())
  })

  // PR #242 review finding 11: a page-level sanity check that `apiUrl`'s
  // `window.location.search` fallback actually reaches a real fetch —
  // this component builds its URL via `apiUrl('/api/system/weekly-narrative')`
  // with no explicit `agency`, relying entirely on that fallback.
  // `window.history.pushState` (not `MemoryRouter`'s `initialEntries`,
  // which never touches the real `window.location`) is what makes this a
  // meaningful positive-case test.
  test('carries agency=sfmta on the fetch when the current URL has ?agency=sfmta', async () => {
    window.history.pushState({}, '', '/?agency=sfmta')
    try {
      mockFetch(() => Promise.resolve({ ok: true, json: () => Promise.resolve(payload) }))
      renderLede()
      await waitFor(() => expect(fetch).toHaveBeenCalled())
      expect(fetch.mock.calls[0][0]).toBe('/api/system/weekly-narrative?agency=sfmta')
    } finally {
      window.history.pushState({}, '', '/')
    }
  })
})
