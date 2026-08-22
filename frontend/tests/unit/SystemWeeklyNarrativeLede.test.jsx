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
import { vi } from 'vitest'
import SystemWeeklyNarrativeLede from '../../src/components/SystemWeeklyNarrativeLede'

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
    const { container } = render(<SystemWeeklyNarrativeLede />)
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })

  test('renders nothing on fetch failure', async () => {
    mockFetch(() => Promise.reject(new Error('down')))
    const { container } = render(<SystemWeeklyNarrativeLede />)
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })

  test('renders nothing when the cached narrative is empty', async () => {
    mockFetch(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({ ...payload, narrative: '' }) }),
    )
    const { container } = render(<SystemWeeklyNarrativeLede />)
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
    const { container } = render(<SystemWeeklyNarrativeLede />)
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })

  test('renders the narrative and meta line when present', async () => {
    mockFetch(() => Promise.resolve({ ok: true, json: () => Promise.resolve(payload) }))
    render(<SystemWeeklyNarrativeLede />)
    await waitFor(() => expect(screen.getByText(payload.narrative)).toBeVisible())
    expect(screen.getByText(/week ending 2026-08-16/i)).toBeVisible()
    expect(screen.getByText(/claude-sonnet-4-6/)).toBeVisible()
    expect(screen.queryByText(/may be out of date/i)).not.toBeInTheDocument()
  })

  test('shows the staleness banner when is_stale is true', async () => {
    mockFetch(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({ ...payload, is_stale: true }) }),
    )
    render(<SystemWeeklyNarrativeLede />)
    await waitFor(() => expect(screen.getByText(/may be out of date/i)).toBeVisible())
  })
})
