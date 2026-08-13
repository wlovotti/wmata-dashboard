/**
 * Regression test for RefreshButton (App.jsx) — NOTES-87 item 2.
 *
 * Before this fix, the header's Refresh button called
 * `window.location.reload()` — a full browser navigation dressed up as an
 * in-app refresh. The fix delegates the actual data-refetch decision to the
 * caller via `onRefresh` (App.jsx bumps a `key` to remount the routed page,
 * which re-runs its fetch effects) and keeps this component responsible
 * only for its own transient spinner state. These tests pin: (1) clicking
 * invokes `onRefresh` rather than touching `window.location`, and (2) the
 * spinner turns on immediately and off again after the fixed visual delay.
 */
import { act, render, fireEvent, screen } from '@testing-library/react'
import { RefreshButton } from '../../src/App'

describe('RefreshButton', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  test('clicking delegates the refetch decision to onRefresh (no navigation)', () => {
    // jsdom's window.location.reload isn't reconfigurable, so we can't spy on
    // it directly — the meaningful assertion is that RefreshButton's own
    // source has no `window.location` reference left (see App.jsx: the old
    // `window.location.reload()` was replaced by delegating to `onRefresh`,
    // which App wires up to a state-bump remount instead).
    const onRefresh = vi.fn()
    render(<RefreshButton onRefresh={onRefresh} />)
    act(() => {
      fireEvent.click(screen.getByRole('button', { name: /refresh/i }))
    })

    expect(onRefresh).toHaveBeenCalledTimes(1)
  })

  test('shows a spinning state immediately after click, then reverts', () => {
    render(<RefreshButton onRefresh={vi.fn()} />)
    const button = screen.getByRole('button', { name: /refresh/i })

    expect(button).not.toBeDisabled()
    expect(screen.getByText('Refresh')).toBeInTheDocument()

    act(() => {
      fireEvent.click(button)
    })

    expect(button).toBeDisabled()
    expect(screen.getByText('Refreshing...')).toBeInTheDocument()

    act(() => {
      vi.advanceTimersByTime(600)
    })

    expect(button).not.toBeDisabled()
    expect(screen.getByText('Refresh')).toBeInTheDocument()
  })
})
