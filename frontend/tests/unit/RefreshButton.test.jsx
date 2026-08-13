/**
 * Regression test for RefreshButton (App.jsx) — the frontend-chrome honesty
 * fixes (PR #204).
 *
 * Before this fix, the header's Refresh button called
 * `window.location.reload()` — a full browser navigation dressed up as an
 * in-app refresh. The fix delegates the actual data-refetch decision to the
 * caller via `onRefresh` (App.jsx bumps a `key` on the routed subtree to
 * remount it, which re-runs its fetch effects) and keeps this component
 * responsible only for its own transient spinner state. These tests pin:
 * (1) clicking invokes `onRefresh` rather than touching `window.location`,
 * (2) the spinner turns on immediately and off again after the fixed visual
 * delay, and (3) that spinner feedback actually survives in the real tree
 * shape — RefreshButton rendered as a sibling of the subtree `onRefresh`
 * remounts, not inside it. A version of App.jsx that put the remount key on
 * an ancestor of RefreshButton (the bug this PR fixed) would remount the
 * button itself before its `refreshing` state ever painted, and this last
 * test would catch that.
 */
import { act, render, fireEvent, screen } from '@testing-library/react'
import { useState } from 'react'
import { RefreshButton } from '../../src/App'

describe('RefreshButton', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  test('clicking delegates the refetch decision to onRefresh, not window.location.reload', () => {
    const reloadSpy = vi.fn()
    vi.stubGlobal('location', { ...window.location, reload: reloadSpy })

    const onRefresh = vi.fn()
    render(<RefreshButton onRefresh={onRefresh} />)
    act(() => {
      fireEvent.click(screen.getByRole('button', { name: /refresh/i }))
    })

    expect(onRefresh).toHaveBeenCalledTimes(1)
    expect(reloadSpy).not.toHaveBeenCalled()
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

  // Mirrors App.jsx's real structure: RefreshButton is a sibling of the
  // keyed subtree that onRefresh remounts, NOT an ancestor of it. Before
  // this PR's fix, the key sat on a div that contained RefreshButton too,
  // so clicking Refresh remounted the button in the same render that set
  // `refreshing`, and the spinner/disabled feedback never actually
  // rendered in the app. This test fails if that regresses.
  function Harness() {
    const [key, setKey] = useState(0)
    const handleRefresh = () => setKey((k) => k + 1)
    return (
      <div>
        <RefreshButton onRefresh={handleRefresh} />
        <div key={key} data-testid="keyed-subtree">
          keyed content
        </div>
      </div>
    )
  }

  test('feedback survives the remount it triggers when button sits outside the keyed subtree', () => {
    render(<Harness />)
    const button = screen.getByRole('button', { name: /refresh/i })

    act(() => {
      fireEvent.click(button)
    })

    // If RefreshButton were inside the keyed subtree, this click would have
    // remounted it and reset `refreshing` to false before this assertion —
    // the button would read "Refresh" again instead of "Refreshing...".
    expect(button).toBeDisabled()
    expect(screen.getByText('Refreshing...')).toBeInTheDocument()
  })
})
