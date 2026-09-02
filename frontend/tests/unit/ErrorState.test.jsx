/**
 * ErrorState (NOTES-142): the shared inline error message + Retry button
 * used across the diagnostics pages. Pins the rendered message text and
 * that clicking Retry invokes the caller's callback exactly once per click
 * — callers rely on that to re-run their failed fetch.
 */
import { render, screen, fireEvent } from '@testing-library/react'
import { vi } from 'vitest'
import ErrorState from '../../src/components/ErrorState'

describe('ErrorState', () => {
  test('renders the default title and message', () => {
    render(<ErrorState message="HTTP 500" />)
    expect(screen.getByRole('alert')).toHaveTextContent('Unable to load data: HTTP 500')
  })

  test('renders a custom title', () => {
    render(<ErrorState title="Unable to load blocks" message="Network error" />)
    expect(screen.getByRole('alert')).toHaveTextContent('Unable to load blocks: Network error')
  })

  test('omits the retry button when no onRetry is passed', () => {
    render(<ErrorState message="oops" />)
    expect(screen.queryByRole('button', { name: 'Retry' })).not.toBeInTheDocument()
  })

  test('clicking Retry calls onRetry once', () => {
    const onRetry = vi.fn()
    render(<ErrorState message="oops" onRetry={onRetry} />)
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    expect(onRetry).toHaveBeenCalledTimes(1)
  })
})
