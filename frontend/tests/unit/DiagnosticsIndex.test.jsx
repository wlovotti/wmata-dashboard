/**
 * DiagnosticsIndex (NOTES-84 nav collapse): the /diagnostics landing page
 * cards the four tools demoted from the top-level nav. Pins that all four
 * links exist with their original URLs — the collapse moves nav entries,
 * never breaks bookmarks.
 */
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import DiagnosticsIndex from '../../src/components/DiagnosticsIndex'

describe('DiagnosticsIndex', () => {
  test('renders a card link for each demoted tool at its original URL', () => {
    render(
      <MemoryRouter>
        <DiagnosticsIndex />
      </MemoryRouter>,
    )
    expect(screen.getByRole('link', { name: /blocks/i })).toHaveAttribute('href', '/blocks')
    expect(screen.getByRole('link', { name: /targets/i })).toHaveAttribute('href', '/targets')
    expect(screen.getByRole('link', { name: /schedule audit/i })).toHaveAttribute(
      'href',
      '/schedule-audit',
    )
    expect(screen.getByRole('link', { name: /segments/i })).toHaveAttribute('href', '/segments')
  })

  test('renders the page heading', () => {
    render(
      <MemoryRouter>
        <DiagnosticsIndex />
      </MemoryRouter>,
    )
    expect(screen.getByRole('heading', { name: 'Diagnostics' })).toBeVisible()
  })
})
