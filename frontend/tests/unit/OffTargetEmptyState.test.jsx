/**
 * Regression test for OffTargetEmptyState (Overview.jsx) — NOTES-87 item 3.
 *
 * The Off-target panel only ranks routes with a hand-edited override in
 * `config/route_targets.yaml`, which ships with an empty `routes: {}`
 * block — so the panel renders empty by default, not because of a bug.
 * Before this fix the explanation was thin ("Set per-route targets in
 * config/route_targets.yaml to populate this view."); these tests pin
 * that the two distinct empty cases — no overrides configured at all vs.
 * overrides exist but none for the selected metric — each get their own,
 * more explicit message, and that the two aren't conflated.
 */
import { render, screen } from '@testing-library/react'
import { OffTargetEmptyState } from '../../src/components/Overview'

describe('OffTargetEmptyState', () => {
  test('hasAnyOverrides=false explains the config file is empty by default', () => {
    render(<OffTargetEmptyState hasAnyOverrides={false} metricLabel="EWT" />)
    expect(
      screen.getByText(/no route has a per-route target/i),
    ).toBeInTheDocument()
    expect(screen.getByText(/config\/route_targets\.yaml/)).toBeInTheDocument()
    // Should not render the narrower "none for this metric" message.
    expect(
      screen.queryByText(/no per-route overrides configured for/i),
    ).not.toBeInTheDocument()
  })

  test('hasAnyOverrides=true names the metric that has no overrides', () => {
    render(<OffTargetEmptyState hasAnyOverrides={true} metricLabel="EWT" />)
    expect(
      screen.getByText('No per-route overrides configured for EWT.'),
    ).toBeInTheDocument()
    // Should not render the broader "config is empty" message.
    expect(
      screen.queryByText(/no route has a per-route target/i),
    ).not.toBeInTheDocument()
  })
})
