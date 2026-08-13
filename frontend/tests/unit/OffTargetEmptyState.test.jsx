/**
 * Regression test for OffTargetEmptyState (Overview.jsx) — the
 * frontend-chrome honesty fixes (PR #204).
 *
 * The Off-target panel only ranks routes with a hand-edited override in
 * `config/route_targets.yaml`, which ships with an empty `routes: {}`
 * block — so the panel renders empty by default, not because of a bug.
 * Before this fix the explanation was thin ("Set per-route targets in
 * config/route_targets.yaml to populate this view."); these tests pin
 * that the three distinct empty cases — targets not loaded yet (or fetch
 * failed), no overrides configured at all, and overrides exist but none
 * for the selected metric — each get their own, more explicit message,
 * and that they aren't conflated. In particular, `targetsLoaded=false`
 * must NOT render the confident "config/route_targets.yaml ships empty"
 * claim, since while the fetch is in flight (or failed) the panel has no
 * idea whether the config is actually empty.
 */
import { render, screen } from '@testing-library/react'
import { OffTargetEmptyState } from '../../src/components/Overview'

describe('OffTargetEmptyState', () => {
  test('targetsLoaded=false shows a neutral message, not the config-is-empty claim', () => {
    render(
      <OffTargetEmptyState hasAnyOverrides={false} targetsLoaded={false} metricLabel="EWT" />,
    )
    // Must not assert anything about the config file's actual contents —
    // we don't know them yet.
    expect(
      screen.queryByText(/no route has a per-route target/i),
    ).not.toBeInTheDocument()
    expect(
      screen.queryByText(/config\/route_targets\.yaml/),
    ).not.toBeInTheDocument()
    expect(
      screen.queryByText(/no per-route overrides configured for/i),
    ).not.toBeInTheDocument()
    expect(screen.getByText(/isn't available right now/i)).toBeInTheDocument()
  })

  test('hasAnyOverrides=false explains the config file is empty by default', () => {
    render(
      <OffTargetEmptyState hasAnyOverrides={false} targetsLoaded={true} metricLabel="EWT" />,
    )
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
    render(
      <OffTargetEmptyState hasAnyOverrides={true} targetsLoaded={true} metricLabel="EWT" />,
    )
    expect(
      screen.getByText('No per-route overrides configured for EWT.'),
    ).toBeInTheDocument()
    // Should not render the broader "config is empty" message.
    expect(
      screen.queryByText(/no route has a per-route target/i),
    ).not.toBeInTheDocument()
  })
})
