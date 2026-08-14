/**
 * OverviewHero (NOTES-84): the big-number verdict that replaced the
 * HealthPulse banner. Pins the plain-language framing rules:
 *   - headline = 7-day OTP mean with an up/down/steady clause
 *   - subline = routes-below-target count
 *   - a "sore spot" sentence appears only when the worst metric isn't OTP
 */
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import OverviewHero from '../../src/components/OverviewHero'

const day = (i) => `2026-08-${String(i).padStart(2, '0')}`

// 14 clean days: prior week at 77, recent week at 75 → delta −2.0 pp.
const otpSeries = [
  ...[1, 2, 3, 4, 5, 6, 7].map((i) => ({ date: day(i), value: 77 })),
  ...[8, 9, 10, 11, 12, 13, 14].map((i) => ({ date: day(i), value: 75 })),
]

const systemMetrics = (bunchingCurrent) => [
  { key: 'otp', label: 'OTP', higherIsBetter: true, current: 75, target: 75 },
  { key: 'bunching', label: 'Bunching', higherIsBetter: false, current: bunchingCurrent, target: 10 },
]

const routes = [
  { otp_all_pct: 60, targets: { otp: 75 } },
  { otp_all_pct: 80, targets: { otp: 75 } },
]

function renderHero(props) {
  return render(
    <MemoryRouter>
      <OverviewHero
        systemMetrics={systemMetrics(9)}
        scorecardRoutes={routes}
        otpSeries={otpSeries}
        {...props}
      />
    </MemoryRouter>,
  )
}

describe('OverviewHero', () => {
  test('headline: weekly OTP with a signed week-over-week clause', () => {
    renderHero()
    expect(screen.getByText(/75% on time this week/i)).toBeVisible()
    expect(screen.getByText(/down 2\.0 pts/i)).toBeVisible()
  })

  test('subline counts routes below target', () => {
    renderHero()
    expect(screen.getByText(/1 of 2 routes below target/i)).toBeVisible()
  })

  test('sore-spot sentence appears only when the worst metric is not OTP', () => {
    renderHero({ systemMetrics: systemMetrics(14) }) // bunching 14 vs target 10 → worst
    expect(screen.getByText(/Bunching is the sore spot/i)).toBeVisible()
  })

  test('no sore-spot sentence when OTP itself is worst or nothing is off-target', () => {
    renderHero() // bunching 9 beats its target of 10
    expect(screen.queryByText(/sore spot/i)).not.toBeInTheDocument()
  })

  test('degrades to a neutral message when the OTP series is too thin', () => {
    renderHero({ otpSeries: [{ date: day(1), value: 75 }] })
    expect(screen.getByText(/not enough history yet/i)).toBeVisible()
  })
})
