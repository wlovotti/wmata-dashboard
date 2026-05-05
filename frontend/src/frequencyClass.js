// WMATA route frequency-class palette and labels.
//
// Colors approximate the published WMATA system map legend (high=red,
// medium=navy, low=light blue, limited=purple, limited-stop=gray). The
// classification itself is GTFS-derived (P90 of weekday hour-mean headways
// against 12/20/30 min thresholds; "X" suffix → limited-stop) and so won't
// always align with WMATA's branded labels — those are operational policy.

export const FREQUENCY_CLASS_COLORS = {
  high: '#C8102E',
  medium: '#002F6C',
  low: '#5B9BD5',
  limited: '#7558BC',
  limited_stop: '#6B7280',
}

export const FREQUENCY_CLASS_LABELS = {
  high: 'High-Frequency (every 12 min or better)',
  medium: 'Medium-Frequency (every 20 min or better)',
  low: 'Low-Frequency (every 30 min or better)',
  limited: 'Limited-Frequency (30+ min)',
  limited_stop: 'Limited-Stop',
}

const DEFAULT_BADGE_COLOR = '#002F6C'
const NO_DATA_BADGE_COLOR = '#919D9D'

export function badgeColor(frequencyClass, hasMetrics) {
  if (frequencyClass && FREQUENCY_CLASS_COLORS[frequencyClass]) {
    return FREQUENCY_CLASS_COLORS[frequencyClass]
  }
  return hasMetrics ? DEFAULT_BADGE_COLOR : NO_DATA_BADGE_COLOR
}
