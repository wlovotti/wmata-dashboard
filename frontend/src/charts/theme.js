/**
 * Shared recharts theming — the "one chart idiom" applied to every
 * recharts instance in the app (AgencyComparison, PeriodDrilldown,
 * RouteDiagnosisPanel, RouteTrend, RunDetail). Centralizing these props
 * means a chart's axis-tick style, grid stroke/dash, tooltip box, and
 * margins are one decision instead of five slightly-different
 * per-component ones (NOTES-85).
 *
 * Colors are exported as CSS custom-property references (e.g.
 * `'var(--color-brand)'`) rather than read at render time via
 * `getComputedStyle`. recharts emits them as SVG presentation attributes
 * (`stroke`, `fill`); `var()` resolution there is verified in Chromium
 * (the Playwright baseline browser) and works in current Firefox, but
 * WebKit has historically NOT resolved `var()` inside presentation
 * attributes, and the failure is silent (gridlines vanish, bars render
 * black). If Safari support ever matters, switch these to plain values
 * read once from `getComputedStyle(document.documentElement)` — do not
 * add a second hardcoded palette here. Under jsdom the string is passed
 * through untouched, so unit tests are unaffected either way.
 */

/** Default margin for a standalone chart with axis labels on all sides. */
export const CHART_MARGIN = { top: 8, right: 16, left: 0, bottom: 4 }

/** CartesianGrid props shared by every chart that shows gridlines. */
export const GRID_PROPS = {
  strokeDasharray: '3 3',
  stroke: 'var(--border-default)',
}

/** Tick label style for XAxis/YAxis. */
export const AXIS_TICK_STYLE = { fontSize: 12, fill: 'var(--text-secondary)' }

/**
 * Tick label style for compact charts (≤150px tall, e.g. the compare
 * page's distribution histograms), where the default 12px ticks crowd
 * the plot.
 */
export const AXIS_TICK_STYLE_COMPACT = { fontSize: 11, fill: 'var(--text-muted)' }

/** Axis line style, for charts that draw one explicitly. */
export const AXIS_LINE_PROPS = { stroke: 'var(--border-strong)' }

/** Hover-cursor fill behind a bar/column under the pointer. */
export const TOOLTIP_CURSOR_PROPS = { fill: 'var(--surface-subtle)' }

/**
 * className for every custom recharts `<Tooltip content={...}>` box, so
 * all chart tooltips share one background/border/shadow recipe (see
 * `.chart-tooltip` in App.css) instead of each chart defining its own.
 */
export const CHART_TOOLTIP_CLASS = 'chart-tooltip'

/** Compact tooltip className for the small charts that use AXIS_TICK_STYLE_COMPACT. */
export const CHART_TOOLTIP_COMPACT_CLASS = 'chart-tooltip chart-tooltip--compact'

/**
 * Semantic series colors, mirroring the six App.css color tokens, for
 * charts that color a series by status (on-target/behind/etc.) rather
 * than by an arbitrary categorical palette.
 */
export const SERIES_COLOR = {
  good: 'var(--color-good)',
  warn: 'var(--color-warn)',
  bad: 'var(--color-bad)',
  brand: 'var(--color-brand)',
  muted: 'var(--color-muted)',
  neutral: 'var(--color-neutral)',
}
