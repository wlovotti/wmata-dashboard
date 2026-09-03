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
 * `getComputedStyle`. SVG presentation attributes (`stroke`, `fill`) are
 * styleable properties, so the browser resolves `var()` inside them
 * against the ancestor's computed style exactly as it would inside a
 * `style` attribute — no DOM read, no mount-time flash of an unstyled
 * color, and no behavior difference under jsdom in unit tests (the
 * string is just passed through as an SVG attribute either way). If the
 * app ever needs a color as a plain JS value (e.g. to compute a
 * gradient stop), read the matching constant from `App.css`'s `:root`
 * token block via `getComputedStyle(document.documentElement)` at that
 * call site rather than adding a second source of truth here.
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
