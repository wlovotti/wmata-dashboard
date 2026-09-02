import useWindowDays, { WINDOW_DAY_OPTIONS } from '../hooks/useWindowDays'

/**
 * App-shell time-window control (NOTES-140). A 7/30-day segmented toggle
 * (90 was dropped in PR #239 review — see `useWindowDays.js`) that
 * reads/writes the shared `?days=` URL param via
 * `useWindowDays`. Rendered in the header next to `RefreshButton`, outside
 * the routed subtree, so switching windows never remounts the current page
 * — every page reads the same URL param itself and refetches when it
 * changes.
 *
 * @returns {JSX.Element}
 */
function WindowPicker() {
  const [days, setDays] = useWindowDays()

  return (
    <div className="window-picker" role="group" aria-label="Analysis window">
      {WINDOW_DAY_OPTIONS.map((option) => (
        <button
          key={option}
          type="button"
          className={option === days ? 'window-picker-btn active' : 'window-picker-btn'}
          aria-pressed={option === days}
          onClick={() => setDays(option)}
          title={`Show the last ${option} days`}
        >
          {option}d
        </button>
      ))}
    </div>
  )
}

export default WindowPicker
