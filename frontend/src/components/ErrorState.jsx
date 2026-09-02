import './ErrorState.css'

/**
 * Shared inline error state for fetch-driven pages (NOTES-142).
 *
 * Renders a short message plus a "Retry" button that re-invokes the
 * caller-supplied `onRetry` callback. Callers own the actual retry
 * mechanics (typically bumping a state value that's part of a `useEffect`
 * dependency array so the failed fetch re-fires) — this component only
 * renders the message and wires up the click.
 *
 * @param {object} props
 * @param {string} [props.title] - Short label prefixed to the message
 *   (e.g. "Unable to load schedule audit"). Defaults to a generic label.
 * @param {string} [props.message] - The underlying error detail (HTTP
 *   status text, network error message, etc.). Rendered as-is.
 * @param {() => void} [props.onRetry] - Called when the user clicks
 *   "Retry". When omitted, no retry button is rendered.
 * @returns {JSX.Element}
 */
function ErrorState({ title = 'Unable to load data', message, onRetry }) {
  return (
    <div className="error-state" role="alert">
      <span className="error-state-icon" aria-hidden="true">
        ⚠️
      </span>
      <div className="error-state-body">
        <p className="error-state-message">
          <strong>{title}</strong>
          {message ? `: ${message}` : ''}
        </p>
        {onRetry && (
          <button
            type="button"
            className="error-state-retry"
            onClick={onRetry}
          >
            Retry
          </button>
        )}
      </div>
    </div>
  )
}

export default ErrorState
