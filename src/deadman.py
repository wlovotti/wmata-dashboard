"""Dead-man's-switch ping for long-running daemons (NOTES-91).

The receiving service (healthchecks.io or similar) alerts when pings STOP
arriving. The caller must therefore invoke :func:`ping_healthcheck` only
when the work it guards actually succeeded — pinging unconditionally from
a wedged loop defeats the purpose (the 2026-07-17 collector wedge kept its
process alive for 9 hours while writing nothing).
"""

import requests


def ping_healthcheck(url: str | None, timeout: float = 5.0) -> bool:
    """POST a liveness ping to ``url``; never raises.

    Args:
        url: Healthcheck endpoint. ``None`` or empty disables pinging.
        timeout: Request timeout in seconds — kept short so a slow alerting
            service can never stall a collector tick.

    Returns:
        ``True`` if an HTTP exchange completed (any status counts — the
        alerting service registers receipt, not status), ``False`` when
        disabled or on any network failure.
    """
    if not url:
        return False
    try:
        requests.post(url, timeout=timeout)
        return True
    except requests.RequestException:
        return False
