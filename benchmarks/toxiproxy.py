import json
import os
import sys
import urllib.error
import urllib.request
from urllib.parse import urlparse
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent

load_dotenv(ROOT / ".env")

TOXIPROXY_API = os.getenv("TOXIPROXY_API")
TOXIPROXY_PROXY = os.getenv("TOXIPROXY_PROXY")

LATENCY_UP_TOXIC = "latency-up"
LATENCY_DOWN_TOXIC = "latency-down"
PROXY_LISTEN = "0.0.0.0:15432"
PROXY_UPSTREAM = "source-db:5432"


def toxiproxy_request(
    method,
    path,
    payload=None,
    ignore_not_found=False,
):
    """Send an HTTP request to the configured Toxiproxy API.

    The payload, when provided, is encoded as JSON. HTTP and connection
    errors are converted to RuntimeError, except for HTTP 404 responses
    when ignore_not_found is True.

    Args:
        method: HTTP method to use for the request.
        path: API path relative to TOXIPROXY_API.
        payload: Optional data to encode as the JSON request body.
        ignore_not_found: Whether HTTP 404 responses should be ignored.

    Returns:
        The response body as bytes, or None when an ignored 404 occurs.
    """
    url = f"{TOXIPROXY_API}{path}"

    data = None
    headers = {}

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=10
        ) as response:
            return response.read()

    except urllib.error.HTTPError as error:
        if (
            ignore_not_found
            and error.code == 404
        ):
            return None

        body = error.read().decode(
            "utf-8",
            errors="replace",
        )

        raise RuntimeError(
            f"Toxiproxy request failed: "
            f"{method} {url}\n"
            f"HTTP {error.code}: {body}"
        ) from error

    except urllib.error.URLError as error:
        raise RuntimeError(
            f"Could not connect to Toxiproxy "
            f"at {TOXIPROXY_API}: "
            f"{error.reason}"
        ) from error

def ensure_proxy():
    """Create the Toxiproxy proxy if it doesn't exist yet."""
    existing = toxiproxy_request(
        "GET",
        f"/proxies/{TOXIPROXY_PROXY}",
        ignore_not_found=True,
    )

    if existing is not None:
        upstream = json.loads(existing)["upstream"]
        if upstream != PROXY_UPSTREAM:
            raise RuntimeError(
                f"Proxy {TOXIPROXY_PROXY!r} already exists with upstream "
                f"{upstream!r}, expected {PROXY_UPSTREAM!r}."
            )
        return

    print(f"Creating Toxiproxy proxy {TOXIPROXY_PROXY!r}...")
    toxiproxy_request(
        "POST",
        "/proxies",
        {
            "name": TOXIPROXY_PROXY,
            "listen": PROXY_LISTEN,
            "upstream": PROXY_UPSTREAM,
        },
    )

def setup_toxiproxy_routing():
    """Dynamically configure the environment to route through Toxiproxy."""
    try:
        ensure_proxy()
        response = toxiproxy_request("GET", f"/proxies/{TOXIPROXY_PROXY}")
        data = json.loads(response)
        proxy_port = data.get("listen", "").split(":")[-1]
        
        # Extract the host dynamically from the API URL
        proxy_host = urlparse(TOXIPROXY_API).hostname or "127.0.0.1"
        
        print(f"Auto-routing through Toxiproxy on {proxy_host}:{proxy_port}")
        os.environ["SOURCE_PORT"] = proxy_port
        os.environ["SOURCE_HOST"] = proxy_host
    except Exception as e:
        sys.exit(
            f"Error configuring routing: {e}\n"
            "Ensure the Docker containers are running (docker compose up -d)."
        )

def reset_latency_toxics(strict=True):
    """Remove the benchmark latency toxics from Toxiproxy.

    Missing toxics are ignored. If Toxiproxy is not configured, no
    request is made.

    Args:
        strict: Whether errors while removing toxics should be raised.
            When False, errors are reported as warnings instead.

    Returns:
        True when the toxics were successfully reset, otherwise False.
    """
    if not (TOXIPROXY_API and TOXIPROXY_PROXY):
        return False

    try:
        for toxic in (
            LATENCY_UP_TOXIC,
            LATENCY_DOWN_TOXIC,
        ):
            toxiproxy_request(
                "DELETE",
                (
                    f"/proxies/{TOXIPROXY_PROXY}"
                    f"/toxics/{toxic}"
                ),
                ignore_not_found=True,
            )
    except (RuntimeError, OSError) as error:
        if strict:
            raise

        print(
            f"Warning: could not reset Toxiproxy latency toxics: {error}\n"
            f"Check manually: curl {TOXIPROXY_API}/proxies/"
            f"{TOXIPROXY_PROXY}/toxics",
            file=sys.stderr,
        )
        return False

    return True


def configure_toxiproxy(rtt_ms):
    """Configure Toxiproxy to simulate the requested round-trip latency.

    Existing benchmark latency toxics are removed before applying the
    new configuration. The requested round-trip time is split between
    the upstream and downstream directions. A value of zero leaves the
    proxy without latency toxics.

    Args:
        rtt_ms: Round-trip latency in milliseconds.
    """
    reset_latency_toxics(strict=True)

    if rtt_ms == 0:
        return

    upstream_latency = rtt_ms // 2
    downstream_latency = (
        rtt_ms - upstream_latency
    )

    toxiproxy_request(
        "POST",
        (
            f"/proxies/{TOXIPROXY_PROXY}"
            "/toxics"
        ),
        {
            "name": LATENCY_UP_TOXIC,
            "type": "latency",
            "stream": "upstream",
            "toxicity": 1.0,
            "attributes": {
                "latency": upstream_latency,
                "jitter": 0,
            },
        },
    )

    toxiproxy_request(
        "POST",
        (
            f"/proxies/{TOXIPROXY_PROXY}"
            "/toxics"
        ),
        {
            "name": LATENCY_DOWN_TOXIC,
            "type": "latency",
            "stream": "downstream",
            "toxicity": 1.0,
            "attributes": {
                "latency": downstream_latency,
                "jitter": 0,
            },
        },
    )


def prepare_latency(rtt_ms):
    """Prepare the source latency for a benchmark run.

    Zero latency can be used without Toxiproxy being configured. For a
    non-zero latency, the required Toxiproxy configuration is validated
    before the latency is applied.

    Args:
        rtt_ms: Source round-trip latency in milliseconds.
    """
    if rtt_ms == 0:
        if TOXIPROXY_API and TOXIPROXY_PROXY:
            configure_toxiproxy(0)
        return

    if not TOXIPROXY_API:
        raise RuntimeError(
            f"Cannot apply {rtt_ms} ms latency because "
            "TOXIPROXY_API is not configured in .env."
        )

    if not TOXIPROXY_PROXY:
        raise RuntimeError(
            f"Cannot apply {rtt_ms} ms latency because "
            "TOXIPROXY_PROXY is not configured in .env"
        )

    print(
        f"  Configuring Toxiproxy for "
        f"{rtt_ms} ms RTT..."
    )

    configure_toxiproxy(rtt_ms)