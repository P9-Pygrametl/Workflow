import json
import sys
import urllib.error
import urllib.request

from benchmark_config import (
    LATENCY_DOWN_TOXIC,
    LATENCY_UP_TOXIC,
    TOXIPROXY_API,
    TOXIPROXY_PROXY,
)


def toxiproxy_request(method, path, payload=None, ignore_not_found=False):
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
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        if ignore_not_found and error.code == 404:
            return None

        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Toxiproxy request failed: {method} {url}\n"
            f"HTTP {error.code}: {body}"
        ) from error
    except urllib.error.URLError as error:
        raise RuntimeError(
            f"Could not connect to Toxiproxy at "
            f"{TOXIPROXY_API}: {error.reason}"
        ) from error


def reset_latency_toxics(strict=True):
    if not (TOXIPROXY_API and TOXIPROXY_PROXY):
        return False

    try:
        for toxic in (LATENCY_UP_TOXIC, LATENCY_DOWN_TOXIC):
            toxiproxy_request(
                "DELETE",
                f"/proxies/{TOXIPROXY_PROXY}/toxics/{toxic}",
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
    reset_latency_toxics(strict=True)

    if rtt_ms == 0:
        return

    upstream_latency = rtt_ms // 2
    downstream_latency = rtt_ms - upstream_latency

    for name, stream, latency in (
        (LATENCY_UP_TOXIC, "upstream", upstream_latency),
        (LATENCY_DOWN_TOXIC, "downstream", downstream_latency),
    ):
        toxiproxy_request(
            "POST",
            f"/proxies/{TOXIPROXY_PROXY}/toxics",
            {
                "name": name,
                "type": "latency",
                "stream": stream,
                "toxicity": 1.0,
                "attributes": {"latency": latency, "jitter": 0},
            },
        )


def apply_network_latency(rtt_ms):
    if not TOXIPROXY_API:
        raise RuntimeError(
            f"Cannot apply {rtt_ms} ms latency because TOXIPROXY_API is not set."
        )
    if not TOXIPROXY_PROXY:
        raise RuntimeError(
            f"Cannot apply {rtt_ms} ms latency because TOXIPROXY_PROXY is not set."
        )
    if rtt_ms == 0:
        configure_toxiproxy(0)
        return

    print(f"  Configuring Toxiproxy for {rtt_ms} ms RTT...")
    configure_toxiproxy(rtt_ms)