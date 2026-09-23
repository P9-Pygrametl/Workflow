import json
import resource
import subprocess
import sys
import time
import urllib.error
import urllib.request

from urllib.parse import urlparse

from benchmark_config import (
    DW_DATABASE,
    LATENCY_DOWN_TOXIC,
    LATENCY_UP_TOXIC,
    PROFILE_SCRIPT,
    ROOT,
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


def prepare_implementation(rtt_ms):
    if not (TOXIPROXY_API and TOXIPROXY_PROXY):
        raise RuntimeError(
            f"Cannot apply {rtt_ms} ms latency because DW_DATABASE is "
            "not currently connected through the Toxiproxy proxy."
        )
    if rtt_ms == 0:
        configure_toxiproxy(0)
        return

    print(f"  Configuring Toxiproxy for {rtt_ms} ms RTT...")
    configure_toxiproxy(rtt_ms)


def run_generator(script, pages):
    subprocess.run(
        [sys.executable, str(ROOT / script), "--pages", str(pages)],
        check=True,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
    )


def generate_sources(pages):
    reset_latency_toxics(strict=True)
    print(f"Generating source data for pages={pages}...")
    run_generator("datagenerator/datagenerator_db.py", pages)


def reset_warehouse():
    subprocess.run(
        ["psql", DW_DATABASE, "-f", str(ROOT / "starschema.sql")],
        check=True,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
    )


def child_cpu_time():
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def run_clean_benchmark(name, script, rtt_ms):
    prepare_implementation(rtt_ms)
    print(f"  Resetting warehouse for {name}...")
    reset_warehouse()

    cpu_start = child_cpu_time()
    wall_start = time.perf_counter()
    subprocess.run(
        [sys.executable, str(ROOT / script)],
        check=True,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
    )
    wall_time = time.perf_counter() - wall_start
    cpu_time = child_cpu_time() - cpu_start
    waiting_time = max(0.0, wall_time - cpu_time)

    return {
        "implementation": name,
        "source_rtt_ms": rtt_ms,
        "clean_wall_seconds": wall_time,
        "python_cpu_seconds": cpu_time,
        "waiting_seconds": waiting_time,
        "cpu_percent": cpu_time / wall_time * 100 if wall_time else 0.0,
    }


def run_profile(implementation, rtt_ms):
    prepare_implementation(rtt_ms)
    completed = subprocess.run(
        [sys.executable, str(PROFILE_SCRIPT), implementation],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    if completed.returncode != 0:
        print(completed.stdout)
        print(completed.stderr, file=sys.stderr)
        raise subprocess.CalledProcessError(
            completed.returncode,
            completed.args,
        )

    prefix = "PROFILE_RESULT="
    for line in completed.stdout.splitlines():
        if line.startswith(prefix):
            return json.loads(line[len(prefix):])

    raise RuntimeError("profile_etl.py did not produce a PROFILE_RESULT line")