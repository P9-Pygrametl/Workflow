import json
import resource
import subprocess
import sys
import time

from benchmark_config import PROFILE_SCRIPT, ROOT
from toxiproxy import apply_network_latency
from warehouse import reset_warehouse


def child_cpu_time():
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def run_clean_benchmark(name, script, rtt_ms):
    apply_network_latency(rtt_ms)
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
    apply_network_latency(rtt_ms)
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