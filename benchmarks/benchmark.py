import argparse
import json
import sqlite3
import os
import resource
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent

load_dotenv(ROOT / ".env")

DW_DATABASE = os.getenv("DW_DATABASE")

TOXIPROXY_API = os.getenv("TOXIPROXY_API")
TOXIPROXY_PROXY = os.getenv("TOXIPROXY_PROXY")

ALL_IMPLEMENTATIONS = {
    "csv": "cpygrametl1.py",
    "database": "cpygrametl1_db.py",
}

BENCHMARK_IMPLEMENTATION = os.getenv(
    "BENCHMARK_IMPLEMENTATION",
    "both",
).lower()

IMPLEMENTATION_ALIASES = {
    "both": None,
    "csv": "csv",
    "db": "database",
    "database": "database",
}

if BENCHMARK_IMPLEMENTATION not in IMPLEMENTATION_ALIASES:
    raise ValueError(
        "BENCHMARK_IMPLEMENTATION must be one of: "
        "both, csv, db, database"
    )

selected_implementation = IMPLEMENTATION_ALIASES[
    BENCHMARK_IMPLEMENTATION
]

if selected_implementation is None:
    IMPLEMENTATIONS = ALL_IMPLEMENTATIONS
else:
    IMPLEMENTATIONS = {
        selected_implementation:
        ALL_IMPLEMENTATIONS[
            selected_implementation
        ]
    }


PHASES = [
    "initialisation",
    "extraction_merge",
    "transformation",
    "page_dimension",
    "date_dimension",
    "test_dimension",
    "fact_insert",
    "commit",
    "connection_close",
    "other",
]

REPEATS_RAW = os.getenv("REPEATS", "1")

try:
    REPEATS = int(REPEATS_RAW)
except ValueError as error:
    raise ValueError(
        "REPEATS must be an integer"
    ) from error

if REPEATS < 1:
    raise ValueError(
        "REPEATS must be at least 1"
    )

DEFAULT_PAGE_SIZES = [100]
DEFAULT_SOURCE_RTT_MS = [0]

RESULTS_DB = ROOT / "data" / "benchmark_results.db"
PROFILE_SCRIPT = ROOT / "benchmarks" / "profile_etl.py"

LATENCY_UP_TOXIC = "latency-up"
LATENCY_DOWN_TOXIC = "latency-down"


def toxiproxy_request(
    method,
    path,
    payload=None,
    ignore_not_found=False,
):
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
    

def reset_latency_toxics(strict=True):
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


def prepare_implementation(implementation, rtt_ms):
    if implementation != "database":
        return

    if rtt_ms == 0:
        if TOXIPROXY_API and TOXIPROXY_PROXY:
            configure_toxiproxy(0)
        return

    if not TOXIPROXY_API:
        raise RuntimeError(
            "Cannot apply {rtt_ms} ms latency because "
            "TOXIPROXY_API is not configured in .env."
        )

    if not TOXIPROXY_PROXY:
        raise RuntimeError(
            "Cannot apply {rtt_ms} ms latency because "
            "TOXIPROXY_PROXY is not configured in .env"
        )

    print(
        f"  Configuring Toxiproxy for "
        f"{rtt_ms} ms RTT..."
    )

    configure_toxiproxy(
        rtt_ms
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark the CSV and PostgreSQL ETL implementations "
            "across one or more workload sizes."
        )
    )

    parser.add_argument(
        "--page-sizes",
        nargs="+",
        dest="page_sizes",
        type=int,
        default=DEFAULT_PAGE_SIZES,
        help=(
            "Workload sizes expressed as generator page counts. "
            "Example: --page-sizes 10 25 50 100"
        ),
    )

    parser.add_argument(
        "--source-rtt-latency",
        nargs="+",
        dest="source_rtt_latency",
        type=int,
        default=DEFAULT_SOURCE_RTT_MS,
        help=(
            "Simulated source round-trip times in milliseconds. "
            "Example: --source-rtt-latency 0 50 100"
        ),
    )

    return parser.parse_args()


def validate_sizes(sizes):
    invalid_sizes = [size for size in sizes if size < 1]

    if invalid_sizes:
        raise ValueError(
            f"All sizes must be positive integers, got {invalid_sizes!r}"
        )


def validate_latency(latencies):
    invalid_latencies = [
        latency
        for latency in latencies
        if latency < 0
    ]

    if invalid_latencies:
        raise ValueError(
            "Latency values must be non-negative integers, "
            f"got {invalid_latencies!r}"
        )


def run_generator(script, pages):
    command = [
        sys.executable,
        str(ROOT / script),
        "--pages",
        str(pages),
    ]

    subprocess.run(
        command,
        check=True,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
    )


def generate_sources(pages):
    reset_latency_toxics(strict=True)
    print(f"Generating source data for pages={pages}...")

    run_generator(
        "datagenerator/datagenerator_db.py",
        pages,
    )


def reset_warehouse():
    subprocess.run(
        [
            "psql",
            DW_DATABASE,
            "-f",
            str(ROOT / "starschema.sql"),
        ],
        check=True,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
    )


def child_cpu_time():
    usage = resource.getrusage(
        resource.RUSAGE_CHILDREN
    )

    return usage.ru_utime + usage.ru_stime


def implementation_order(run_number):
    implementations = list(
        IMPLEMENTATIONS.items()
    )

    # Alternate order between runs to reduce systematic
    # ordering/cache effects when both implementations run.
    if (
        len(implementations) > 1
        and run_number % 2 == 0
    ):
        implementations.reverse()

    return implementations


def run_clean_benchmark(name, script, rtt_ms):
    prepare_implementation(name, rtt_ms)

    print(f"  Resetting warehouse for {name}...")
    reset_warehouse()

    cpu_start = child_cpu_time()
    wall_start = time.perf_counter()

    subprocess.run(
        [
            sys.executable,
            str(ROOT / script),
        ],
        check=True,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
    )

    wall_end = time.perf_counter()
    cpu_end = child_cpu_time()

    wall_time = wall_end - wall_start
    cpu_time = cpu_end - cpu_start

    waiting_time = max(
        0.0,
        wall_time - cpu_time,
    )

    cpu_percent = (
        cpu_time / wall_time * 100
        if wall_time > 0
        else 0.0
    )

    return {
        "implementation": name,
        "source_rtt_ms": rtt_ms,
        "clean_wall_seconds": wall_time,
        "python_cpu_seconds": cpu_time,
        "waiting_seconds": waiting_time,
        "cpu_percent": cpu_percent,
    }


def run_profile(implementation, rtt_ms):
    prepare_implementation(
        implementation,
        rtt_ms,
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(PROFILE_SCRIPT),
            implementation,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    if completed.returncode != 0:
        print(completed.stdout)
        print(
            completed.stderr,
            file=sys.stderr,
        )

        raise subprocess.CalledProcessError(
            completed.returncode,
            completed.args,
        )

    prefix = "PROFILE_RESULT="

    for line in completed.stdout.splitlines():
        if line.startswith(prefix):
            return json.loads(
                line[len(prefix):]
            )

    raise RuntimeError(
        "profile_etl.py did not produce "
        "a PROFILE_RESULT line"
    )


def add_profile_data(result, profile):
    profiled_wall = profile[
        "total_profiled_wall_seconds"
    ]

    result["profiled_wall_seconds"] = (
        profiled_wall
    )

    result["rows"] = profile["rows"]

    timings = profile["timings"]
    timings_cpu = profile["timings_cpu"]
    timings_waiting = profile["timings_waiting"]

    for phase in PHASES:
        seconds = timings.get(phase, 0.0)
        cpu_seconds = timings_cpu.get(phase, 0.0)
        waiting_seconds = timings_waiting.get(
            phase,
            0.0,
        )

        percentage = (
            seconds / profiled_wall * 100
            if profiled_wall > 0
            else 0.0
        )

        result[
            f"profile_{phase}_seconds"
        ] = seconds

        result[
            f"profile_{phase}_percent"
        ] = percentage

        result[
            f"profile_{phase}_cpu_seconds"
        ] = cpu_seconds

        result[
            f"profile_{phase}_waiting_seconds"
        ] = waiting_seconds


def save_results(results):
    columns = [
        ("workload_pages", "INTEGER"),
        ("implementation", "TEXT"),
        ("run", "INTEGER"),
        ("source_rtt_ms", "INTEGER"),
        ("clean_wall_seconds", "REAL"),
        ("python_cpu_seconds", "REAL"),
        ("waiting_seconds", "REAL"),
        ("cpu_percent", "REAL"),
        ("profiled_wall_seconds", "REAL"),
        ("rows", "INTEGER"),
        ("timestamp", "TEXT"),
    ]

    for phase in PHASES:
        columns.append((f"profile_{phase}_seconds", "REAL"))
        columns.append((f"profile_{phase}_percent", "REAL"))
        columns.append((f"profile_{phase}_cpu_seconds", "REAL"))
        columns.append((f"profile_{phase}_waiting_seconds", "REAL"))

    column_names = [name for name, _ in columns]
    column_defs = ", ".join(f"{name} {type_}" for name, type_ in columns)
    placeholders = ", ".join("?" for _ in columns)

    with sqlite3.connect(RESULTS_DB) as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS results ({column_defs}) STRICT")
        conn.executemany(
            f"INSERT INTO results ({', '.join(column_names)}) "
            f"VALUES ({placeholders})",
            [
                tuple(result.get(name) for name in column_names)
                for result in results
            ],
        )

    conn.close()


def print_summary(results):
    print("\nBenchmark summary")
    print("-" * 60)

    workload_sizes = sorted(
        {
            result["workload_pages"]
            for result in results
        }
    )

    for pages in workload_sizes:
        latencies = sorted(
            {
                result["source_rtt_ms"]
                for result in results
                if result["workload_pages"] == pages
            }
        )

        for source_rtt_ms in latencies:
            print(
                f"\nWorkload: pages={pages}, "
                f"RTT latency={source_rtt_ms} ms"
            )

            for name in IMPLEMENTATIONS:
                matching = [
                    result
                    for result in results
                    if result["workload_pages"] == pages
                    and result["source_rtt_ms"] == source_rtt_ms
                    and result["implementation"] == name
                ]

                wall_times = [
                    result["clean_wall_seconds"]
                    for result in matching
                ]

                cpu_times = [
                    result["python_cpu_seconds"]
                    for result in matching
                ]

                waiting_times = [
                    result["waiting_seconds"]
                    for result in matching
                ]

                cpu_percentages = [
                    result["cpu_percent"]
                    for result in matching
                ]

                print(
                    f"{name:10} "
                    f"median wall="
                    f"{statistics.median(wall_times):.2f}s | "
                    f"median Python CPU="
                    f"{statistics.median(cpu_times):.2f}s | "
                    f"median waiting="
                    f"{statistics.median(waiting_times):.2f}s"
                )

                print(
                    f"{'':10} "
                    f"median CPU utilisation="
                    f"{statistics.median(cpu_percentages):.2f}%"
                )

                print(
                    "  Median phase wall / CPU / waiting:"
                )

                for phase in PHASES:
                    percentages = [
                        result[
                            f"profile_{phase}_percent"
                        ]
                        for result in matching
                    ]

                    cpu_seconds = [
                        result[
                            f"profile_{phase}_cpu_seconds"
                        ]
                        for result in matching
                    ]

                    waiting_seconds = [
                        result[
                            f"profile_{phase}_waiting_seconds"
                        ]
                        for result in matching
                    ]

                    print(
                        f"    {phase:20} "
                        f"{statistics.median(percentages):6.2f}% "
                        f"CPU="
                        f"{statistics.median(cpu_seconds):.2f}s "
                        f"waiting="
                        f"{statistics.median(waiting_seconds):.2f}s"
                    )


def main():
    args = parse_args()
    validate_sizes(args.page_sizes)
    validate_latency(args.source_rtt_latency)
    try:
        results_by_key = {}

        for pages in args.page_sizes:
            print(
                f"\n=== Workload size: {pages} pages ==="
            )
    
            generate_sources(pages)
    
            for source_rtt_ms in args.source_rtt_latency:
                print(
                    f"\n=== Source RTT: {source_rtt_ms} ms ==="
                )
    
                print("=== Clean benchmark runs ===")
    
                # First run all clean benchmarks before profiling.
                # This prevents profiling instrumentation from affecting
                # the clean benchmark sequence.
                for run_number in range(
                    1,
                    REPEATS + 1,
                ):
                    print(
                        f"\nClean run "
                        f"{run_number}/{REPEATS}"
                    )
    
                    for name, script in implementation_order(
                        run_number
                    ):
                        print(f"Benchmarking {name}...")
                        result = run_clean_benchmark(
                            name,
                            script,
                            source_rtt_ms,
                        )
                        result["workload_pages"] = pages
                        result["run"] = run_number
                        result["source_rtt_ms"] = source_rtt_ms
                        result["timestamp"] = time.asctime()
                        results_by_key[
                            (pages, source_rtt_ms, run_number, name)
                        ] = result
    
                        print(
                            f"  Wall: "
                            f"{result['clean_wall_seconds']:.2f}s"
                        )
    
                        print(
                            f"  Python CPU: "
                            f"{result['python_cpu_seconds']:.2f}s"
                        )
    
                print("\n=== Phase profiling runs ===")
    
                # Profiling is deliberately done after all clean benchmarks
                # because the profiler adds overhead.
                for run_number in range(
                    1,
                    REPEATS + 1,
                ):
                    print(
                        f"\nProfile run "
                        f"{run_number}/{REPEATS}"
                    )
    
                    for name, _ in implementation_order(
                        run_number
                    ):
                        print(f"Profiling {name}...")
                        profile = run_profile(name, source_rtt_ms)
                        result = results_by_key[
                            (pages, source_rtt_ms, run_number, name)
                        ]
                        add_profile_data(result, profile)
    
                        print(
                            f"  Profiled wall: "
                            f"{profile['total_profiled_wall_seconds']:.2f}s"
                        )
    
        results = [
            results_by_key[key]
            for key in sorted(
                results_by_key,
                key=lambda value: (
                    value[0],
                    value[1],
                    value[2],
                    value[3],
                ),
            )
        ]
        save_results(results)
    finally:
        reset_latency_toxics(strict=False)
    print_summary(results)
    print(
        f"\nResults written to: "
        f"{RESULTS_DB.resolve()}"
    )


if __name__ == "__main__":
    main()
