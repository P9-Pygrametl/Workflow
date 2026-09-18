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

SOURCE_RTT_RAW = os.getenv("SOURCE_RTT_MS")

if SOURCE_RTT_RAW is None:
    SOURCE_RTT_MS = None
else:
    try:
        SOURCE_RTT_MS = int(SOURCE_RTT_RAW)
    except ValueError as error:
        raise ValueError(
            "SOURCE_RTT_MS must be an integer"
        ) from error

    if SOURCE_RTT_MS < 0:
        raise ValueError(
            "SOURCE_RTT_MS cannot be negative"
        )


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

RESULTS_DB = ROOT / "benchmarks" / "benchmark_results.db"
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
            request
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


def remove_latency_toxics():
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


def configure_toxiproxy(rtt_ms):
    remove_latency_toxics()

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


def prepare_implementation(implementation):
    if implementation != "database":
        return

    if SOURCE_RTT_MS is None:
        return

    if not TOXIPROXY_API:
        raise RuntimeError(
            "SOURCE_RTT_MS was specified, but "
            "TOXIPROXY_API is not configured"
        )

    if not TOXIPROXY_PROXY:
        raise RuntimeError(
            "SOURCE_RTT_MS was specified, but "
            "TOXIPROXY_PROXY is not configured"
        )

    print(
        f"  Configuring Toxiproxy for "
        f"{SOURCE_RTT_MS} ms RTT..."
    )

    configure_toxiproxy(
        SOURCE_RTT_MS
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


def run_clean_benchmark(name, script):
    prepare_implementation(name)

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

    return {
        "implementation": name,
        "source_rtt_ms": (
            SOURCE_RTT_MS
            if name == "database"
            else None
        ),
        "clean_wall_seconds": wall_time,
        "python_cpu_seconds": cpu_time,
    }


def run_profile(implementation):
    prepare_implementation(
        implementation
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

    for phase in PHASES:
        seconds = timings.get(phase, 0.0)

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


def save_results(results):
    columns = [
        ("implementation", "TEXT"),
        ("run", "INTEGER"),
        ("source_rtt_ms", "REAL),
        ("clean_wall_seconds", "REAL"),
        ("python_cpu_seconds", "REAL"),
        ("profiled_wall_seconds", "REAL"),
        ("rows", "INTEGER"),
        ("timestamp", "TEXT"),
    ]

    for phase in PHASES:
        columns.append((f"profile_{phase}_seconds", "REAL"))
        columns.append((f"profile_{phase}_percent", "REAL"))

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
    
    for name in IMPLEMENTATIONS:
        matching = [
            result
            for result in results
            if result["implementation"] == name
        ]

        wall_times = [
            result["clean_wall_seconds"]
            for result in matching
        ]

        cpu_times = [
            result["python_cpu_seconds"]
            for result in matching
        ]

        print(
            f"{name:10} "
            f"median wall="
            f"{statistics.median(wall_times):.2f}s | "
            f"median Python CPU="
            f"{statistics.median(cpu_times):.2f}s"
        )

        if (
            name == "database"
            and SOURCE_RTT_MS is not None
        ):
            print(
                f"  Source RTT: "
                f"{SOURCE_RTT_MS} ms"
            )

        print("  Median phase percentages:")

        for phase in PHASES:
            percentages = [
                result[
                    f"profile_{phase}_percent"
                ]
                for result in matching
            ]

            median_percentage = (
                statistics.median(percentages)
            )

            print(
                f"    {phase:20} "
                f"{median_percentage:6.2f}%"
            )


def main():
    results_by_key = {}

    selected_names = ", ".join(
        IMPLEMENTATIONS
    )

    print(
        f"Benchmark implementation(s): "
        f"{selected_names}"
    )

    print("=== Clean benchmark runs ===")

    # First run all clean benchmarks.
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
            )

            result["run"] = run_number

            result["timestamp"] = time.asctime()

            results_by_key[
                (run_number, name)
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

    # Profiling is deliberately done after all clean
    # benchmarks because the profiler adds overhead.
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

            profile = run_profile(name)

            result = results_by_key[
                (run_number, name)
            ]

            add_profile_data(
                result,
                profile,
            )

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
            ),
        )
    ]

    save_results(results)
    print_summary(results)

    print(
        f"\nResults written to: "
        f"{RESULTS_DB.resolve()}"
    )


if __name__ == "__main__":
    main()