import sqlite3
import os
import statistics
import psycopg
import sys
import time
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent

# Allow imports from the repository root when running this file directly.
sys.path.insert(0, str(ROOT))

from benchmarks.profile_etl import profile
from datagenerator.datagenerator_db import generate
from cpygrametl1_db import main as run_etl
from benchmarks.toxiproxy import (
    prepare_latency,
    reset_latency_toxics,
    setup_toxiproxy_routing,
)
from benchmarks.benchmark_args_cli import (
    parse_args,
    validate_page_sizes,
    validate_source_latency,
)

load_dotenv(ROOT / ".env")

DW_DATABASE = os.getenv("DW_DATABASE")
DW_HOST = os.getenv("DW_HOST", "localhost")
DW_PORT_RAW = os.getenv("DW_PORT", "5432")

try:
    DW_PORT = int(DW_PORT_RAW)
except ValueError as error:
    raise ValueError(
        f"DW_PORT must be an integer, got {DW_PORT_RAW!r}"
    ) from error

BENCHMARK_SCRIPT = "cpygrametl1_db.py"

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

RESULTS_DB = ROOT / "data" / "benchmark_results.db"

def reset_warehouse():
    """Reset the PostgreSQL data warehouse using the star schema."""
    schema = (ROOT / "starschema.sql").read_text()

    connection = psycopg.connect(
        host=DW_HOST,
        port=DW_PORT,
        dbname=DW_DATABASE,
        user=os.getenv("USERNAME"),
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(schema)

        connection.commit()
    finally:
        connection.close()


def run_benchmark(rtt_ms):
    """Run one unprofiled ETL benchmark.

    The requested source latency is prepared and the data warehouse is
    reset before execution. Wall-clock time and Python CPU time measure
    only the ETL execution itself.

    Waiting time is estimated as the difference between wall-clock and
    Python CPU time.

    Args:
        rtt_ms: Source round-trip latency in milliseconds.

    Returns:
        A dictionary containing the source latency, wall-clock time,
        Python CPU time, estimated waiting time, and CPU utilisation.
    """
    prepare_latency(rtt_ms)

    print("  Resetting warehouse...")
    reset_warehouse()

    cpu_start = time.process_time()
    wall_start = time.perf_counter()

    run_etl()

    wall_end = time.perf_counter()
    cpu_end = time.process_time()

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
        "source_rtt_ms": rtt_ms,
        "wall_seconds": wall_time,
        "python_cpu_seconds": cpu_time,
        "waiting_seconds": waiting_time,
        "cpu_percent": cpu_percent,
    }


def add_profile_data_to_result(result, profile_result):
    """Add phase profiling measurements to a benchmark result.

    The benchmark result is updated in place with the total profiled
    wall time and processed row count. For every profiling phase, wall
    time, percentage of total profiled wall time, CPU time, and
    estimated waiting time are added.

    Args:
        result: Benchmark result dictionary to update.
        profile_result: Profiling result containing total and per-phase
            timing measurements.
    """
    profiled_wall = profile_result[
        "total_profiled_wall_seconds"
    ]

    result["profiled_wall_seconds"] = (
        profiled_wall
    )

    result["rows"] = profile_result["rows"]

    timings = profile_result["timings"]
    timings_cpu = profile_result["timings_cpu"]
    timings_waiting = profile_result["timings_waiting"]

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
    """Store benchmark results in the SQLite results database.

    The results table is created when necessary. In addition to the
    general benchmark measurements, timing columns are generated for
    every phase listed in PHASES.

    Args:
        results: Benchmark result dictionaries to store.
    """
    columns = [
        ("workload_pages", "INTEGER"),
        ("run", "INTEGER"),
        ("source_rtt_ms", "INTEGER"),
        ("wall_seconds", "REAL"),
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
    column_defs = ", ".join(
        f"{name} {type_}"
        for name, type_ in columns
    )
    placeholders = ", ".join("?" for _ in columns)

    with sqlite3.connect(RESULTS_DB) as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS results ({column_defs}) STRICT"
        )
        conn.executemany(
            f"INSERT INTO results ({', '.join(column_names)}) "
            f"VALUES ({placeholders})",
            [
                tuple(result.get(name) for name in column_names)
                for result in results
            ],
        )


def print_summary(results):
    """Print median benchmark measurements grouped by workload and latency.

    Args:
        results: Benchmark result dictionaries to summarise.
    """
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

            matching = [
                result
                for result in results
                if result["workload_pages"] == pages
                and result["source_rtt_ms"] == source_rtt_ms
            ]

            wall_times = [
                result["wall_seconds"]
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
                f"median wall="
                f"{statistics.median(wall_times):.2f}s | "
                f"median Python CPU="
                f"{statistics.median(cpu_times):.2f}s | "
                f"median waiting="
                f"{statistics.median(waiting_times):.2f}s"
            )

            print(
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
    """Run all requested benchmark and profiling configurations."""
    args = parse_args()
    validate_page_sizes(args.page_sizes)
    validate_source_latency(args.source_rtt_latency)
    if args.source_rtt_latency_requested:
        setup_toxiproxy_routing()

    try:
        results_by_key = {}

        for pages in args.page_sizes:
            print(
                f"\n=== Workload size: {pages} pages ==="
            )

            reset_latency_toxics(strict=True)
            print(f"Generating source data for pages={pages}...")
            generate(pages)

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

                    result = run_benchmark(
                        source_rtt_ms
                    )

                    result["workload_pages"] = pages
                    result["run"] = run_number
                    result["source_rtt_ms"] = source_rtt_ms
                    result["timestamp"] = time.asctime()

                    results_by_key[
                        (pages, source_rtt_ms, run_number)
                    ] = result

                    print(
                        f"  Wall: "
                        f"{result['wall_seconds']:.2f}s"
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

                    prepare_latency(source_rtt_ms)

                    print("  Resetting warehouse...")
                    reset_warehouse()

                    profile_result = profile()

                    result = results_by_key[
                        (pages, source_rtt_ms, run_number)
                    ]

                    add_profile_data_to_result(
                        result,
                        profile_result,
                    )

                    print(
                        f"  Profiled wall: "
                        f"{profile_result['total_profiled_wall_seconds']:.2f}s"
                    )

        results = [
            results_by_key[key]
            for key in sorted(results_by_key)
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