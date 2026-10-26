import asyncio
import sqlite3
import os
import statistics
import psycopg
import sys
import time
import threading
from pathlib import Path

import psutil

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent

# Allow imports from the repository root when running this file directly.
sys.path.insert(0, str(ROOT))

from benchmarks.profile_etl import profile
from datagenerator.datagenerator_db import generate

from shared.cpygrametl1_db import main as run_sync_etl
from shared.cpygrametl1_async_db import main as run_async_etl
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

ETL_MODE = os.getenv("ETL_MODE", "sync").lower()

if ETL_MODE not in ("sync", "async"):
    raise ValueError("ETL_MODE must be 'sync' or 'async'")

# Record which SlowlyChangingDimension implementation the run uses.
SCD_MODE = os.getenv("SCD_MODE", "original").lower()
if SCD_MODE not in ("original", "optimized"):
    raise ValueError("SCD_MODE must be 'original' or 'optimized'")
if ETL_MODE == "sync" and SCD_MODE == "optimized":
    raise ValueError(
        "The synchronous ETL does not select PostgreSQLOptimizedSCD. "
        "Use ETL_MODE=async or SCD_MODE=original."
    )

# Keep the benchmark metadata aligned with AsyncSQLSource's configuration.
# Synchronous runs have no prefetch setting (stored as SQL NULL).
PREFETCH = None
if ETL_MODE == "async":
    prefetch_raw = os.getenv("PREFETCH", "0")
    try:
        PREFETCH = int(prefetch_raw)
    except ValueError as error:
        raise ValueError(
            f"PREFETCH must be a non-negative integer, got {prefetch_raw!r}"
        ) from error
    if PREFETCH < 0:
        raise ValueError("PREFETCH must be a non-negative integer")

BENCHMARK_SCRIPT = (
    "cpygrametl1_db.py"
    if ETL_MODE == "sync"
    else "cpygrametl1_async_db.py"
)


def run_selected_etl():
    if ETL_MODE == "sync":
        run_sync_etl()
    else:
        asyncio.run(run_async_etl())


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
RUN_PROFILING = os.getenv("RUN_PROFILING", "0") == "1"

MEMORY_INTERVAL = 0.1  # seconds


def sample_memory(stop_event, samples):
    """Sample this benchmark process's resident memory during an ETL run.

    The sequential ETL uses the same process, so process RSS is suitable here.
    Do not reuse this as a total-memory measure for multiprocessing workloads.
    """
    process = psutil.Process(os.getpid())
    while not stop_event.is_set():
        try:
            samples.append(process.memory_info().rss / (1024 * 1024))
        except psutil.Error:
            break
        stop_event.wait(MEMORY_INTERVAL)



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

    stop_event = threading.Event()
    rss_samples = []
    memory_monitor = threading.Thread(
        target=sample_memory, args=(stop_event, rss_samples), daemon=True
    )
    memory_monitor.start()

    cpu_start = time.process_time()
    wall_start = time.perf_counter()
    try:
        run_selected_etl()
    finally:
        wall_end = time.perf_counter()
        cpu_end = time.process_time()
        stop_event.set()
        memory_monitor.join()

    # Include a final sample after the workload ends.
    rss_samples.append(psutil.Process().memory_info().rss / (1024 * 1024))

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
        "avg_rss_mb": statistics.mean(rss_samples) if rss_samples else None,
        "peak_rss_mb": max(rss_samples) if rss_samples else None,
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
        ("etl_mode", "TEXT"),
        ("scd_mode", "TEXT"),
        ("prefetch", "INTEGER"),
        ("workload_pages", "INTEGER"),
        ("run", "INTEGER"),
        ("source_rtt_ms", "INTEGER"),
        ("wall_seconds", "REAL"),
        ("python_cpu_seconds", "REAL"),
        ("waiting_seconds", "REAL"),
        ("cpu_percent", "REAL"),
        ("avg_rss_mb", "REAL"),
        ("peak_rss_mb", "REAL"),
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

    RESULTS_DB.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(RESULTS_DB) as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS results ({column_defs}) STRICT"
        )
        # Preserve historical results from older versions of this benchmark.
        existing = {row[1] for row in conn.execute("PRAGMA table_info(results)")}
        for name, col_type in columns:
            if name not in existing:
                conn.execute(f"ALTER TABLE results ADD COLUMN {name} {col_type}")
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
                f"RTT latency={source_rtt_ms} ms, "
                f"mode={ETL_MODE}, "
                f"scd={SCD_MODE}, "
                f"prefetch={PREFETCH if PREFETCH is not None else 'N/A'}"
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
                f"median average RSS={statistics.median(r['avg_rss_mb'] for r in matching):.2f} MB | "
                f"median peak RSS={statistics.median(r['peak_rss_mb'] for r in matching):.2f} MB"
            )

            if not RUN_PROFILING:
                continue

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
                    print(
                        f"  Average RSS: {result['avg_rss_mb']:.2f} MB | "
                        f"Peak RSS: {result['peak_rss_mb']:.2f} MB"
                    )

                if not RUN_PROFILING:
                    continue

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

                    profile_result = profile(mode=ETL_MODE)

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

        for result in results:
            result["etl_mode"] = ETL_MODE
            result["scd_mode"] = SCD_MODE
            result["prefetch"] = PREFETCH
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