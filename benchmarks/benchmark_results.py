import sqlite3
import statistics

from benchmark_config import IMPLEMENTATIONS, PHASES, RESULTS_DB


BASE_COLUMNS = [
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


def add_profile_data(result, profile):
    profiled_wall = profile["total_profiled_wall_seconds"]
    result["profiled_wall_seconds"] = profiled_wall
    result["rows"] = profile["rows"]

    for phase in PHASES:
        seconds = profile["timings"].get(phase, 0.0)
        cpu_seconds = profile["timings_cpu"].get(phase, 0.0)
        waiting_seconds = profile["timings_waiting"].get(phase, 0.0)
        percentage = seconds / profiled_wall * 100 if profiled_wall else 0.0

        result[f"profile_{phase}_seconds"] = seconds
        result[f"profile_{phase}_percent"] = percentage
        result[f"profile_{phase}_cpu_seconds"] = cpu_seconds
        result[f"profile_{phase}_waiting_seconds"] = waiting_seconds


def result_columns():
    columns = list(BASE_COLUMNS)
    for phase in PHASES:
        columns.extend(
            [
                (f"profile_{phase}_seconds", "REAL"),
                (f"profile_{phase}_percent", "REAL"),
                (f"profile_{phase}_cpu_seconds", "REAL"),
                (f"profile_{phase}_waiting_seconds", "REAL"),
            ]
        )
    return columns


def save_results(results):
    columns = result_columns()
    column_names = [name for name, _ in columns]
    column_defs = ", ".join(f"{name} {type_}" for name, type_ in columns)
    placeholders = ", ".join("?" for _ in columns)
    RESULTS_DB.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(RESULTS_DB) as connection:
        connection.execute(
            f"CREATE TABLE IF NOT EXISTS results ({column_defs}) STRICT"
        )
        connection.executemany(
            f"INSERT INTO results ({', '.join(column_names)}) "
            f"VALUES ({placeholders})",
            [
                tuple(result.get(name) for name in column_names)
                for result in results
            ],
        )
    connection.close()


def print_summary(results):
    print("\nBenchmark summary")
    print("-" * 60)

    workload_sizes = sorted({result["workload_pages"] for result in results})
    for pages in workload_sizes:
        latencies = sorted(
            {
                result["source_rtt_ms"]
                for result in results
                if result["workload_pages"] == pages
            }
        )

        for source_rtt_ms in latencies:
            print(f"\nWorkload: pages={pages}, RTT latency={source_rtt_ms} ms")
            name, = IMPLEMENTATIONS
            matching = [
                result
                for result in results
                if result["workload_pages"] == pages
                and result["source_rtt_ms"] == source_rtt_ms
                and result["implementation"] == name
            ]
            print_benchmark_summary(name, matching)


def print_benchmark_summary(name, results):
    wall_times = [result["clean_wall_seconds"] for result in results]
    cpu_times = [result["python_cpu_seconds"] for result in results]
    waiting_times = [result["waiting_seconds"] for result in results]
    cpu_percentages = [result["cpu_percent"] for result in results]

    print(
        f"{name:10} median wall={statistics.median(wall_times):.2f}s | "
        f"median Python CPU={statistics.median(cpu_times):.2f}s | "
        f"median waiting={statistics.median(waiting_times):.2f}s"
    )
    print(
        f"{'':10} median CPU utilisation="
        f"{statistics.median(cpu_percentages):.2f}%"
    )
    print("  Median phase wall / CPU / waiting:")

    for phase in PHASES:
        percentages = [result[f"profile_{phase}_percent"] for result in results]
        cpu_seconds = [
            result[f"profile_{phase}_cpu_seconds"] for result in results
        ]
        waiting_seconds = [
            result[f"profile_{phase}_waiting_seconds"] for result in results
        ]
        print(
            f"    {phase:20} {statistics.median(percentages):6.2f}% "
            f"CPU={statistics.median(cpu_seconds):.2f}s "
            f"waiting={statistics.median(waiting_seconds):.2f}s"
        )
