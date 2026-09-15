import csv
import json
import os
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent

load_dotenv(ROOT / ".env")

DW_DATABASE = os.getenv("DW_DATABASE")

IMPLEMENTATIONS = {
    "csv": "cpygrametl1.py",
    "database": "cpygrametl1_db.py",
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

REPEATS = 1

RESULTS_FILE = ROOT / "benchmarks" / "benchmark_results.csv"
PROFILE_SCRIPT = ROOT / "benchmarks" / "profile_etl.py"


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
    # ordering/cache effects.
    if run_number % 2 == 0:
        implementations.reverse()

    return implementations


def run_clean_benchmark(name, script):
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
        "clean_wall_seconds": wall_time,
        "python_cpu_seconds": cpu_time,
    }


def run_profile(implementation):
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
        print(completed.stderr, file=sys.stderr)

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
    fieldnames = [
        "implementation",
        "run",
        "clean_wall_seconds",
        "python_cpu_seconds",
        "profiled_wall_seconds",
        "rows",
    ]

    for phase in PHASES:
        fieldnames.extend(
            [
                f"profile_{phase}_seconds",
                f"profile_{phase}_percent",
            ]
        )

    with open(
        RESULTS_FILE,
        "w",
        newline="",
    ) as outfile:
        writer = csv.DictWriter(
            outfile,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(results)


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
        f"{RESULTS_FILE}"
    )


if __name__ == "__main__":
    main()