import argparse
import time

from benchmark_config import (
    DEFAULT_PAGE_SIZES,
    DEFAULT_SOURCE_RTT_MS,
    IMPLEMENTATIONS,
    REPEATS,
    RESULTS_DB,
    validate_latency,
    validate_sizes,
)
from benchmark_results import add_profile_data, print_summary, save_results
from benchmark_runtime import (
    generate_sources,
    implementation_order,
    reset_latency_toxics,
    run_clean_benchmark,
    run_profile,
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


def run_workload(pages, source_rtt_latency, results_by_key):
    print(f"\n=== Workload size: {pages} pages ===")
    generate_sources(pages)

    for source_rtt_ms in source_rtt_latency:
        print(f"\n=== Source RTT: {source_rtt_ms} ms ===")
        run_clean_benchmarks(pages, source_rtt_ms, results_by_key)
        run_profiled_benchmarks(pages, source_rtt_ms, results_by_key)


def run_clean_benchmarks(pages, source_rtt_ms, results_by_key):
    print("=== Clean benchmark runs ===")
    for run_number in range(1, REPEATS + 1):
        print(f"\nClean run {run_number}/{REPEATS}")
        for name, script in implementation_order(run_number):
            print(f"Benchmarking {name}...")
            result = run_clean_benchmark(name, script, source_rtt_ms)
            result.update(
                {
                    "workload_pages": pages,
                    "run": run_number,
                    "source_rtt_ms": source_rtt_ms,
                    "timestamp": time.asctime(),
                }
            )
            results_by_key[(pages, source_rtt_ms, run_number, name)] = result
            print(f"  Wall: {result['clean_wall_seconds']:.2f}s")
            print(f"  Python CPU: {result['python_cpu_seconds']:.2f}s")


def run_profiled_benchmarks(pages, source_rtt_ms, results_by_key):
    print("\n=== Phase profiling runs ===")
    for run_number in range(1, REPEATS + 1):
        print(f"\nProfile run {run_number}/{REPEATS}")
        for name, _ in implementation_order(run_number):
            print(f"Profiling {name}...")
            profile = run_profile(name, source_rtt_ms)
            result = results_by_key[(pages, source_rtt_ms, run_number, name)]
            add_profile_data(result, profile)
            print(
                f"  Profiled wall: "
                f"{profile['total_profiled_wall_seconds']:.2f}s"
            )


def ordered_results(results_by_key):
    return [
        results_by_key[key]
        for key in sorted(
            results_by_key,
            key=lambda value: (value[0], value[1], value[2], value[3]),
        )
    ]


def main():
    args = parse_args()
    validate_sizes(args.page_sizes)
    validate_latency(args.source_rtt_latency)
    results_by_key = {}

    try:
        for pages in args.page_sizes:
            run_workload(pages, args.source_rtt_latency, results_by_key)

        results = ordered_results(results_by_key)
        save_results(results)
    finally:
        reset_latency_toxics(strict=False)

    print_summary(results)
    print(f"\nResults written to: {RESULTS_DB.resolve()}")


if __name__ == "__main__":
    main()
