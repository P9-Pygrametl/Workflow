"""Run every ETL implementation with the same lifecycle and metrics.

Each run is a subprocess so CPython 3.14, free-threaded CPython 3.14t,
and Jython can be compared in one invocation. The warehouse is reset
before every run, and wall time, CPU time, peak memory (PSS on Linux,
RSS on macOS), and warehouse row counts are recorded in one SQLite table. 
Each run is checked for orphaned facts and, by default, its warehouse fingerprint
is compared with the sequential cpygrametl1_db run.

Example:
    .venv/bin/python -m run_all.main --page-sizes 1 --repeats 1
"""

import argparse
import os
import sys
import time
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datagenerator.datagenerator_db import generate
from run_all.config import DW_CONFIG, MODES, RESULTS_DB, SCHEMA_FILE, Mode
from run_all.measure import get_versions, run_child
from run_all.results import ResultsStore, RunResult
from run_all.validation import validate, warehouse_state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run every ETL implementation with unified metrics. "
            "Source data is generated once per page size, and the "
            "warehouse is reset before every run."
        )
    )
    parser.add_argument(
        "--page-sizes",
        nargs="+",
        type=int,
        default=[1],
        help="Generator page counts. Example: --page-sizes 1 10 100",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="Runs per implementation and page size.",
    )
    parser.add_argument(
        "--no-deep-validate",
        action="store_true",
        help=(
            "Skip warehouse content fingerprints. Structural checks "
            "(exit code, row counts, orphaned facts) still run."
        ),
    )
    args = parser.parse_args()

    invalid = [size for size in args.page_sizes if size < 1]
    if invalid:
        parser.error(f"page sizes must be positive integers, got {invalid!r}")
    if args.repeats < 1:
        parser.error("--repeats must be at least 1")

    return args


def run_mode(
    store: ResultsStore,
    mode: Mode,
    versions: tuple[str, str],
    pages: int,
    run_number: int,
    deep_validate: bool,
) -> RunResult:
    """Reset the warehouse, run one implementation, validate, and save."""
    with psycopg.connect(**DW_CONFIG) as connection:
        connection.execute(SCHEMA_FILE.read_text())

    measurement = run_child(mode.command, os.environ.copy())
    state = warehouse_state(deep_validate=deep_validate, config=DW_CONFIG)
    errors = validate(measurement, state, deep_validate)

    runtime_version, pygrametl_version = versions
    result = RunResult(
        timestamp=time.asctime(),
        implementation=mode.implementation,
        runtime=mode.runtime,
        pages=pages,
        run=run_number,
        wall_seconds=measurement.wall_seconds,
        cpu_seconds=measurement.cpu_seconds,
        cpu_percent=measurement.cpu_percent,
        peak_pss_mb=measurement.peak_pss_mb,
        peak_rss_mb=measurement.peak_rss_mb,
        page_rows=state["page"],
        test_rows=state["test"],
        date_rows=state["date"],
        fact_rows=state["testresults"],
        valid=0 if errors else 1,
        validation_error="; ".join(errors) or None,
        warehouse_signature=state["signature"],
        pygrametl_version=pygrametl_version,
        runtime_version=runtime_version,
        returncode=measurement.returncode,
        error=measurement.error,
    )
    store.save(result)

    print_outcome(result, errors)
    return result


def print_outcome(result: RunResult, errors: list[str]) -> None:
    if result.succeeded:
        pss = (
            f"{result.peak_pss_mb:.1f}MiB"
            if result.peak_pss_mb is not None
            else "N/A"
        )

        rss = (
            f"{result.peak_rss_mb:.1f}MiB"
            if result.peak_rss_mb is not None
            else "N/A"
        )

        print(
            f"  wall={result.wall_seconds:.2f}s "
            f"cpu={result.cpu_seconds:.2f}s "
            f"pss={pss} "
            f"rss={rss} "
            f"facts={result.fact_rows}"
        )
    else:
        print(f"  FAILED (return code {result.returncode})")
        if result.error:
            print(f"  {result.error.splitlines()[-1]}")

    for error in errors:
        print(f"  INVALID: {error}")


# --- Cross-implementation check ---------------------------------------


def apply_oracle_check(
    store: ResultsStore,
    results: list[RunResult],
    oracle: RunResult | None,
) -> None:
    """Require every completed run to match the designated oracle run."""
    completed = [r for r in results if r.succeeded]
    oracle_is_valid = (
        oracle is not None and oracle.valid and oracle.warehouse_signature is not None
    )
    if not oracle_is_valid:
        for result in completed:
            store.mark_invalid(
                result,
                "deep validation failed: oracle unavailable",
            )
        print("INVALID: oracle unavailable")
    else:
        for result in completed:
            if result is oracle:
                continue
            if result.warehouse_signature != oracle.warehouse_signature:
                store.mark_invalid(
                    result,
                    "warehouse fingerprint differs from oracle",
                )
                print(
                    f"  INVALID: {result.implementation} "
                    f"({result.runtime}): warehouse fingerprint "
                    f"differs from oracle"
                )

    store.commit()


# --- One workload size ------------------------------------------------


def run_page_size(
    store: ResultsStore,
    pages: int,
    repeats: int,
    deep_validate: bool,
) -> list[RunResult]:
    """Generate data for one page size and run every mode against it."""
    print(f"\n=== Workload size: {pages} pages ===")
    generate(pages)

    results = []
    oracle = None
    for mode in MODES:
        versions = get_versions(mode.interpreter)

        for run_number in range(1, repeats + 1):
            print(f"\n--- {mode.label} run {run_number}/{repeats} ---")
            result = run_mode(store, mode, versions, pages, run_number, deep_validate)
            results.append(result)
            if mode.oracle and oracle is None:
                oracle = result

    if deep_validate:
        apply_oracle_check(store, results, oracle)

    return results


def main() -> int:
    args = parse_args()
    deep_validate = not args.no_deep_validate

    all_results = []
    with ResultsStore() as store:
        for pages in args.page_sizes:
            all_results += run_page_size(store, pages, args.repeats, deep_validate)

    failed = any(not r.succeeded or not r.valid for r in all_results)

    print(f"\nResults written to: {RESULTS_DB.resolve()}")
    print(f"Status: {'FAILED' if failed else 'OK'}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
