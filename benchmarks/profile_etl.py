import argparse
import importlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent

# The ETL scripts use paths such as "DownloadLog.csv", so run from repo root.
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

DW_DATABASE = os.getenv("DW_DATABASE")
DB_USERNAME = os.getenv("USERNAME")

IMPLEMENTATIONS = {
    "csv": "cpygrametl1",
    "database": "cpygrametl1_db",
    "database_async_extract": "cpygrametl1_db_async_minimal",  # match your actual filename, no .py
}


def reset_warehouse():
    subprocess.run(
        [
            "psql",
            "-U",
            DB_USERNAME,
            "-d",
            DW_DATABASE,
            "-f",
            str(ROOT / "starschema.sql"),
        ],
        check=True,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
    )


def timed_function(
    function,
    timing_name,
    timings,
    timings_cpu,
):
    def wrapper(*args, **kwargs):
        wall_start = time.perf_counter()
        cpu_start = time.process_time()

        try:
            return function(*args, **kwargs)
        finally:
            timings[timing_name] += (
                time.perf_counter() - wall_start
            )
            timings_cpu[timing_name] += (
                time.process_time() - cpu_start
            )

    return wrapper


class TimedIterable:
    def __init__(self, source, timings, timings_cpu):
        self.source = source
        self.timings = timings
        self.timings_cpu = timings_cpu
        self.rows = 0

    def __iter__(self):
        iterator = iter(self.source)

        while True:
            wall_start = time.perf_counter()
            cpu_start = time.process_time()

            try:
                row = next(iterator)
            except StopIteration:
                self.timings["extraction_merge"] += (
                    time.perf_counter() - wall_start
                )
                self.timings_cpu["extraction_merge"] += (
                    time.process_time() - cpu_start
                )
                return

            self.timings["extraction_merge"] += (
                time.perf_counter() - wall_start
            )
            self.timings_cpu["extraction_merge"] += (
                time.process_time() - cpu_start
            )

            self.rows += 1

            yield row


class TimedProxy:
    def __init__(self, obj, methods, timings, timings_cpu):
        self._obj = obj

        for method_name, timing_name in methods.items():
            method = getattr(obj, method_name)

            setattr(
                self,
                method_name,
                timed_function(
                    method,
                    timing_name,
                    timings,
                    timings_cpu,
                ),
            )

    def __getattr__(self, name):
        return getattr(self._obj, name)


def profile(implementation):
    timings = {
        "initialisation": 0.0,
        "extraction_merge": 0.0,
        "transformation": 0.0,
        "page_dimension": 0.0,
        "date_dimension": 0.0,
        "test_dimension": 0.0,
        "fact_insert": 0.0,
        "commit": 0.0,
        "connection_close": 0.0,
    }

    timings_cpu = {
        name: 0.0 for name in timings
    }

    print(f"Resetting warehouse for {implementation}...")
    reset_warehouse()

    module_name = IMPLEMENTATIONS[implementation]

    print(f"Importing {module_name}...")

    # Importing the ETL module creates the database connections,
    # dimensions, fact table and data sources.
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    module = importlib.import_module(module_name)
    timings["initialisation"] = (
        time.perf_counter() - wall_start
    )
    timings_cpu["initialisation"] = (
        time.process_time() - cpu_start
    )

    # Measure extraction + MergeJoiningSource.
    #
    # The async extraction script has no module-level `inputdata` iterable
    # for TimedIterable to wrap -- extraction happens inside main() via a
    # single run_extraction() call, not a per-row generator. Wrap that
    # function directly instead, and read the row count back from the
    # module-level counter it sets rather than from TimedIterable.rows.
    if implementation == "database_async_extract":
        timed_input = None
        module.run_extraction = timed_function(
            module.run_extraction,
            "extraction_merge",
            timings,
            timings_cpu,
        )
    else:
        timed_input = TimedIterable(
            module.inputdata,
            timings,
            timings_cpu,
        )
        module.inputdata = timed_input

    # Measure the explicit transformation functions.
    module.extractdomaininfo = timed_function(
        module.extractdomaininfo,
        "transformation",
        timings,
        timings_cpu,
    )

    module.extractserverinfo = timed_function(
        module.extractserverinfo,
        "transformation",
        timings,
        timings_cpu,
    )

    # The ETL script calls pygrametl.getint directly.
    # Replacing only the module's pygrametl reference means internal
    # pygrametl code is not affected by this wrapper.
    module.pygrametl = TimedProxy(
        module.pygrametl,
        {
            "getint": "transformation",
        },
        timings,
        timings_cpu,
    )

    # Measure top-level pygrametl table operations.
    module.pagedim = TimedProxy(
        module.pagedim,
        {
            "scdensure": "page_dimension",
        },
        timings,
        timings_cpu,
    )

    module.datedim = TimedProxy(
        module.datedim,
        {
            "ensure": "date_dimension",
        },
        timings,
        timings_cpu,
    )

    module.testdim = TimedProxy(
        module.testdim,
        {
            "lookup": "test_dimension",
        },
        timings,
        timings_cpu,
    )

    module.facttbl = TimedProxy(
        module.facttbl,
        {
            "insert": "fact_insert",
        },
        timings,
        timings_cpu,
    )

    # Measure commit and closing of the target connection.
    module.connection = TimedProxy(
        module.connection,
        {
            "commit": "commit",
            "close": "connection_close",
        },
        timings,
        timings_cpu,
    )

    # The database-source implementation closes two long-lived source
    # connections explicitly in main(), as a separate step after all rows
    # are processed -- that's what this wrapping times.
    #
    # The async extraction script's source connections are opened AND
    # closed inside fetch_all(), entirely within the extraction step, for
    # each call. That overhead is already inside whatever run_extraction()
    # measures as "extraction_merge" above, not a separate "connection_close"
    # cost -- so this wrapping only applies to "database", not
    # "database_async_extract". This is a genuine structural difference
    # between the two scripts' phase breakdowns, not an oversight: don't
    # read "connection_close" as apples-to-apples between them.
    if implementation == "database":
        module.sourceconn1 = TimedProxy(
            module.sourceconn1,
            {
                "close": "connection_close",
            },
            timings,
            timings_cpu,
        )

        module.sourceconn2 = TimedProxy(
            module.sourceconn2,
            {
                "close": "connection_close",
            },
            timings,
            timings_cpu,
        )

    print(f"Profiling {implementation} ETL...")

    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    module.main()
    main_seconds = time.perf_counter() - wall_start
    main_cpu_seconds = time.process_time() - cpu_start

    # Wrapper installation itself is deliberately excluded.
    total_profiled_wall = (
        timings["initialisation"]
        + main_seconds
    )

    total_profiled_cpu = (
        timings_cpu["initialisation"]
        + main_cpu_seconds
    )

    measured = sum(timings.values())
    measured_cpu = sum(timings_cpu.values())

    timings["other"] = max(
        0.0,
        total_profiled_wall - measured,
    )

    timings_cpu["other"] = max(
        0.0,
        total_profiled_cpu - measured_cpu,
    )

    timings_waiting = {
        name: max(
            0.0,
            timings[name] - timings_cpu[name],
        )
        for name in timings
    }

    if timed_input is not None:
        rows = timed_input.rows
    else:
        rows = module._last_extraction_row_count

    result = {
        "implementation": implementation,
        "rows": rows,
        "total_profiled_wall_seconds": total_profiled_wall,
        "total_profiled_cpu_seconds": total_profiled_cpu,
        "timings": timings,
        "timings_cpu": timings_cpu,
        "timings_waiting": timings_waiting,
    }

    print()
    print("Phase profile")
    print("-" * 78)
    print(
        f"{'phase':22}"
        f"{'wall':>10} "
        f"{'wall%':>7} "
        f"{'cpu':>10} "
        f"{'waiting':>10} "
        f"{'cpu%':>7}"
    )
    print("-" * 78)

    for name, seconds in timings.items():
        percentage = (
            seconds / total_profiled_wall * 100
            if total_profiled_wall > 0
            else 0.0
        )

        cpu_seconds = timings_cpu[name]
        waiting_seconds = timings_waiting[name]

        cpu_percentage = (
            cpu_seconds / total_profiled_wall * 100
            if total_profiled_wall > 0
            else 0.0
        )

        print(
            f"{name:22}"
            f"{seconds:9.2f}s "
            f"{percentage:6.2f}% "
            f"{cpu_seconds:9.2f}s "
            f"{waiting_seconds:9.2f}s "
            f"{cpu_percentage:6.2f}%"
        )

    print("-" * 78)
    print(
        f"{'total':22}"
        f"{total_profiled_wall:9.2f}s "
        f"{100.00:6.2f}% "
        f"{total_profiled_cpu:9.2f}s "
        f"{max(0.0, total_profiled_wall - total_profiled_cpu):9.2f}s "
        f"{(total_profiled_cpu / total_profiled_wall * 100) if total_profiled_wall > 0 else 0.0:6.2f}%"
    )

    print()
    print(
        "PROFILE_RESULT="
        + json.dumps(
            result,
            sort_keys=True,
        )
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "implementation",
        choices=IMPLEMENTATIONS,
        help="ETL implementation to profile",
    )

    args = parser.parse_args()

    profile(args.implementation)


if __name__ == "__main__":
    main()