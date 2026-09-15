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

IMPLEMENTATIONS = {
    "csv": "cpygrametl1",
    "database": "cpygrametl1_db",
}


def reset_warehouse():
    subprocess.run(
        [
            "psql",
            DW_DATABASE,
            "-f",
            str(ROOT / "starschema.sql"),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )


def timed_function(function, timing_name, timings):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()

        try:
            return function(*args, **kwargs)
        finally:
            timings[timing_name] += (
                time.perf_counter() - start
            )

    return wrapper


class TimedIterable:
    def __init__(self, source, timings):
        self.source = source
        self.timings = timings
        self.rows = 0

    def __iter__(self):
        iterator = iter(self.source)

        while True:
            start = time.perf_counter()

            try:
                row = next(iterator)
            except StopIteration:
                self.timings["extraction_merge"] += (
                    time.perf_counter() - start
                )
                return

            self.timings["extraction_merge"] += (
                time.perf_counter() - start
            )

            self.rows += 1

            yield row


class TimedProxy:
    def __init__(self, obj, methods, timings):
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

    print(f"Resetting warehouse for {implementation}...")
    reset_warehouse()

    module_name = IMPLEMENTATIONS[implementation]

    print(f"Importing {module_name}...")

    # Importing the ETL module creates the database connections,
    # dimensions, fact table and data sources.
    start = time.perf_counter()
    module = importlib.import_module(module_name)
    timings["initialisation"] = (
        time.perf_counter() - start
    )

    # Measure extraction + MergeJoiningSource.
    timed_input = TimedIterable(
        module.inputdata,
        timings,
    )
    module.inputdata = timed_input

    # Measure the explicit transformation functions.
    module.extractdomaininfo = timed_function(
        module.extractdomaininfo,
        "transformation",
        timings,
    )

    module.extractserverinfo = timed_function(
        module.extractserverinfo,
        "transformation",
        timings,
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
    )

    # Measure top-level pygrametl table operations.
    module.pagedim = TimedProxy(
        module.pagedim,
        {
            "scdensure": "page_dimension",
        },
        timings,
    )

    module.datedim = TimedProxy(
        module.datedim,
        {
            "ensure": "date_dimension",
        },
        timings,
    )

    module.testdim = TimedProxy(
        module.testdim,
        {
            "lookup": "test_dimension",
        },
        timings,
    )

    module.facttbl = TimedProxy(
        module.facttbl,
        {
            "insert": "fact_insert",
        },
        timings,
    )

    # Measure commit and closing of the target connection.
    module.connection = TimedProxy(
        module.connection,
        {
            "commit": "commit",
            "close": "connection_close",
        },
        timings,
    )

    # The database-source implementation also closes two source
    # connections in main().
    if implementation == "database":
        module.sourceconn1 = TimedProxy(
            module.sourceconn1,
            {
                "close": "connection_close",
            },
            timings,
        )

        module.sourceconn2 = TimedProxy(
            module.sourceconn2,
            {
                "close": "connection_close",
            },
            timings,
        )

    print(f"Profiling {implementation} ETL...")

    start = time.perf_counter()
    module.main()
    main_seconds = time.perf_counter() - start

    # Wrapper installation itself is deliberately excluded.
    total_profiled_wall = (
        timings["initialisation"]
        + main_seconds
    )

    measured = sum(timings.values())

    timings["other"] = max(
        0.0,
        total_profiled_wall - measured,
    )

    result = {
        "implementation": implementation,
        "rows": timed_input.rows,
        "total_profiled_wall_seconds": total_profiled_wall,
        "timings": timings,
    }

    print()
    print("Phase profile")
    print("-" * 55)

    for name, seconds in timings.items():
        percentage = (
            seconds / total_profiled_wall * 100
            if total_profiled_wall > 0
            else 0.0
        )

        print(
            f"{name:22}"
            f"{seconds:10.2f}s "
            f"{percentage:7.2f}%"
        )

    print("-" * 55)
    print(
        f"{'total':22}"
        f"{total_profiled_wall:10.2f}s "
        f"{100.00:7.2f}%"
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