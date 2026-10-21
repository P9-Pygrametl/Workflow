"""Measure Jython pygrametl runs across JVM heap and GC settings."""

import argparse
import csv
import itertools
import os
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from datagenerator.datagenerator_db import generate


DEFAULT_JYTHON = "/opt/jython/bin/jython"
DEFAULT_JDBC_JAR = "/usr/share/java/postgresql-jdbc/postgresql.jar"


def run_once(script, heap, gc, args):
    command = [
        args.jython,
        "-J-Xmx%s" % heap,
        "-J-cp",
        str(args.jdbc_jar),
    ]

    if gc == "g1":
        command.append("-J-XX:+UseG1GC")

    command.append(str(script))

    started_wall = time.perf_counter()
    started_usage = resource.getrusage(resource.RUSAGE_CHILDREN)

    try:
        process = subprocess.run(
            command,
            cwd=ROOT,
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=args.timeout,
        )
        returncode = process.returncode
        stderr = process.stderr
    except subprocess.TimeoutExpired as error:
        returncode = -1
        stderr = error.stderr or ""

    wall_seconds = time.perf_counter() - started_wall
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)

    return {
        "timestamp": time.asctime(),
        "pages": args.pages,
        "repeat": None,
        "heap": heap,
        "gc": gc,
        "returncode": returncode,
        "wall_seconds": wall_seconds,
        "user_seconds": usage.ru_utime - started_usage.ru_utime,
        "system_seconds": usage.ru_stime - started_usage.ru_stime,
        "stderr": stderr,
    }


def write_results(results, path):
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "timestamp",
        "pages",
        "repeat",
        "heap",
        "gc",
        "returncode",
        "wall_seconds",
        "user_seconds",
        "system_seconds",
    ]

    with path.open("w", newline="") as output:
        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(results)


def summarize(results):
    successful = [
        result for result in results
        if result["returncode"] == 0
    ]
    grouped = {}

    for result in successful:
        key = (result["heap"], result["gc"])
        grouped.setdefault(key, []).append(result)

    print("\nResults")
    print("-" * 72)
    print(
        "%-10s %-8s %8s %8s %10s %10s"
        % ("heap", "gc", "runs", "median", "min", "cpu")
    )
    print("-" * 72)

    rows = []

    for (heap, gc), runs in grouped.items():
        rows.append((
            statistics.median(r["wall_seconds"] for r in runs),
            heap,
            gc,
            len(runs),
            min(r["wall_seconds"] for r in runs),
            statistics.median(r["user_seconds"] for r in runs),
        ))

    for median, heap, gc, count, minimum, cpu in sorted(rows):
        print(
            "%-10s %-8s %8d %7.2fs %7.2fs %9.2fs"
            % (heap, gc, count, median, minimum, cpu)
        )

    failed = [r for r in results if r["returncode"] != 0]
    if failed:
        print("\nFailed runs: %d" % len(failed))


def main():
    parser = argparse.ArgumentParser(
        description="Measure Jython heap and GC configurations."
    )
    parser.add_argument("--pages", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument(
        "--script",
        type=Path,
        default=Path("jython/pygrametl2.py"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/jython_tuning.csv"),
    )
    parser.add_argument("--heaps", nargs="+", default=["4g", "6g"])
    parser.add_argument("--gcs", nargs="+", default=["default"])
    parser.add_argument("--jython", default=DEFAULT_JYTHON)
    parser.add_argument("--jdbc-jar", type=Path, default=Path(DEFAULT_JDBC_JAR))
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    script = (ROOT / args.script).resolve()
    output = (ROOT / args.output).resolve()

    configs = list(itertools.product(args.heaps, args.gcs))
    print(
        "Configurations: %d, repeats: %d, pages: %d"
        % (len(configs), args.repeats, args.pages)
    )

    if args.dry_run:
        for heap, gc in configs:
            print("%s %s" % (heap, gc))
        return

    if not args.skip_generate:
        print("Generating source data for pages=%d..." % args.pages)
        generate(args.pages)

    results = []
    total = len(configs) * args.repeats
    current = 0

    for heap, gc in configs:
        print("\nheap=%s gc=%s" % (heap, gc))

        for repeat in range(1, args.repeats + 1):
            current += 1
            print("  run %d/%d" % (current, total))
            result = run_once(script, heap, gc, args)
            result["repeat"] = repeat
            results.append(result)
            write_results(results, output)

            if result["returncode"] == 0:
                print(
                    "    wall=%.2fs user=%.2fs"
                    % (result["wall_seconds"], result["user_seconds"])
                )
            else:
                print("    FAILED with code %d" % result["returncode"])
                print(result["stderr"][-1000:])

    summarize(results)
    print("\nResults written to: %s" % output)


if __name__ == "__main__":
    main()
