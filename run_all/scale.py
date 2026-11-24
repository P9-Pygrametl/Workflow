"""Measure how performance changes with the number of available P-cores.

Each trial pins the whole process tree to physical P-cores with
``taskset``. The implementation and its tuned parameters stay fixed; only
the CPU affinity changes. The warehouse is reset and validated before each
run, so the results are directly comparable with the main benchmark.

Example:
    .venv/bin/python -m run_all.scale --pages 100 --repeats 10
"""

import argparse
import csv
import json
import os
import random
import shutil
import sqlite3
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datagenerator.datagenerator_db import generate
from run_all.config import (
    DW_CONFIG,
    MODES,
    ROOT,
    SCHEMA_FILE,
    SOURCE_CONFIG,
    TUNED_FILE,
)
from run_all.main import apply_profile, load_profile
from run_all.measure import run_child
from run_all.tuning import build_environment
from run_all.validation import validate, warehouse_state


SCALING_DB = ROOT / "data" / "scaling.db"
SCALING_CSV = ROOT / "data" / "scaling.csv"


@dataclass
class ScalingResult:
    timestamp: str
    implementation: str
    runtime: str
    cores: int
    run: int
    pages: int
    wall_seconds: float | None = None
    cpu_seconds: float | None = None
    cpu_percent: float | None = None
    peak_pss_mb: float | None = None
    peak_rss_mb: float | None = None
    fact_rows: int | None = None
    valid: int = 0
    validation_error: str | None = None
    warehouse_signature: str | None = None
    params: str | None = None
    profile: str | None = None
    returncode: int | None = None
    error: str | None = None
    id: int | None = None


SQL_TYPES = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "timestamp": "TEXT NOT NULL",
    "implementation": "TEXT NOT NULL",
    "runtime": "TEXT NOT NULL",
    "cores": "INTEGER NOT NULL",
    "run": "INTEGER NOT NULL",
    "pages": "INTEGER NOT NULL",
    "wall_seconds": "REAL",
    "cpu_seconds": "REAL",
    "cpu_percent": "REAL",
    "peak_pss_mb": "REAL",
    "peak_rss_mb": "REAL",
    "fact_rows": "INTEGER",
    "valid": "INTEGER NOT NULL",
    "validation_error": "TEXT",
    "warehouse_signature": "TEXT",
    "params": "TEXT",
    "profile": "TEXT",
    "returncode": "INTEGER",
    "error": "TEXT",
}


class ScalingStore:
    """SQLite storage for CPU-affinity scaling runs."""

    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        columns = ", ".join(f"{name} {sql_type}" for name, sql_type in SQL_TYPES.items())
        self.connection.execute(
            f"CREATE TABLE IF NOT EXISTS scaling_runs ({columns}) STRICT"
        )
        self.connection.commit()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.connection.close()

    def exists(self, implementation, runtime, cores, pages, run):
        row = self.connection.execute(
            """
            SELECT 1 FROM scaling_runs
            WHERE implementation = ? AND runtime = ? AND cores = ?
              AND pages = ? AND run = ?
            LIMIT 1
            """,
            (implementation, runtime, cores, pages, run),
        ).fetchone()
        return row is not None

    def save(self, result):
        names = list(SQL_TYPES)
        values = asdict(result)
        self.connection.execute(
            f"INSERT INTO scaling_runs ({', '.join(names)}) "
            f"VALUES ({', '.join('?' for _ in names)})",
            [values[name] for name in names],
        )
        self.connection.commit()

    def rows(self):
        return self.connection.execute(
            """
            SELECT implementation, runtime, cores, wall_seconds, valid
            FROM scaling_runs
            ORDER BY implementation, runtime, cores, run
            """
        ).fetchall()

    def export_csv(self, path):
        cursor = self.connection.execute(
            "SELECT * FROM scaling_runs ORDER BY id"
        )
        columns = [column[0] for column in cursor.description]
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as output:
            writer = csv.writer(output)
            writer.writerow(columns)
            writer.writerows(cursor)


def parse_cpu_list(spec):
    """Parse a taskset-style CPU list such as '0-3,8,10-11'."""
    cpus = []
    for token in spec.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            start, end = token.split("-", 1)
            cpus.extend(range(int(start), int(end) + 1))
        else:
            cpus.append(int(token))
    return cpus


def expected_fact_rows():
    with psycopg.connect(**SOURCE_CONFIG) as connection:
        return connection.execute("SELECT count(*) FROM testresults").fetchone()[0]


def reset_warehouse():
    with psycopg.connect(**DW_CONFIG) as connection:
        connection.execute(SCHEMA_FILE.read_text())


def execute(mode, cpu_list, pages, run, expected_facts, oracle_signature):
    """Run one affinity-constrained trial and return its measurement."""
    reset_warehouse()

    environment = build_environment(os.environ, mode.env)
    cpu_spec = ",".join(str(cpu) for cpu in cpu_list)
    command = ["taskset", "-c", cpu_spec] + mode.command
    measurement = run_child(command, environment)
    state = warehouse_state(deep_validate=True, config=DW_CONFIG)
    errors = validate(measurement, state, deep_validate=True)

    if state["testresults"] != expected_facts:
        errors.append(
            f"expected {expected_facts} fact rows, found {state['testresults']}"
        )

    if oracle_signature is not None and state["signature"] != oracle_signature:
        errors.append("warehouse fingerprint differs from oracle")

    return ScalingResult(
        timestamp=time.asctime(),
        implementation=mode.implementation,
        runtime=mode.runtime,
        cores=len(cpu_list),
        run=run,
        pages=pages,
        wall_seconds=measurement.wall_seconds,
        cpu_seconds=measurement.cpu_seconds,
        cpu_percent=measurement.cpu_percent,
        peak_pss_mb=measurement.peak_pss_mb,
        peak_rss_mb=measurement.peak_rss_mb,
        fact_rows=state["testresults"],
        valid=0 if errors else 1,
        validation_error="; ".join(errors) or None,
        warehouse_signature=state["signature"],
        params=json.dumps(dict(mode.params), sort_keys=True) if mode.params else None,
        profile=mode.profile,
        returncode=measurement.returncode,
        error=measurement.error,
    )


def print_outcome(result, cores):
    if result.returncode == 0 and result.valid:
        wall = (
            f"{result.wall_seconds:.2f}s"
            if result.wall_seconds is not None
            else "N/A"
        )
        cpu = (
            f"{result.cpu_seconds:.2f}s"
            if result.cpu_seconds is not None
            else "N/A"
        )
        pss = (
            f"{result.peak_pss_mb:.1f}MiB"
            if result.peak_pss_mb is not None
            else "N/A"
        )
        print(
            f"  cores={cores} wall={wall} cpu={cpu} pss={pss} "
            f"facts={result.fact_rows}"
        )
    else:
        print(f"  cores={cores} FAILED (return code {result.returncode})")
        if result.error:
            print(f"  {result.error.splitlines()[-1]}")
    if result.validation_error:
        print(f"  INVALID: {result.validation_error}")


def select_modes(names):
    if not names:
        return list(MODES)

    selected = []
    for name in names:
        matches = [
            mode
            for mode in MODES
            if mode.key == name
            or mode.label == name
            or mode.implementation == name
        ]
        if not matches:
            raise ValueError(f"No modes matched {name!r}")
        for mode in matches:
            if mode not in selected:
                selected.append(mode)
    return selected


def summarize(store):
    rows = store.rows()
    groups = {}
    for implementation, runtime, cores, wall, valid in rows:
        if valid and wall is not None:
            groups.setdefault((implementation, runtime, cores), []).append(wall)

    medians = {
        key: statistics.median(values)
        for key, values in groups.items()
        if values
    }

    print("\nMedians by mode and available P-cores")
    print("-" * 78)
    print(f"{'mode':42} {'cores':>5} {'n':>3} {'median':>9} {'speedup':>8}")
    print("-" * 78)

    for implementation, runtime in sorted(set((key[0], key[1]) for key in medians)):
        base = medians.get((implementation, runtime, 1))
        core_counts = sorted(
            {
                key[2]
                for key in medians
                if key[0] == implementation and key[1] == runtime
            }
        )
        for cores in core_counts:
            key = (implementation, runtime, cores)
            median = medians[key]
            speedup = f"{base / median:.2f}x" if base else "-"
            label = f"{implementation} ({runtime})"
            print(
                f"{label:42} {cores:5d} {len(groups[key]):3d} "
                f"{median:8.2f}s {speedup:>8}"
            )


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Measure wall time and resource use while limiting the process "
            "tree to 1, 2, 4, or 8 P-cores with taskset."
        )
    )
    parser.add_argument("--pages", type=int, default=100)
    parser.add_argument("--cores", nargs="+", type=int, default=[1, 2, 4, 8])
    parser.add_argument(
        "--p-cores",
        default="0-7",
        help=(
            "CPU IDs of the physical P-cores, in order. "
            "Example: 0-7 or 0,2,4,6,8,10,12,14."
        ),
    )
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument(
        "--modes",
        nargs="*",
        help="Optional mode keys, labels, or implementation names.",
    )
    parser.add_argument(
        "--tuned",
        type=Path,
        default=TUNED_FILE,
        help=f"Tuned parameter profile (default: {TUNED_FILE}).",
    )
    parser.add_argument(
        "--defaults",
        action="store_true",
        help="Ignore the tuned profile.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=SCALING_DB,
        help=f"Trial database (default: {SCALING_DB}).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=SCALING_CSV,
        help=f"CSV export (default: {SCALING_CSV}).",
    )
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Use the source data already in the database.",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete the scaling database before starting.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan without running anything.",
    )
    args = parser.parse_args()

    if args.pages < 1:
        parser.error("--pages must be positive")
    if args.repeats < 1:
        parser.error("--repeats must be at least 1")
    if any(cores < 1 for cores in args.cores):
        parser.error("--cores must be positive")
    return args


def main():
    args = parse_args()

    try:
        modes = select_modes(args.modes)
    except ValueError as error:
        print(error)
        return 1

    p_cores = parse_cpu_list(args.p_cores)
    if max(args.cores) > len(p_cores):
        print(
            f"Requested {max(args.cores)} cores, "
            f"but only {len(p_cores)} P-cores were given"
        )
        return 1

    if shutil.which("taskset") is None:
        print("taskset is required but was not found")
        return 1

    profile = {} if args.defaults else load_profile(args.tuned)
    profile_label = "defaults" if args.defaults or not profile else args.tuned.name

    print(f"P-cores: {p_cores}")
    print(f"Core counts: {args.cores}")
    print(f"Modes: {len(modes)}, repeats: {args.repeats}, pages: {args.pages}")
    print(f"Tuned profile: {'none' if not profile else args.tuned.resolve()}")

    if args.dry_run:
        total = len(modes) * len(args.cores) * args.repeats
        print(f"Total measured runs: {total} plus one oracle run")
        for mode in modes:
            cpu_lists = [
                ",".join(str(cpu) for cpu in p_cores[:cores])
                for cores in args.cores
            ]
            print(f"  {mode.key}: {', '.join(cpu_lists)}")
        return 0

    if args.fresh and args.db.exists():
        print(f"Removing {args.db}")
        args.db.unlink()

    if not args.skip_generate:
        print(f"Generating source data for pages={args.pages}...")
        generate(args.pages)

    expected_facts = expected_fact_rows()
    print(f"Expected fact rows: {expected_facts}")

    oracle_mode = next(mode for mode in MODES if mode.oracle)
    oracle_mode = apply_profile(
        oracle_mode,
        profile,
        f"tuned:{args.tuned.name}",
    )
    oracle_cpus = ",".join(str(cpu) for cpu in p_cores)
    print(f"\n=== oracle {oracle_mode.key} on {oracle_cpus} ===")
    oracle = execute(
        oracle_mode,
        p_cores,
        args.pages,
        0,
        expected_facts,
        None,
    )
    print_outcome(oracle, len(p_cores))
    if not oracle.valid or oracle.warehouse_signature is None:
        print("Oracle validation failed; aborting")
        return 1

    oracle_signature = oracle.warehouse_signature

    trials = [
        (mode, cores, run)
        for mode in modes
        for cores in args.cores
        for run in range(1, args.repeats + 1)
    ]
    random.Random(args.seed).shuffle(trials)

    with ScalingStore(args.db) as store:
        for mode, cores, run in trials:
            mode = apply_profile(mode, profile, f"tuned:{args.tuned.name}")
            if store.exists(
                mode.implementation,
                mode.runtime,
                cores,
                args.pages,
                run,
            ):
                print(f"\n--- {mode.label} cores={cores} run {run} reused ---")
                continue

            cpu_list = p_cores[:cores]
            cpu_spec = ",".join(str(cpu) for cpu in cpu_list)
            print(f"\n--- {mode.label} cores={cores} run {run} ---")
            print(f"  cpus={cpu_spec}")
            result = execute(
                mode,
                cpu_list,
                args.pages,
                run,
                expected_facts,
                oracle_signature,
            )
            store.save(result)
            print_outcome(result, cores)

        store.export_csv(args.output)
        summarize(store)

    print(f"\nTrial log: {args.db.resolve()}")
    print(f"CSV export: {args.output.resolve()}")
    print(f"Profile: {profile_label}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
