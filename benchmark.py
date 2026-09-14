#!/usr/bin/env python3

"""
Benchmark wallclock, CPU, disk I/O, and PostgreSQL I/O for a command,
including per-phase PostgreSQL I/O if the command reports its own internal
phases.
"""

import argparse
import csv
import os
import subprocess
import sys
import threading
import time

import psutil

try:
    import psycopg2
    HAVE_PSYCOPG2 = True
except ImportError:
    HAVE_PSYCOPG2 = False


def _pg_stat_io_has_byte_columns(cursor):
    cursor.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'pg_stat_io' AND column_name = 'read_bytes'
        """
    )
    return cursor.fetchone() is not None


def get_postgres_io_snapshot(dsn):
    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SHOW track_io_timing")
            track_io_timing = cursor.fetchone()[0] == "on"

            if _pg_stat_io_has_byte_columns(cursor):
                cursor.execute(
                    """
                    SELECT reads, read_time, read_bytes,
                           writes, write_time, write_bytes
                    FROM pg_stat_io
                    WHERE reads IS NOT NULL OR writes IS NOT NULL
                    """
                )
                rows = cursor.fetchall()
                reads = writes = 0
                read_bytes = write_bytes = 0
                read_time_ms = write_time_ms = 0.0
                for r, rt, rb, w, wt, wb in rows:
                    if r:
                        reads += r
                        read_bytes += rb or 0
                        read_time_ms += rt or 0.0
                    if w:
                        writes += w
                        write_bytes += wb or 0
                        write_time_ms += wt or 0.0
            else:
                cursor.execute(
                    """
                    SELECT reads, read_time, writes, write_time, op_bytes
                    FROM pg_stat_io
                    WHERE reads IS NOT NULL OR writes IS NOT NULL
                    """
                )
                rows = cursor.fetchall()
                reads = writes = 0
                read_bytes = write_bytes = 0
                read_time_ms = write_time_ms = 0.0
                for r, rt, w, wt, op_bytes in rows:
                    op_bytes = op_bytes or 0
                    if r:
                        reads += r
                        read_bytes += r * op_bytes
                        read_time_ms += rt or 0.0
                    if w:
                        writes += w
                        write_bytes += w * op_bytes
                        write_time_ms += wt or 0.0
    finally:
        conn.close()

    return {
        "reads": reads,
        "read_bytes": read_bytes,
        "read_time_ms": read_time_ms,
        "writes": writes,
        "write_bytes": write_bytes,
        "write_time_ms": write_time_ms,
        "track_io_timing": track_io_timing,
    }


def calculate_postgres_io_difference(before, after):
    return {
        "reads": after["reads"] - before["reads"],
        "read_bytes": after["read_bytes"] - before["read_bytes"],
        "read_time_s": (after["read_time_ms"] - before["read_time_ms"]) / 1000.0,
        "writes": after["writes"] - before["writes"],
        "write_bytes": after["write_bytes"] - before["write_bytes"],
        "write_time_s": (after["write_time_ms"] - before["write_time_ms"]) / 1000.0,
        "track_io_timing": after["track_io_timing"],
    }



IO_COUNTERS_UNSUPPORTED = (
    psutil.AccessDenied,
    psutil.NoSuchProcess,
    AttributeError,
    NotImplementedError,
)


# Phase tracking
PHASE_START_PREFIX = "PHASE_START,"
PHASE_END_PREFIX = "PHASE_END,"


class PhaseTracker:
    def __init__(self, pg_dsn):
        self.pg_dsn = pg_dsn
        self._open = {}
        self._completed = []
        self._lock = threading.Lock()

    def handle_line(self, line):
        if line.startswith(PHASE_START_PREFIX):
            name = line[len(PHASE_START_PREFIX):].strip()
            if name:
                self._snapshot_start(name)
        elif line.startswith(PHASE_END_PREFIX):
            rest = line[len(PHASE_END_PREFIX):].split(",", 1)
            name = rest[0].strip()
            rows = None
            if len(rest) > 1 and rest[1].strip():
                try:
                    rows = int(rest[1].strip())
                except ValueError:
                    rows = None
            if name:
                self._snapshot_end(name, rows)

    def _postgres_snapshot(self):
        if not self.pg_dsn:
            return None
        try:
            return get_postgres_io_snapshot(self.pg_dsn)
        except Exception as e:
            print(f"Warning: could not snapshot PostgreSQL I/O at a phase boundary: {e}", file=sys.stderr)
            return None

    def _snapshot_start(self, name):
        postgres_snap_start = self._postgres_snapshot()
        wall = time.perf_counter()
        with self._lock:
            self._open[name] = (wall, postgres_snap_start)

    def _snapshot_end(self, name, rows):
        wall_end = time.perf_counter()
        postgres_snap_end = self._postgres_snapshot()
        with self._lock:
            start = self._open.pop(name, None)
        if start is None:
            print(f"Warning: PHASE_END for '{name}' with no matching PHASE_START; ignoring.", file=sys.stderr)
            return
        wall_start, postgres_snap_start = start
        entry = {
            "phase": name,
            "wallclock_s": wall_end - wall_start,
            "rows": rows,
            "pg": None,
        }
        if postgres_snap_start is not None and postgres_snap_end is not None:
            entry["pg"] = calculate_postgres_io_difference(postgres_snap_start, postgres_snap_end)
        with self._lock:
            self._completed.append(entry)

    def results(self):
        with self._lock:
            return list(self._completed)


# Running benchmarking subprocesses and collecting stats
def run_once(cmd, interval=0.1, pg_dsn=None):
    postgres_before = None
    if pg_dsn:
        try:
            postgres_before = get_postgres_io_snapshot(pg_dsn)
        except Exception as e:
            print(f"Warning: could not read PostgreSQL I/O stats: {e}", file=sys.stderr)
            pg_dsn = None  # stop trying for the rest of this run

    tracker = PhaseTracker(pg_dsn)

    start_wall = time.perf_counter()

    process = psutil.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    def reader():
        try:
            for line in process.stdout:
                line = line.rstrip("\n")
                sys.stdout.write(line + "\n")
                if line.startswith(PHASE_START_PREFIX) or line.startswith(PHASE_END_PREFIX):
                    tracker.handle_line(line)
        except Exception as e:
            print(f"Warning: error reading child output: {e}", file=sys.stderr)

    reader_thread = threading.Thread(target=reader, daemon=True)
    reader_thread.start()

    cpu_percents = []
    io_start = None
    io_end = None

    try:
        io_start = process.io_counters()
    except IO_COUNTERS_UNSUPPORTED:
        io_start = None

    try:
        process.cpu_percent(interval=None)
    except psutil.NoSuchProcess:
        pass

    last_io = io_start


    while True:
        try:
            last_io = process.io_counters()
        except IO_COUNTERS_UNSUPPORTED:
            pass
        try:
            cpu_percents.append(process.cpu_percent(interval=None))
        except psutil.NoSuchProcess:
            pass
        if process.poll() is not None:
            break
        time.sleep(interval)

    reader_thread.join()

    io_end = last_io
    returncode = process.returncode
    end_wall = time.perf_counter()

    postgres_difference = None
    if pg_dsn:
        try:
            postgres_after = get_postgres_io_snapshot(pg_dsn)
            postgres_difference = calculate_postgres_io_difference(postgres_before, postgres_after)
        except Exception as e:
            print(f"Warning: could not read PostgreSQL I/O stats: {e}", file=sys.stderr)
            postgres_difference = None

    wallclock = end_wall - start_wall
    avg_cpu = sum(cpu_percents) / len(cpu_percents) if cpu_percents else 0.0
    max_cpu = max(cpu_percents) if cpu_percents else 0.0

    result = {
        "returncode": returncode,
        "runtime_bin": os.path.basename(cmd[0]) if cmd else "",
        "wallclock_s": wallclock,
        "avg_cpu_percent": avg_cpu,
        "max_cpu_percent": max_cpu,
    }

    if io_start is not None and io_end is not None:
        result["read_count"] = io_end.read_count - io_start.read_count
        result["write_count"] = io_end.write_count - io_start.write_count
        result["read_bytes"] = io_end.read_bytes - io_start.read_bytes
        result["write_bytes"] = io_end.write_bytes - io_start.write_bytes
    else:
        result["io_unavailable"] = True

    if postgres_difference is not None:
        result["pg"] = postgres_difference

    phases = tracker.results()
    if phases:
        result["phases"] = phases

    return result


# CSV output
FIELDS = [
    "run", "runtime", "returncode", "scope", "phase",
    "wallclock_s", "rows",
    "cpu_avg_percent", "cpu_max_percent",
    "py_read_count", "py_write_count", "py_read_bytes", "py_write_bytes",
    "pg_reads", "pg_read_bytes", "pg_read_time_s",
    "pg_writes", "pg_write_bytes", "pg_write_time_s",
]


def _postgres_columns(pg):
    if pg is None:
        return {}
    return {
        "pg_reads": pg["reads"],
        "pg_read_bytes": pg["read_bytes"],
        "pg_read_time_s": f"{pg['read_time_s']:.6f}",
        "pg_writes": pg["writes"],
        "pg_write_bytes": pg["write_bytes"],
        "pg_write_time_s": f"{pg['write_time_s']:.6f}",
    }


def write_csv(runs_results, csv_path):
    out_rows = []

    for i, run in enumerate(runs_results, 1):
        base = {
            "run": i,
            "runtime": run.get("runtime_bin", ""),
            "returncode": run["returncode"],
        }

        run_row = dict(base)
        run_row.update({
            "scope": "run",
            "phase": "",
            "wallclock_s": f"{run['wallclock_s']:.6f}",
            "cpu_avg_percent": f"{run['avg_cpu_percent']:.2f}",
            "cpu_max_percent": f"{run['max_cpu_percent']:.2f}",
        })
        if not run.get("io_unavailable"):
            run_row.update({
                "py_read_count": run["read_count"],
                "py_write_count": run["write_count"],
                "py_read_bytes": run["read_bytes"],
                "py_write_bytes": run["write_bytes"],
            })
        run_row.update(_postgres_columns(run.get("pg")))
        out_rows.append(run_row)

        for p in run.get("phases", []):
            phase_row = dict(base)
            phase_row.update({
                "scope": "phase",
                "phase": p["phase"],
                "wallclock_s": f"{p['wallclock_s']:.6f}",
                "rows": p["rows"] if p["rows"] is not None else "",
            })
            phase_row.update(_postgres_columns(p.get("pg")))
            out_rows.append(phase_row)

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, restval="")
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Results written to {csv_path}", file=sys.stderr)

    pg_runs = [r["pg"] for r in runs_results if r.get("pg") is not None]
    if pg_runs and not pg_runs[0]["track_io_timing"]:
        print(
            "Note: track_io_timing is OFF on this PostgreSQL server, so "
            "pg_read_time_s / pg_write_time_s are always 0.000000 (not "
            "measured, not instantaneous). Enable it with:\n"
            "  ALTER SYSTEM SET track_io_timing = on; SELECT pg_reload_conf();",
            file=sys.stderr,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark wallclock, CPU, disk I/O, and PostgreSQL I/O "
                     "(overall and per-phase) for a command, written to CSV."
    )
    parser.add_argument("--runs", type=int, default=1, help="Number of times to run the command")
    parser.add_argument("--interval", type=float, default=0.1, help="Sampling interval in seconds")
    parser.add_argument(
        "--pg-dsn",
        default=None,
        help=(
            "PostgreSQL connection string (e.g. 'dbname=... user=... host=...') "
            "to also track PostgreSQL I/O via pg_stat_io, overall and per phase. "
            "Requires Postgres 16+; set track_io_timing = on for read/write "
            "time to be non-zero."
        ),
    )
    parser.add_argument(
        "--csv",
        default="benchmark_results.csv",
        help="CSV path to write results to (default: benchmark_results.csv)",
    )
    parser.add_argument("cmd", nargs=argparse.REMAINDER, help="Command to run, prefixed with --")
    args = parser.parse_args()

    cmd = args.cmd
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]

    if not cmd:
        parser.error("No command given. Usage: python benchmark.py -- <command> [args...]")

    pg_dsn = args.pg_dsn
    if pg_dsn and not HAVE_PSYCOPG2:
        print("Warning: --pg-dsn given but psycopg2 is not installed; skipping PostgreSQL I/O.", file=sys.stderr)
        pg_dsn = None

    results = []
    for i in range(args.runs):
        print(f"Running iteration {i + 1}/{args.runs}...", file=sys.stderr)
        results.append(run_once(cmd, interval=args.interval, pg_dsn=pg_dsn))

    write_csv(results, csv_path=args.csv)


if __name__ == "__main__":
    main()