#!/usr/bin/env python3
"""Compare original and PostgreSQL-optimised SCD on identical source data.

Run from Workflow after preparing a 25-page source dataset:
    python -m experiments.check_scd_etl_correctness --yes-reset-warehouse

WARNING: This script re-executes Workflow/starschema.sql before EACH run,
recreating the benchmark warehouse schema and its seeded dimensions.
It does not regenerate source data.
"""

import argparse
import asyncio
import hashlib
import os
import sys
from pathlib import Path
from dataclasses import dataclass

import psycopg
from psycopg import sql
from dotenv import load_dotenv

from shared.cpygrametl1_async_db import create_etl, run_etl


SCHEMA = "pygrametlexa"
ROOT = Path(__file__).resolve().parents[1]
COMPARED_TABLES = ("page", "testresults", "date", "test")


@dataclass(frozen=True)
class Fingerprint:
    rows: int
    sha256: str


def connect_dw():
    return psycopg.connect(
        host=os.getenv("DW_HOST", "localhost"),
        port=int(os.getenv("DW_PORT", "5432")),
        dbname=os.environ["DW_DATABASE"],
        user=os.environ["USERNAME"],
    )


def fingerprint(conn, table):
    """Hash *all* rows in deterministic order, including duplicates.

    ORDER BY every column avoids reliance on physical row ordering, which
    can differ between the original and optimised executions.
    """
    qualified = sql.Identifier(SCHEMA, table)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT * FROM {} LIMIT 0").format(qualified))
        columns = [desc.name for desc in cur.description]

    if not columns:
        raise RuntimeError(f"No columns found in {SCHEMA}.{table}")

    select_columns = sql.SQL(", ").join(map(sql.Identifier, columns))
    statement = sql.SQL("SELECT {} FROM {} ORDER BY {}").format(
        select_columns, qualified, select_columns
    )

    digest = hashlib.sha256()
    count = 0
    # Server-side cursor to avoid storing millions of fact rows in memory.
    with conn.cursor(name=f"compare_{table}") as cur:
        cur.itersize = 10_000
        cur.execute(statement)
        for record in cur:
            # Both runs happen in the same interpreter, using the same
            # PostgreSQL driver and schema, so tuple repr is deterministic.
            encoded = repr(record).encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
            count += 1

    return Fingerprint(rows=count, sha256=digest.hexdigest())


def fingerprints(tables):
    with connect_dw() as conn:
        return {table: fingerprint(conn, table) for table in tables}



def count_source_joined_rows():
    """Fail early if the loaded source dataset is not the requested workload."""
    with psycopg.connect(
        host=os.getenv("SOURCE_HOST", "localhost"),
        port=int(os.getenv("SOURCE_PORT", "5432")),
        dbname=os.environ["SOURCE_DATABASE"],
        user=os.environ["USERNAME"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM downloadlog d "
                "JOIN testresults t ON d.localfile = t.localfile"
            )
            return cur.fetchone()[0]

def reset_warehouse():
    """Reset the entire benchmark DW identically to benchmarks/benchmark.py.

    CAUTION: starschema.sql recreates the warehouse schema/tables. Use only
    against your disposable benchmark database.
    """
    schema = (ROOT / "starschema.sql").read_text()
    with connect_dw() as conn:
        with conn.cursor() as cur:
            cur.execute(schema)
        conn.commit()


async def run_one(mode, prefetch):
    os.environ["SCD_MODE"] = mode
    etl = await create_etl(prefetch=prefetch)
    dim = etl[4]
    expected_class = (
        "SlowlyChangingDimension" if mode == "original"
        else "PostgreSQLOptimizedSCD"
    )
    actual_class = type(dim).__name__
    if actual_class != expected_class:
        # create_etl has already opened connections: do not silently run
        # the wrong implementation, even if this requires process cleanup.
        raise RuntimeError(
            f"Wrong dimension selected: {actual_class}; expected {expected_class}"
        )
    if mode == "optimized" and not dim._postgres_fastpath:
        raise RuntimeError("Optimised SCD did not enable its PostgreSQL fast path")
    print(f"Running {mode}: {actual_class} (prefetch={prefetch})", flush=True)
    await run_etl(*etl)


def print_fingerprints(label, results):
    print(f"\n{label}:")
    for table, result in results.items():
        print(f"  {SCHEMA}.{table}: rows={result.rows:,}, SHA-256={result.sha256}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yes-reset-warehouse", action="store_true",
        help="Required: acknowledge that starschema.sql will reset the complete benchmark warehouse twice",
    )
    parser.add_argument("--prefetch", type=int, default=2)
    parser.add_argument(
        "--expected-facts", type=int, default=1_125_000,
        help="Expected fact count (25-page workload: 1,125,000)",
    )
    args = parser.parse_args()

    if not args.yes_reset_warehouse:
        parser.error(
            "This script executes starschema.sql and resets the complete "
            "benchmark warehouse twice. "
            "Pass --yes-reset-warehouse only on the disposable benchmark DW."
        )
    if args.prefetch < 0 or args.expected_facts < 0:
        parser.error("--prefetch and --expected-facts must be non-negative")

    load_dotenv()
    if not all(os.getenv(key) for key in ("DW_DATABASE", "SOURCE_DATABASE", "USERNAME")):
        parser.error("Set DW_DATABASE, SOURCE_DATABASE and USERNAME in .env")

    source_rows = count_source_joined_rows()
    print(f"Loaded source produces {source_rows:,} joined rows", flush=True)
    if source_rows != args.expected_facts:
        parser.error(
            f"Wrong source dataset: {source_rows:,} joined rows, "
            f"expected {args.expected_facts:,}. Regenerate the 25-page "
            "source workload using your benchmark driver before retrying."
        )

    # Prevent instrumenting either implementation for this correctness test.
    original_mode = os.environ.get("SCD_MODE")
    try:
        print("Comparing complete page/fact/date/test output ...", flush=True)
        results = {}
        for mode in ("original", "optimized"):
            print(f"\n--- {mode.upper()} ---", flush=True)
            reset_warehouse()
            asyncio.run(run_one(mode, args.prefetch))
            results[mode] = fingerprints(COMPARED_TABLES)
            print_fingerprints(mode, results[mode])

            fact_rows = results[mode]["testresults"].rows
            if fact_rows != args.expected_facts:
                raise AssertionError(
                    f"{mode}: found {fact_rows:,} fact rows, expected "
                    f"{args.expected_facts:,}. Is the intended source workload loaded?"
                )

        print("\n--- COMPARISON ---")
        failures = []
        for table in COMPARED_TABLES:
            expected = results["original"][table]
            actual = results["optimized"][table]
            if actual == expected:
                print(f"PASS: {SCHEMA}.{table} has identical row count and full-table digest")
            else:
                print(f"FAIL: {SCHEMA}.{table} differs")
                failures.append(table)

        if failures:
            print("\nFAILED: Complete ETL outputs differ. Inspect affected tables.")
            return 1
        print("\nPASS: Original and optimised ETL outputs match for this dataset.")
        print("Both ETL runs started from the same starschema.sql reset.")
        print("This does not check failure or concurrency semantics.")
        return 0
    finally:
        if original_mode is None:
            os.environ.pop("SCD_MODE", None)
        else:
            os.environ["SCD_MODE"] = original_mode


if __name__ == "__main__":
    sys.exit(main())
