"""Low-overhead sampled SCD timing for the Workflow async ETL.

Run from Workflow:
    python -m experiments.profile_scd_light --pages 25 50 100 --repeats 3

WARNING: Regenerates source tables and rebuilds the warehouse star schema.
Unlike the original phase profiler, this samples only calls to scdensure.
"""

import argparse
import asyncio
import os
import statistics
import time
from functools import wraps
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from datagenerator.datagenerator_db import generate
from shared.cpygrametl1_async_db import create_etl, run_etl

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def reset_warehouse():
    schema = (ROOT / "starschema.sql").read_text()
    with psycopg.connect(
        host=os.getenv("DW_HOST", "localhost"),
        port=int(os.getenv("DW_PORT", "5432")),
        dbname=os.environ["DW_DATABASE"],
        user=os.getenv("USERNAME"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(schema)


def sampled_scd(dimension, sample_every=97):
    """Time every Nth SCD call; extrapolate time from the sampled mean.

    A prime interval helps avoid aligning with 5 test results per download.
    This is diagnostic sampling, not a substitute for a full phase trace.
    """
    original = dimension.scdensure
    stats = {"calls": 0, "samples": 0, "sample_ns": 0}

    @wraps(original)
    def wrapped(*args, **kwargs):
        stats["calls"] += 1
        if stats["calls"] % sample_every:
            return original(*args, **kwargs)

        start = time.perf_counter_ns()
        try:
            return original(*args, **kwargs)
        finally:
            stats["samples"] += 1
            stats["sample_ns"] += time.perf_counter_ns() - start

    dimension.scdensure = wrapped
    return stats


async def run_one(mode, prefetch, sample_every):
    os.environ["SCD_MODE"] = mode
    etl = await create_etl(prefetch=prefetch)
    stats = sampled_scd(etl[4], sample_every)
    start = time.perf_counter()
    await run_etl(*etl)
    wall = time.perf_counter() - start

    mean_us = (stats["sample_ns"] / stats["samples"] / 1000
               if stats["samples"] else float("nan"))
    estimated_scd = (stats["sample_ns"] / 1e9 *
                     stats["calls"] / stats["samples"]
                     if stats["samples"] else float("nan"))
    return {"wall": wall, "scd_estimate": estimated_scd,
            "mean_us": mean_us, **stats}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pages", nargs="+", type=int, default=[25])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--prefetch", type=int, default=2)
    parser.add_argument("--sample-every", type=int, default=97)
    parser.add_argument("--yes-reset-warehouse", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.yes_reset_warehouse:
        raise SystemExit("This experiment recreates source and warehouse tables. "
                         "Add --yes-reset-warehouse for disposable benchmark DBs.")
    if min(args.pages) < 1 or args.repeats < 1 or args.sample_every < 1:
        raise SystemExit("Pages, repeats and sample interval must be positive")
    if args.prefetch < 0:
        raise SystemExit("Prefetch must be non-negative")
    print("IMPORTANT: This regenerates source tables and resets the DW.")
    for pages in args.pages:
        print(f"\n=== pages={pages}: regenerating source data ===", flush=True)
        generate(pages)
        results = {"original": [], "optimized": []}
        # Alternating order reduces systematic run-order / thermal bias.
        for rep in range(args.repeats):
            modes = ("original", "optimized") if rep % 2 == 0 else ("optimized", "original")
            for mode in modes:
                reset_warehouse()
                print(f"  {mode} repetition {rep + 1}/{args.repeats}...", flush=True)
                r = asyncio.run(run_one(mode, args.prefetch, args.sample_every))
                results[mode].append(r)
                print(f"    wall={r['wall']:.2f}s "
                      f"estimated_scd={r['scd_estimate']:.2f}s "
                      f"sample_mean={r['mean_us']:.2f}us "
                      f"samples={r['samples']:,}/{r['calls']:,}", flush=True)
        print("  MEDIANS (sampled SCD estimates, not full phase timings):")
        for mode, runs in results.items():
            print(f"    {mode:9} total_wall={statistics.median(r['wall'] for r in runs):.2f}s "
                  f"estimated_scd={statistics.median(r['scd_estimate'] for r in runs):.2f}s")
    print("\nCompare the run wall times to existing clean results to assess sampling overhead.")


if __name__ == "__main__":
    main()
