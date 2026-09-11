import csv
import time
from pathlib import Path


def write_benchmark_csv(script_name, runtime, timings, rows_processed):
    """Write a benchmark summary CSV under Workflow/benchmarks/<runtime>/."""
    root = Path(__file__).resolve().parent
    out_dir = root / "benchmarks" / runtime
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = time.strftime("%Y-%m-%d_%H%M%S")
    out_file = out_dir / f"{script_name}_{timestamp}.csv"

    with out_file.open("w", newline="") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=["runtime", "phase", "seconds", "rows"]
        )
        writer.writeheader()
        for phase, seconds in timings:
            writer.writerow(
                {
                    "runtime": runtime,
                    "phase": phase,
                    "seconds": round(seconds, 6),
                    "rows": rows_processed,
                }
            )

    return out_file
