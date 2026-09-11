import csv
import os
import sys
import time
from pathlib import Path


def write_benchmark_csv(script_name, runtime, timings, rows_processed):
    """Write a benchmark summary CSV under Workflow/benchmarks/<runtime>/."""
    root = Path(__file__).resolve().parent
    out_dir = root / "benchmarks" / runtime
    if not out_dir.exists():
        os.makedirs(str(out_dir))

    timestamp = time.strftime("%Y-%m-%d_%H%M%S")
    out_file = out_dir / "{script_name}_{timestamp}.csv".format(
        script_name=script_name, timestamp=timestamp
    )

    if sys.version_info[0] < 3:
        csvfile = out_file.open("wb")
    else:
        csvfile = out_file.open("w", newline="")

    with csvfile:
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
