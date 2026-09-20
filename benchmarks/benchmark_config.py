import os
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

DW_DATABASE = os.getenv("DW_DATABASE")
TOXIPROXY_API = os.getenv("TOXIPROXY_API")
TOXIPROXY_PROXY = os.getenv("TOXIPROXY_PROXY")

ALL_IMPLEMENTATIONS = {
    "csv": "cpygrametl1.py",
    "database": "cpygrametl1_db.py",
}

IMPLEMENTATION_ALIASES = {
    "both": None,
    "csv": "csv",
    "db": "database",
    "database": "database",
}

BENCHMARK_IMPLEMENTATION = os.getenv(
    "BENCHMARK_IMPLEMENTATION",
    "both",
).lower()

if BENCHMARK_IMPLEMENTATION not in IMPLEMENTATION_ALIASES:
    raise ValueError(
        "BENCHMARK_IMPLEMENTATION must be one of: "
        "both, csv, db, database"
    )

selected_implementation = IMPLEMENTATION_ALIASES[
    BENCHMARK_IMPLEMENTATION
]

if selected_implementation is None:
    IMPLEMENTATIONS = ALL_IMPLEMENTATIONS
else:
    IMPLEMENTATIONS = {
        selected_implementation: ALL_IMPLEMENTATIONS[
            selected_implementation
        ]
    }

PHASES = [
    "initialisation",
    "extraction_merge",
    "transformation",
    "page_dimension",
    "date_dimension",
    "test_dimension",
    "fact_insert",
    "commit",
    "connection_close",
    "other",
]

try:
    REPEATS = int(os.getenv("REPEATS", "1"))
except ValueError as error:
    raise ValueError("REPEATS must be an integer") from error

if REPEATS < 1:
    raise ValueError("REPEATS must be at least 1")

DEFAULT_PAGE_SIZES = [100]
DEFAULT_SOURCE_RTT_MS = [0]
RESULTS_DB = ROOT / "data" / "benchmark_results.db"
PROFILE_SCRIPT = ROOT / "benchmarks" / "profile_etl.py"
LATENCY_UP_TOXIC = "latency-up"
LATENCY_DOWN_TOXIC = "latency-down"


def validate_sizes(sizes):
    invalid_sizes = [size for size in sizes if size < 1]

    if invalid_sizes:
        raise ValueError(
            f"All sizes must be positive integers, got {invalid_sizes!r}"
        )


def validate_latency(latencies):
    invalid_latencies = [latency for latency in latencies if latency < 0]

    if invalid_latencies:
        raise ValueError(
            "Latency values must be non-negative integers, "
            f"got {invalid_latencies!r}"
        )
