# benchmark_config.py
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

DW_DATABASE = os.getenv("DW_DATABASE")
TOXIPROXY_API = os.getenv("TOXIPROXY_API")
TOXIPROXY_PROXY = os.getenv("TOXIPROXY_PROXY")

IMPLEMENTATIONS = {
    "database": "cpygrametl1_db.py",
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