"""Paths, database settings, and the list of implementations to benchmark."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

RESULTS_DB = ROOT / "data" / "results.db"
RESULTS_CSV = ROOT / "data" / "runs.csv"
TUNED_FILE = ROOT / "data" / "tuned.json"
SCHEMA_FILE = ROOT / "starschema.sql"

DW_CONFIG = {
    "host": os.getenv("DW_HOST", "localhost"),
    "port": int(os.getenv("DW_PORT", "5432")),
    "dbname": os.getenv("DW_DATABASE"),
    "user": os.getenv("USERNAME"),
}

SOURCE_CONFIG = {
    "host": os.getenv("SOURCE_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_PORT", "5432")),
    "dbname": os.getenv("SOURCE_DATABASE"),
    "user": os.getenv("USERNAME"),
}


CPYTHON = [str(ROOT / ".venv" / "bin" / "python")]
CPYTHON_NOGIL = [str(ROOT / ".venv-t" / "bin" / "python")]
JYTHON = [
    os.getenv("JYTHON", "/opt/jython/bin/jython"),
    f"-J-Xmx{os.getenv('JYTHON_HEAP', '4g')}",
    "-J-cp",
    os.getenv(
        "JYTHON_JDBC_JAR",
        "/usr/share/java/postgresql-jdbc/postgresql.jar",
    ),
]


@dataclass(frozen=True)
class Mode:
    """One implementation running on one runtime."""

    implementation: str
    runtime: str
    interpreter: list[str]
    args: list[str]
    oracle: bool = False
    heap: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    params: Mapping[str, object] = field(default_factory=dict)
    profile: str = "default"

    @property
    def command(self):
        interpreter = list(self.interpreter)

        if self.heap is not None:
            interpreter = [
                arg for arg in interpreter if not arg.startswith("-J-Xmx")
            ]
            interpreter.insert(1, f"-J-Xmx{self.heap}")

        return interpreter + self.args

    @property
    def label(self):
        return f"{self.implementation} ({self.runtime})"

    @property
    def key(self):
        return f"{self.implementation}@{self.runtime}"


MODES = [
    Mode(
        "cpygrametl1_db",
        "cpython-3.15",
        CPYTHON,
        ["-m", "shared.cpygrametl1_db"],
        oracle=True,
    ),
    Mode(
        "cpygrametl1_db",
        "cpython-3.15t",
        CPYTHON_NOGIL,
        ["-m", "shared.cpygrametl1_db"],
    ),
    Mode(
        "cpygrametl2_db",
        "cpython-3.15",
        CPYTHON,
        ["-m", "shared.cpygrametl2_db"],
    ),
    Mode(
        "cpygrametl2_db",
        "cpython-3.15t",
        CPYTHON_NOGIL,
        ["-m", "shared.cpygrametl2_db"],
    ),
    Mode(
        "cpygrametl1_async_db",
        "cpython-3.15",
        CPYTHON,
        ["-m", "shared.cpygrametl1_async_db"],
    ),
    Mode(
        "cpygrametl1_async_db",
        "cpython-3.15t",
        CPYTHON_NOGIL,
        ["-m", "shared.cpygrametl1_async_db"],
    ),
    Mode(
        "cpygrametl2_async_db",
        "cpython-3.15",
        CPYTHON,
        ["-m", "shared.cpygrametl2_async_db"],
    ),
    Mode(
        "cpygrametl2_async_db",
        "cpython-3.15t",
        CPYTHON_NOGIL,
        ["-m", "shared.cpygrametl2_async_db"],
    ),
    Mode(
        "pipeline sequential",
        "cpython-3.15",
        CPYTHON,
        ["-m", "pipelined.pipelined_sequential"],
    ),
    Mode(
        "pipeline multiprocessing",
        "cpython-3.15",
        CPYTHON,
        ["-m", "pipelined.pipelined_parallel"],
    ),
    Mode(
        "pipeline free-threading",
        "cpython-3.15t",
        CPYTHON_NOGIL,
        ["-m", "pipelined.pipelined_parallel"],
    ),
    Mode("jython1", "jython-2.7", JYTHON, ["jython/pygrametl1.py"]),
    Mode("jython2", "jython-2.7", JYTHON, ["jython/pygrametl2.py"]),
]
