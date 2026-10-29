"""Paths, database settings, and the list of implementations to benchmark."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

RESULTS_DB = ROOT / "data" / "results.db"
SCHEMA_FILE = ROOT / "starschema.sql"

DW_CONFIG = {
    "host": os.getenv("DW_HOST", "localhost"),
    "port": int(os.getenv("DW_PORT", "5432")),
    "dbname": os.getenv("DW_DATABASE"),
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

    @property
    def command(self):
        return self.interpreter + self.args

    @property
    def label(self):
        return f"{self.implementation} ({self.runtime})"


MODES = [
    Mode(
        "cpygrametl1_db",
        "cpython-3.14",
        CPYTHON,
        ["-m", "shared.cpygrametl1_db"],
        oracle=True,
    ),
    Mode(
        "cpygrametl1_db",
        "cpython-3.14t",
        CPYTHON_NOGIL,
        ["-m", "shared.cpygrametl1_db"],
    ),
        Mode(
        "cpygrametl1_db_async",
        "cpython-3.14",
        CPYTHON,
        ["-m", "async_workflows.cpygrametl1_db_async"],
        oracle=True,
    ),
    Mode(
        "cpygrametl1_db_async",
        "cpython-3.14t",
        CPYTHON_NOGIL,
        ["-m", "async_workflows.cpygrametl1_db_async"],
    ),
    Mode("cpygrametl2_db", "cpython-3.14", CPYTHON, ["-m", "shared.cpygrametl2_db"]),
    Mode(
        "cpygrametl2_db",
        "cpython-3.14t",
        CPYTHON_NOGIL,
        ["-m", "shared.cpygrametl2_db"],
    ),
    Mode("cpygrametl2_db_async", "cpython-3.14", CPYTHON, ["-m", "async_workflows.cpygrametl2_db_async"]),
    Mode(
        "cpygrametl2_db_async",
        "cpython-3.14t",
        CPYTHON_NOGIL,
        ["-m", "async_workflows.cpygrametl2_db_async"],
    ),
    Mode(
        "pipeline",
        "cpython-3.14",
        CPYTHON,
        ["pipelined_workflows/cpygrametl1_db_pipelined.py"],
    ),
    Mode("jython1", "jython-2.7", JYTHON, ["jython/pygrametl1.py"]),
    Mode("jython2", "jython-2.7", JYTHON, ["jython/pygrametl2.py"]),
]
