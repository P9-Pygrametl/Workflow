"""SQLite storage for benchmark runs."""

import sqlite3
from dataclasses import asdict, dataclass

from run_all.config import RESULTS_DB


@dataclass
class RunResult:
    timestamp: str
    implementation: str
    runtime: str
    pages: int
    run: int
    wall_seconds: float | None = None
    cpu_seconds: float | None = None
    cpu_percent: float | None = None
    peak_memory_mb: float | None = None
    memory_metric: str | None = None
    page_rows: int | None = None
    test_rows: int | None = None
    date_rows: int | None = None
    fact_rows: int | None = None
    valid: int | None = None
    validation_error: str | None = None
    warehouse_signature: str | None = None
    pygrametl_version: str | None = None
    runtime_version: str | None = None
    returncode: int | None = None
    error: str | None = None
    id: int | None = None

    @property
    def succeeded(self):
        return self.returncode == 0


SQL_TYPES = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "timestamp": "TEXT NOT NULL",
    "implementation": "TEXT NOT NULL",
    "runtime": "TEXT NOT NULL",
    "pages": "INTEGER NOT NULL",
    "run": "INTEGER NOT NULL",
    "wall_seconds": "REAL",
    "cpu_seconds": "REAL",
    "cpu_percent": "REAL",
    "peak_memory_mb": "REAL",
    "memory_metric": "TEXT",
    "page_rows": "INTEGER",
    "test_rows": "INTEGER",
    "date_rows": "INTEGER",
    "fact_rows": "INTEGER",
    "valid": "INTEGER",
    "validation_error": "TEXT",
    "warehouse_signature": "TEXT",
    "pygrametl_version": "TEXT",
    "runtime_version": "TEXT",
    "returncode": "INTEGER",
    "error": "TEXT",
}


class ResultsStore:
    """Results table that creates itself and upgrades older databases."""

    def __init__(self, path=RESULTS_DB):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self._create_table()
        self._add_missing_columns()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.connection.close()

    def save(self, result):
        """Insert a run and set result.id."""
        names = [n for n in SQL_TYPES if n in self._existing_columns()]
        values = asdict(result)

        cursor = self.connection.execute(
            f"INSERT INTO runs ({', '.join(names)}) "
            f"VALUES ({', '.join('?' for _ in names)})",
            [values[name] for name in names],
        )
        self.connection.commit()
        result.id = cursor.lastrowid

    def mark_invalid(self, result, message):
        """Flag a saved run as invalid, keeping any earlier reasons."""
        reasons = [result.validation_error, message]
        result.validation_error = "; ".join(r for r in reasons if r)
        result.valid = 0

        self.connection.execute(
            "UPDATE runs SET valid = 0, validation_error = ? WHERE id = ?",
            (result.validation_error, result.id),
        )

    def commit(self):
        self.connection.commit()

    def _existing_columns(self):
        rows = self.connection.execute("PRAGMA table_info(runs)")
        return {row[1] for row in rows}

    def _create_table(self):
        columns = ", ".join(f"{n} {t}" for n, t in SQL_TYPES.items())
        self.connection.execute(f"CREATE TABLE IF NOT EXISTS runs ({columns}) STRICT")
        self.connection.commit()

    def _add_missing_columns(self):
        existing = self._existing_columns()
        for name, sql_type in SQL_TYPES.items():
            if name in existing or "NOT NULL" in sql_type:
                continue
            self.connection.execute(f"ALTER TABLE runs ADD COLUMN {name} {sql_type}")
        self.connection.commit()
