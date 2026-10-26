
"""Compare original and PostgreSQL-optimised SCD implementations.

Uses PostgreSQL temporary tables. Does not modify the real warehouse.
Run from Workflow with:
    python -m experiments.check_scd_correctness
"""

import os
import random
from copy import deepcopy
from datetime import date

import psycopg
from dotenv import load_dotenv

from pygrametl import ConnectionWrapper
from pygrametl.tables import (
    PostgreSQLOptimizedSCD,
    SlowlyChangingDimension,
)


load_dotenv()

ATTRIBUTES = [
    "url",
    "size",
    "domain",
    "topleveldomain",
    "serverversion",
    "server",
    "validfrom",
    "validto",
    "version",
]


def create_temp_table(connection, name):
    connection.execute(
        f"""
        CREATE TEMP TABLE {name} (
            pageid INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            size INTEGER,
            domain TEXT,
            topleveldomain TEXT,
            serverversion TEXT,
            server TEXT,
            validfrom DATE,
            validto DATE,
            version INTEGER
        ) ON COMMIT PRESERVE ROWS
        """
    )

    # Start with an existing dimension member to exercise prefill
    # and the initial MAX(pageid) surrogate-key calculation.
    connection.execute(
        f"""
        INSERT INTO {name} (
            pageid, url, size, domain, topleveldomain,
            serverversion, server, validfrom, validto, version
        )
        VALUES (
            10, 'seed.example.com', 100, 'example.com', 'com',
            'Apache/1', 'Apache', '2024-01-01', NULL, 1
        )
        """
    )


def make_dimension(cls, name, connection):
    return cls(
        name=name,
        key="pageid",
        attributes=ATTRIBUTES,
        lookupatts=["url"],
        versionatt="version",
        fromatt="validfrom",
        toatt="validto",
        srcdateatt="lastmoddate",
        cachesize=-1,
        prefill=True,
        targetconnection=connection,
    )


def page(
    url,
    size,
    lastmoddate,
    serverversion="Apache/1",
):
    return {
        "url": url,
        "size": size,
        "domain": "example.com",
        "topleveldomain": "com",
        "serverversion": serverversion,
        "server": "Apache",
        "lastmoddate": lastmoddate,
    }


def build_cases():
    cases = [
        # Existing member loaded through cache prefill.
        ("ensure", page("seed.example.com", 100, "2024-01-01")),
        ("ensure", page("seed.example.com", 200, "2024-01-02")),

        # First version, unchanged version, changed size.
        ("ensure", page("a.example.com", 100, "2024-01-01")),
        ("ensure", page("a.example.com", 100, "2024-01-01")),
        ("ensure", page("a.example.com", 200, "2024-01-02")),

        # Same size, different source date.
        ("ensure", page("a.example.com", 200, "2024-01-03")),

        # Same size and date, changed non-size attribute.
        (
            "ensure",
            page("a.example.com", 200, "2024-01-03", "Apache/2"),
        ),

        # Another business key.
        ("ensure", page("b.example.com", 500, "2024-02-01")),
        ("ensure", page("b.example.com", 500, "2024-02-01")),

        # Explicitly close a version, then ensure it again.
        ("close", ("b.example.com", date(2024, 2, 2))),
        ("ensure", page("b.example.com", 500, "2024-02-03")),
    ]

    # Additional deterministic mixed workload.
    rng = random.Random(42)

    for i in range(500):
        url = f"generated-{i % 20}.example.com"
        size = rng.choice([100, 200, 300, 400])
        day = (i // 20) % 25 + 1
        source_date = f"2024-03-{day:02d}"

        record = page(url, size, source_date)

        cases.append(("ensure", record))

        # Repeating the exact same record should reuse the version.
        if i % 4 == 0:
            cases.append(("ensure", deepcopy(record)))

    return cases


def run_cases(dimension, cases):
    """Capture return values and mutated input rows."""
    results = []

    for index, (operation, data) in enumerate(cases):
        if operation == "ensure":
            row = deepcopy(data)
            key = dimension.scdensure(row)

            results.append({
                "operation": "ensure",
                "key": key,
                "row": row,
            })

        elif operation == "close":
            url, end_date = data

            dimension.closecurrent(
                {"url": url},
                end=end_date,
            )

            results.append({
                "operation": "close",
                "url": url,
                "end": end_date,
            })

        else:
            raise ValueError(
                f"Unknown operation at index {index}: {operation}"
            )

    return results


def fetch_history(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                pageid,
                url,
                size,
                domain,
                topleveldomain,
                serverversion,
                server,
                validfrom,
                validto,
                version
            FROM {table_name}
            ORDER BY pageid
            """
        )
        return cursor.fetchall()


def compare_results(original, optimized, label):
    if len(original) != len(optimized):
        raise AssertionError(
            f"{label}: different lengths "
            f"({len(original)} vs {len(optimized)})"
        )

    for index, (expected, actual) in enumerate(
        zip(original, optimized)
    ):
        if expected != actual:
            raise AssertionError(
                f"\n{label} differs at index {index}\n"
                f"Original:  {expected!r}\n"
                f"Optimized: {actual!r}"
            )

    print(f"PASS: {label} ({len(original):,} records)")


def main():
    database = os.getenv("DW_DATABASE")
    username = os.getenv("USERNAME")

    if not database or not username:
        raise RuntimeError(
            "DW_DATABASE and USERNAME must be configured in .env"
        )

    pgconn = psycopg.connect(
        host=os.getenv("DW_HOST", "localhost"),
        port=int(os.getenv("DW_PORT", "5432")),
        dbname=database,
        user=username,
    )

    try:
        original_table = "scd_check_original"
        optimized_table = "scd_check_optimized"

        create_temp_table(pgconn, original_table)
        create_temp_table(pgconn, optimized_table)

        wrapper = ConnectionWrapper(pgconn)

        original_dim = make_dimension(
            SlowlyChangingDimension,
            original_table,
            wrapper,
        )
        optimized_dim = make_dimension(
            PostgreSQLOptimizedSCD,
            optimized_table,
            wrapper,
        )

        if not optimized_dim._postgres_fastpath:
            raise AssertionError(
                "Optimized dimension did not enable its fast path"
            )

        cases = build_cases()

        print(f"Running {len(cases):,} test operations...")

        original_results = run_cases(original_dim, cases)
        optimized_results = run_cases(optimized_dim, cases)

        # Compare every return value and input-dictionary mutation.
        compare_results(
            original_results,
            optimized_results,
            "Returned keys and row mutations",
        )

        # Compare the complete contents of both dimension tables.
        original_history = fetch_history(
            pgconn, original_table
        )
        optimized_history = fetch_history(
            pgconn, optimized_table
        )

        compare_results(
            original_history,
            optimized_history,
            "Complete SCD version history",
        )

        # Check structural correctness of the resulting history.
        by_url = {}

        for record in optimized_history:
            (
                key,
                url,
                size,
                domain,
                tld,
                serverversion,
                server,
                validfrom,
                validto,
                version,
            ) = record

            by_url.setdefault(url, []).append(record)

        for url, versions in by_url.items():
            numbers = [record[-1] for record in versions]

            if numbers != list(range(1, len(versions) + 1)):
                raise AssertionError(
                    f"Non-sequential versions for {url}: {numbers}"
                )

            open_versions = [
                record for record in versions
                if record[-2] is None
            ]

            if len(open_versions) != 1:
                raise AssertionError(
                    f"{url} has {len(open_versions)} open versions"
                )

        print(
            f"PASS: Sequential versions and one open "
            f"version per URL ({len(by_url):,} URLs)"
        )

        print("\n" + "=" * 55)
        print("ALL SCD CORRECTNESS CHECKS PASSED")
        print("=" * 55)
        print(f"Operations:       {len(cases):,}")
        print(f"Dimension members: {len(by_url):,}")
        print(f"Historical rows:  {len(optimized_history):,}")

    finally:
        # Temporary tables disappear when the connection closes.
        pgconn.rollback()
        pgconn.close()


if __name__ == "__main__":
    main()
