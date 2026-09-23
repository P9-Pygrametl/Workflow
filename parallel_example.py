"""Load the web-download example with pure transforms in subinterpreters.

Install the modified pygrametl package in the workflow's Python environment
and place etl_transforms.py beside this file. Run with CPython 3.14+,
psycopg2, python-dotenv, the existing PostgreSQL tables,
and the environment variables USERNAME, SOURCE_DATABASE, DW_DATABASE,
SOURCE_HOST (optional), and SOURCE_PORT (optional).

The SQL source reader runs in a regular thread. Its two database connections
are created and used in that thread. The destination connection, dimensions,
and bulk fact loader stay in the main interpreter.
"""

import datetime
import os
import threading
import time
from queue import Empty

import psycopg2
import pygrametl
from dotenv import load_dotenv
from pygrametl import ConnectionWrapper
from pygrametl.datasources import MergeJoiningSource, SQLSource
from pygrametl.parallel import createflow
from pygrametl.tables import (
    BulkFactTable,
    CachedDimension,
    SlowlyChangingDimension,
)

from etl_transforms import convertmeasures, extractdomaininfo, extractserverinfo


def datehandling(row, namemapping):
    date = pygrametl.getvalue(row, "date", namemapping)
    year, month, day, _, _, _, _, dayinyear, _ = time.strptime(date, "%Y-%m-%d")
    isoyear, isoweek, _ = datetime.date(year, month, day).isocalendar()

    row["day"] = day
    row["month"] = month
    row["year"] = year
    row["week"] = isoweek
    row["weekyear"] = isoyear
    row["dateid"] = dayinyear + 366 * (year - 1990)
    return row


def source_rows(host, port, database, username):
    """Yield merged records; create both connections in the calling thread."""
    sourceconn1 = psycopg2.connect(
        host=host, port=port, dbname=database, user=username
    )
    try:
        sourceconn2 = psycopg2.connect(
            host=host, port=port, dbname=database, user=username
        )
        try:
            downloadlog = SQLSource(
                connection=sourceconn1,
                query="""
                    SELECT localfile, url, serverversion,
                           size::text AS size,
                           downloaddate::text AS downloaddate,
                           lastmoddate::text AS lastmoddate
                    FROM downloadlog
                    ORDER BY localfile
                """,
                cursorarg="downloadlog_cursor",
                fetchsize=5000,
            )
            testresults = SQLSource(
                connection=sourceconn2,
                query="""
                    SELECT localfile, test, errors::text AS errors
                    FROM testresults
                    ORDER BY localfile, test
                """,
                cursorarg="testresults_cursor",
                fetchsize=5000,
            )
            yield from MergeJoiningSource(
                downloadlog, "localfile", testresults, "localfile"
            )
        finally:
            sourceconn2.close()
    finally:
        sourceconn1.close()


def main():
    load_dotenv()
    profile = os.getenv("ETL_PROFILE") == "1"
    started = time.perf_counter()
    username = os.getenv("USERNAME")
    source_database = os.getenv("SOURCE_DATABASE")
    dw_database = os.getenv("DW_DATABASE")
    if not all((username, source_database, dw_database)):
        raise ValueError("Set USERNAME, SOURCE_DATABASE and DW_DATABASE")

    source_host = os.getenv("SOURCE_HOST", "localhost")
    source_port_value = os.getenv("SOURCE_PORT", "5432")
    try:
        source_port = int(source_port_value)
    except ValueError as exc:
        raise ValueError(
            f"SOURCE_PORT must be an integer, got {source_port_value!r}"
        ) from exc

    # A connection must be constructed in the interpreter that uses it.
    pgconn = psycopg2.connect(host="localhost", dbname=dw_database, user=username)
    connection = ConnectionWrapper(pgconn)
    connection.setasdefault()
    connection.execute("SET search_path TO pygrametlexa")

    def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filehandle):
        with pgconn.cursor() as cursor:
            cursor.copy_from(
                file=filehandle,
                table=name,
                sep=fieldsep,
                null=str(nullval),
                columns=atts,
            )

    pagedim = SlowlyChangingDimension(
        name="page",
        key="pageid",
        attributes=[
            "url", "size", "domain", "topleveldomain", "serverversion",
            "server", "validfrom", "validto", "version",
        ],
        lookupatts=["url"],
        versionatt="version",
        fromatt="validfrom",
        toatt="validto",
        srcdateatt="lastmoddate",
        cachesize=-1,
        prefill=True,
    )
    testdim = CachedDimension(
        name="test",
        key="testid",
        attributes=["testname", "testauthor"],
        lookupatts=["testname"],
        prefill=True,
        defaultidvalue=-1,
    )
    datedim = CachedDimension(
        name="date",
        key="dateid",
        attributes=["date", "day", "month", "year", "week", "weekyear"],
        lookupatts=["date"],
        rowexpander=datehandling,
        prefill=True,
    )
    facttbl = BulkFactTable(
        name="testresults",
        keyrefs=["pageid", "testid", "dateid"],
        measures=["errors"],
        bulkloader=pgcopybulkloader,
        bulksize=250000,
    )
    warehouse_setup_seconds = time.perf_counter() - started

    # Each argument is a stage in its own persistent subinterpreter. Only row
    # dictionaries cross the boundaries; the target tables never do.
    flow_setup_started = time.perf_counter()
    flow = createflow(
        (extractdomaininfo, extractserverinfo),
        convertmeasures,
        batchsize=1000,
        queuesize=25,
    )
    flow_setup_seconds = time.perf_counter() - flow_setup_started
    source_errors = []
    source_stats = {"rows": 0, "elapsed": 0.0, "enqueue": 0.0}
    load_stats = {
        "rows": 0,
        "wait": 0.0,
        "page": 0.0,
        "date": 0.0,
        "test": 0.0,
        "fact": 0.0,
    }
    join_seconds = 0.0
    commit_seconds = 0.0

    def feed_flow():
        source_started = time.perf_counter()
        try:
            for row in source_rows(
                source_host, source_port, source_database, username
            ):
                if profile:
                    enqueue_started = time.perf_counter()
                    flow.process(row)
                    source_stats["enqueue"] += time.perf_counter() - enqueue_started
                    source_stats["rows"] += 1
                else:
                    flow.process(row)
        except Exception as exc:
            source_errors.append(exc)
        finally:
            flow.close()
            source_stats["elapsed"] = time.perf_counter() - source_started

    producer = threading.Thread(target=feed_flow, name="SQL source reader", daemon=True)
    print(time.asctime())
    producer.start()

    try:
        if profile:
            while True:
                wait_started = time.perf_counter()
                try:
                    row = flow.get()
                except Empty:
                    load_stats["wait"] += time.perf_counter() - wait_started
                    break
                load_stats["wait"] += time.perf_counter() - wait_started

                operation_started = time.perf_counter()
                row["pageid"] = pagedim.scdensure(row)
                load_stats["page"] += time.perf_counter() - operation_started

                operation_started = time.perf_counter()
                row["dateid"] = datedim.ensure(row, {"date": "downloaddate"})
                load_stats["date"] += time.perf_counter() - operation_started

                operation_started = time.perf_counter()
                row["testid"] = testdim.lookup(row, {"testname": "test"})
                load_stats["test"] += time.perf_counter() - operation_started

                operation_started = time.perf_counter()
                facttbl.insert(row)
                load_stats["fact"] += time.perf_counter() - operation_started
                load_stats["rows"] += 1
        else:
            for row in flow:
                row["pageid"] = pagedim.scdensure(row)
                row["dateid"] = datedim.ensure(row, {"date": "downloaddate"})
                row["testid"] = testdim.lookup(row, {"testname": "test"})
                facttbl.insert(row)

        producer.join()
        if source_errors:
            raise RuntimeError("Reading the source database failed") from source_errors[0]

        join_started = time.perf_counter()
        flow.join()
        join_seconds = time.perf_counter() - join_started

        commit_started = time.perf_counter()
        connection.commit()
        commit_seconds = time.perf_counter() - commit_started
        print(time.asctime())
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
        if profile:
            print(f"Warehouse setup: {warehouse_setup_seconds:.3f} s")
            print(f"Flow setup:      {flow_setup_seconds:.3f} s")
            print(
                f"Source thread:   {source_stats['elapsed']:.3f} s; "
                f"{source_stats['rows']} rows; "
                f"flow.process: {source_stats['enqueue']:.3f} s"
            )
            print(f"Flow.get waits: {load_stats['wait']:.3f} s")
            for label in ("page", "date", "test", "fact"):
                print(f"{label:>12}: {load_stats[label]:.3f} s")
            print(f"Rows loaded:     {load_stats['rows']}")
            print(f"Flow.join:       {join_seconds:.3f} s")
            print(f"Commit:          {commit_seconds:.3f} s")
            print(f"Total elapsed:   {time.perf_counter() - started:.3f} s")


if __name__ == "__main__":
    main()
