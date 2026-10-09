"""
Async extraction with a synchronous pygrametl load.

The independent downloadlog and testresults queries run concurrently through
asyncio.gather(). Each query uses a plain cursor and fetchall(), so its rows
are materialized in memory before the two result sets are joined. The
downstream page, test, date, and fact-table processing remains synchronous and
uses pygrametl without modification.
"""

import asyncio
import datetime
import os
import time

import psycopg  # psycopg 3.x -- only used for the async extraction step
import psycopg2  # unchanged from the original script, for the DW side
import pygrametl
from dotenv import load_dotenv

from pygrametl import ConnectionWrapper
from pygrametl.tables import (
    BulkFactTable,
    CachedDimension,
    SlowlyChangingDimension,
)


load_dotenv()

username = os.getenv("USERNAME")
source_database = os.getenv("SOURCE_DATABASE")
dw_database = os.getenv("DW_DATABASE")
source_host = os.getenv("SOURCE_HOST", "localhost")
source_port = int(os.getenv("SOURCE_PORT", "5432"))


DOWNLOADLOG_SQL = """
    SELECT localfile, url, serverversion, size::text AS size,
           downloaddate::text AS downloaddate, lastmoddate::text AS lastmoddate
    FROM downloadlog
    ORDER BY localfile
"""

TESTRESULTS_SQL = """
    SELECT localfile, test, errors::text AS errors
    FROM testresults
    ORDER BY localfile, test
"""


# --- async extraction: the one genuine concurrency win ----------------------

async def fetch_all(host, port, dbname, user, query):
    """Plain cursor, no cursorarg -- avoids the BEGIN/DECLARE/CLOSE overhead
    of a named server-side cursor. Fine for result sets that comfortably
    fit in memory; revisit if source tables get large enough that
    server-side cursors become necessary for memory reasons."""
    conn = await psycopg.AsyncConnection.connect(
        host=host, port=port, dbname=dbname, user=user, autocommit=True,
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            names = [d[0] for d in cur.description]
            return [dict(zip(names, r)) for r in rows]
    finally:
        await conn.close()


async def extract_merged_rows():
    """Runs both source reads concurrently, then merge-joins in memory.
    Returns a plain list[dict] -- everything downstream of this is
    synchronous, unmodified pygrametl."""
    downloadlog_rows, testresults_rows = await asyncio.gather(
        fetch_all(source_host, source_port, source_database, username, DOWNLOADLOG_SQL),
        fetch_all(source_host, source_port, source_database, username, TESTRESULTS_SQL),
    )

    # In-memory equivalent of MergeJoiningSource(downloadlog, 'localfile',
    # testresults, 'localfile'): both queries are already ORDER BY localfile,
    # so grouping testresults by localfile and joining preserves the same
    # rows MergeJoiningSource would have produced.
    by_localfile = {}
    for row in testresults_rows:
        by_localfile.setdefault(row["localfile"], []).append(row)

    merged = []
    for dl_row in downloadlog_rows:
        for tr_row in by_localfile.get(dl_row["localfile"], []):
            combined = dl_row.copy()
            combined.update(tr_row)
            merged.append(combined)
    return merged


# Last row count from run_extraction(), for profile_etl.py to read after
# module.main() completes -- there's no module-level `inputdata` iterable
# here for a TimedIterable to wrap (unlike the original SQLSource-based
# scripts), so this is the profiler's hook into row count instead.
_last_extraction_row_count = 0


def run_extraction():
    """Plain synchronous entry point wrapping the async extraction.

    profile_etl.py's timed_function wrapper only measures wall/CPU time
    around a blocking call -- calling extract_merged_rows() directly would
    just construct a coroutine object near-instantly and report ~0s, since
    the actual work happens later inside asyncio.run(). Routing through
    this plain function means the profiler's wrapper call blocks for the
    full asyncio.run() duration and measures it correctly.
    """
    global _last_extraction_row_count
    rows = asyncio.run(extract_merged_rows())
    _last_extraction_row_count = len(rows)
    return rows


# --- everything below is unmodified pygrametl, exactly as the original script

pgconn = psycopg2.connect(host="localhost", dbname=dw_database, user=username)
connection = ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute("SET search_path TO pygrametlexa")


def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filehandle):
    cursor = pgconn.cursor()
    cursor.copy_from(
        file=filehandle, table=name, sep=fieldsep, null=str(nullval), columns=atts,
    )


def datehandling(row, namemapping):
    date = pygrametl.getvalue(row, "date", namemapping)
    (year, month, day, hour, minute, second, weekday, dayinyear, dst) = time.strptime(
        date, "%Y-%m-%d"
    )
    isoyear, isoweek, isoweekday = datetime.date(year, month, day).isocalendar()
    row["day"] = day
    row["month"] = month
    row["year"] = year
    row["week"] = isoweek
    row["weekyear"] = isoyear
    row["dateid"] = dayinyear + 366 * (year - 1990)
    return row


def extractdomaininfo(row):
    domaininfo = row["url"].split("/")[-2]
    row["domain"] = domaininfo
    row["topleveldomain"] = domaininfo.split(".")[-1]


def extractserverinfo(row):
    row["server"] = row["serverversion"].split("/")[0]


pagedim = SlowlyChangingDimension(
    name="page",
    key="pageid",
    attributes=["url", "size", "domain", "topleveldomain", "serverversion",
                "server", "validfrom", "validto", "version"],
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


def main():
    print(time.asctime())

    # The only async part: gather both source reads concurrently, then hand
    # a plain list of dicts to the rest of the (synchronous) workflow.
    # Calling the module-level run_extraction() by name (rather than
    # asyncio.run(extract_merged_rows()) inline) lets profile_etl.py
    # monkey-patch it before main() runs.
    inputdata = run_extraction()

    for row in inputdata:
        extractdomaininfo(row)
        extractserverinfo(row)
        row["size"] = pygrametl.getint(row["size"])

        row["pageid"] = pagedim.scdensure(row)
        row["dateid"] = datedim.ensure(row, {"date": "downloaddate"})
        row["testid"] = testdim.lookup(row, {"testname": "test"})

        facttbl.insert(row)

    connection.commit()
    connection.close()

    print(time.asctime())


if __name__ == "__main__":
    main()