"""
Async extraction with persistent named cursors and batched reads.

The independent downloadlog and testresults queries run concurrently through
asyncio.gather(). Each query uses a named server-side cursor and fetchmany()
with FETCHSIZE, while the results are materialized and joined in memory.
The downstream pygrametl load remains synchronous and unchanged.
"""

import asyncio
import datetime
import os
import time

import psycopg  # psycopg 3.x throughout -- async named cursors need v3
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

FETCHSIZE = 5000  # matches cpygrametl1_db.py's SQLSource fetchsize


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


# --- persistent async connections, opened once, matching sourceconn1/2 -----

_source_conn1 = None
_source_conn2 = None


async def _get_source_connections():
    global _source_conn1, _source_conn2
    if _source_conn1 is None:
        _source_conn1 = await psycopg.AsyncConnection.connect(
            host=source_host, port=source_port, dbname=source_database,
            user=username,
        )
    if _source_conn2 is None:
        _source_conn2 = await psycopg.AsyncConnection.connect(
            host=source_host, port=source_port, dbname=source_database,
            user=username,
        )
    return _source_conn1, _source_conn2


async def fetch_named_cursor(conn, cursor_name, query):
    """Same shape as SQLSource.__iter__: named cursor, fetchmany loop,
    same fetchsize. Only async/await added."""
    async with conn.cursor(cursor_name) as cur:
        await cur.execute(query)
        all_rows = []
        names = None
        while True:
            data = await cur.fetchmany(FETCHSIZE)
            if not data:
                break
            if names is None:
                names = [d[0] for d in cur.description]
            all_rows.extend(dict(zip(names, row)) for row in data)
        return all_rows


async def extract_merged_rows():
    conn1, conn2 = await _get_source_connections()

    # The one change from the sync script: these two run concurrently
    # instead of sequentially.
    downloadlog_rows, testresults_rows = await asyncio.gather(
        fetch_named_cursor(conn1, "downloadlog_cursor", DOWNLOADLOG_SQL),
        fetch_named_cursor(conn2, "testresults_cursor", TESTRESULTS_SQL),
    )

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


_last_extraction_row_count = 0


def run_extraction():
    """Plain sync entry point -- see cpygrametl_async_extract.py for why
    this indirection matters for profile_etl.py's timing wrapper."""
    global _last_extraction_row_count
    rows = asyncio.run(extract_merged_rows())
    _last_extraction_row_count = len(rows)
    return rows


# --- everything below is unmodified pygrametl, identical to cpygrametl1_db.py

import psycopg2  # noqa: E402  -- DW side stays on psycopg2, same as cpygrametl1_db.py

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
