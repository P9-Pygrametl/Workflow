"""Async extraction and merge joining with pygrametl multiprocessing warehouse.

Uses our AsyncSQLSource's built-in prefetching and the ORIGINAL
SlowlyChangingDimension, not PostgreSQLOptimizedSCD.
"""

import asyncio
import os

import psycopg
import pygrametl
from dotenv import load_dotenv

from pygrametl import ConnectionWrapper
from pygrametl.datasources import AsyncMergeJoiningSource, AsyncSQLSource
from pygrametl.parallel import getsharedsequencefactory, shareconnectionwrapper
from pygrametl.tables import (
    BulkFactTable,
    CachedDimension,
    DecoupledDimension,
    DecoupledFactTable,
    DimensionPartitioner,
    SlowlyChangingDimension,
)

from helpers.helpers import datehandling, extractdomaininfo, extractserverinfo

load_dotenv()

BATCHSIZE = 500


def get_prefetch():
    """Number of prefetched SQL batches per source (default: 2)."""
    raw = os.getenv("PREFETCH", "2")
    try:
        prefetch = int(raw)
    except ValueError as exc:
        raise ValueError(f"PREFETCH must be an integer, got {raw!r}") from exc
    if prefetch < 0:
        raise ValueError("PREFETCH must be non-negative")
    return prefetch


def create_etl():
    """Set up worker processes before creating the asyncio event loop."""
    username = os.getenv("USERNAME")
    source_database = os.getenv("SOURCE_DATABASE")
    dw_database = os.getenv("DW_DATABASE")
    source_host = os.getenv("SOURCE_HOST", "localhost")
    source_port = int(os.getenv("SOURCE_PORT", "5432"))
    dw_host = os.getenv("DW_HOST", "localhost")
    dw_port = int(os.getenv("DW_PORT", "5432"))

    pgconn = psycopg.connect(
        host=dw_host,
        port=dw_port,
        dbname=dw_database,
        user=username,
    )

    def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filename):
        sql = (
            f"COPY {name}({', '.join(atts)}) FROM STDIN "
            f"WITH (FORMAT text, DELIMITER '{fieldsep}', NULL '{nullval}')"
        )
        with open(filename, "rb") as file, pgconn.cursor().copy(sql) as copy:
            for chunk in iter(lambda: file.read(1 << 20), b""):
                copy.write(chunk)

    shrdconn = shareconnectionwrapper(
        ConnectionWrapper(pgconn), 10, (pgcopybulkloader,)
    )
    shrdconn.execute("SET search_path TO pygrametlexa")

    idfactory = getsharedsequencefactory(0)

    def page_dimensions():
        for _ in range(2):
            yield DecoupledDimension(
                SlowlyChangingDimension(
                    name="page",
                    key="pageid",
                    attributes=[
                        "url", "size", "domain", "topleveldomain",
                        "serverversion", "server", "validfrom", "validto",
                        "version",
                    ],
                    lookupatts=["url"],
                    versionatt="version",
                    fromatt="validfrom",
                    toatt="validto",
                    srcdateatt="lastmoddate",
                    cachesize=-1,
                    prefill=True,
                    idfinder=idfactory(),
                    targetconnection=shrdconn.copy(),
                ),
                batchsize=BATCHSIZE,
                queuesize=10,
            )

    pagedim = DimensionPartitioner(list(page_dimensions()))

    testdim = CachedDimension(
        name="test",
        key="testid",
        attributes=["testname", "testauthor"],
        lookupatts=["testname"],
        prefill=True,
        defaultidvalue=-1,
        targetconnection=shrdconn.copy(),
    )

    datedim = CachedDimension(
        name="date",
        key="dateid",
        attributes=["date", "day", "month", "year", "week", "weekyear"],
        lookupatts=["date"],
        rowexpander=datehandling,
        prefill=True,
        targetconnection=shrdconn.copy(),
    )

    facttbl = DecoupledFactTable(
        BulkFactTable(
            name="testresults",
            keyrefs=["pageid", "testid", "dateid"],
            measures=["errors"],
            bulksize=250000,
            bulkloader=shrdconn.copy().pgcopybulkloader,
            usefilename=True,
        ),
        batchsize=BATCHSIZE,
        queuesize=10,
        consumes=pagedim.parts,
        returnvalues=False,
    )

    source_connection_args = {
        "host": source_host,
        "port": source_port,
        "dbname": source_database,
        "user": username,
    }

    return shrdconn, source_connection_args, pagedim, datedim, testdim, facttbl


async def run_etl(shrdconn, source_connection_args, pagedim, datedim, testdim, facttbl):
    sourceconn1 = None
    sourceconn2 = None
    try:
        sourceconn1 = await psycopg.AsyncConnection.connect(**source_connection_args)
        sourceconn2 = await psycopg.AsyncConnection.connect(**source_connection_args)

        prefetch = get_prefetch()
        downloadlog = AsyncSQLSource(
            connection=sourceconn1,
            query="""
                SELECT localfile, url, serverversion, size::text AS size,
                       downloaddate::text AS downloaddate,
                       lastmoddate::text AS lastmoddate
                FROM downloadlog
                ORDER BY localfile
            """,
            cursorarg="downloadlog_cursor",
            fetchsize=5000,
            prefetch=prefetch,
        )

        testresults = AsyncSQLSource(
            connection=sourceconn2,
            query="""
                SELECT localfile, test, errors::text AS errors
                FROM testresults
                ORDER BY localfile, test
            """,
            cursorarg="testresults_cursor",
            fetchsize=5000,
            prefetch=prefetch,
        )

        joined = AsyncMergeJoiningSource(
            downloadlog, "localfile", testresults, "localfile"
        )

        async for row in joined:
            extractdomaininfo(row)
            extractserverinfo(row)
            row["size"] = pygrametl.getint(row["size"])

            fact = {"errors": row["errors"]}
            fact["pageid"] = pagedim.scdensure(row)
            fact["dateid"] = datedim.ensure(row, {"date": "downloaddate"})
            fact["testid"] = testdim.lookup(row, {"testname": "test"})
            facttbl.insert(fact)

        # Ask pygrametl to flush registered table buffers and commit.
        shrdconn.commit()

        # These are Decoupled workers, not database connections: they expose
        # shutdowndecoupled(), not close(). Finish the downstream consumer
        # before the page-dimension workers whose FutureResults it consumes.
        facttbl.shutdowndecoupled()
        for dimension in pagedim.parts:
            dimension.shutdowndecoupled()

    finally:
        if sourceconn1 is not None:
            await sourceconn1.close()
        if sourceconn2 is not None:
            await sourceconn2.close()
        shrdconn.close()


def main():
    etl = create_etl()
    asyncio.run(run_etl(*etl))


if __name__ == "__main__":
    main()
