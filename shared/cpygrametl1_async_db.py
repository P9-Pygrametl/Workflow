import asyncio
import os

import psycopg
import pygrametl
from dotenv import load_dotenv

from pygrametl import ConnectionWrapper
from pygrametl.datasources import (
    AsyncMergeJoiningSource,
    AsyncSQLSource,
)
from pygrametl.tables import (
    BulkFactTable,
    CachedDimension,
    SlowlyChangingDimension,
    PostgreSQLOptimizedSCD,
)

from helpers.helpers import (
    datehandling,
    extractdomaininfo,
    extractserverinfo,
)

load_dotenv()


def get_prefetch():
    """Read the number of prefetched batches per asynchronous SQL source."""
    raw = os.getenv("PREFETCH", "0")
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"PREFETCH must be an integer, got {raw!r}") from error
    if value < 0:
        raise ValueError(f"PREFETCH must be non-negative, got {value}")
    return value


async def create_etl(prefetch=None):
    """Construct the ETL pipeline, optionally prefetching SQL result batches.

    prefetch=0 preserves the original async source behaviour. If omitted,
    the PREFETCH environment variable is used (default: 0).
    """
    if prefetch is None:
        prefetch = get_prefetch()
    if not isinstance(prefetch, int) or prefetch < 0:
        raise ValueError("prefetch must be a non-negative integer")

    username = os.getenv("USERNAME")
    source_database = os.getenv("SOURCE_DATABASE")
    dw_database = os.getenv("DW_DATABASE")
    source_host = os.getenv("SOURCE_HOST", "localhost")
    source_port_value = os.getenv("SOURCE_PORT", "5432")
    dw_host = os.getenv("DW_HOST", "localhost")
    dw_port_value = os.getenv("DW_PORT", "5432")

    try:
        source_port = int(source_port_value)
    except ValueError as error:
        raise ValueError(
            f"SOURCE_PORT must be an integer, got {source_port_value!r}"
        ) from error

    try:
        dw_port = int(dw_port_value)
    except ValueError as error:
        raise ValueError(
            f"DW_PORT must be an integer, got {dw_port_value!r}"
        ) from error

    # Connection to target DW (remains synchronous).
    pgconn = psycopg.connect(
        host=dw_host,
        port=dw_port,
        dbname=dw_database,
        user=username,
    )

    def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filehandle):
        sql = (
            f"COPY {name}({', '.join(atts)}) FROM STDIN "
            f"WITH (FORMAT text, DELIMITER '{fieldsep}', NULL '{nullval}')"
        )
        raw = getattr(filehandle, "buffer", filehandle)
        with pgconn.cursor().copy(sql) as copy:
            for chunk in iter(lambda: raw.read(1 << 20), b""):
                copy.write(chunk)

    connection = ConnectionWrapper(pgconn)
    connection.setasdefault()
    connection.execute("SET search_path TO pygrametlexa")

    # Asynchronous connections to the source database.
    sourceconn1 = await psycopg.AsyncConnection.connect(
        host=source_host,
        port=source_port,
        dbname=source_database,
        user=username,
    )
    sourceconn2 = await psycopg.AsyncConnection.connect(
        host=source_host,
        port=source_port,
        dbname=source_database,
        user=username,
    )
    
    scd_mode = os.getenv("SCD_MODE", "original").lower()

    if scd_mode == "original":
        dimension_class = SlowlyChangingDimension
    elif scd_mode == "optimized":
        dimension_class = PostgreSQLOptimizedSCD
    else:
        raise ValueError(
            f"Unsupported SCD_MODE: {scd_mode!r}"
        )

    pagedim = dimension_class(
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

    # A positive prefetch depth buffers that many batches per source.
    downloadlog = AsyncSQLSource(
        connection=sourceconn1,
        query="""
            SELECT
                localfile,
                url,
                serverversion,
                size::text AS size,
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
            SELECT
                localfile,
                test,
                errors::text AS errors
            FROM testresults
            ORDER BY localfile, test
        """,
        cursorarg="testresults_cursor",
        fetchsize=5000,
        prefetch=prefetch,
    )

    inputdata = AsyncMergeJoiningSource(
        downloadlog,
        "localfile",
        testresults,
        "localfile",
    )

    return (
        connection,
        sourceconn1,
        sourceconn2,
        inputdata,
        pagedim,
        datedim,
        testdim,
        facttbl,
    )


async def run_etl(
    connection,
    sourceconn1,
    sourceconn2,
    inputdata,
    pagedim,
    datedim,
    testdim,
    facttbl,
    extract_domain=extractdomaininfo,
    extract_server=extractserverinfo,
    get_int=pygrametl.getint,
):
    try:
        async for row in inputdata:
            extract_domain(row)
            extract_server(row)
            row["size"] = get_int(row["size"])
            row["pageid"] = pagedim.scdensure(row)
            row["dateid"] = datedim.ensure(row, {"date": "downloaddate"})
            row["testid"] = testdim.lookup(row, {"testname": "test"})
            facttbl.insert(row)

        connection.commit()
    finally:
        await sourceconn1.close()
        await sourceconn2.close()
        connection.close()


async def main():
    etl = await create_etl()
    await run_etl(*etl)


if __name__ == "__main__":
    asyncio.run(main())
