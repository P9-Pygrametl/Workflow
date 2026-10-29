import asyncio
import os

from dotenv import load_dotenv
import psycopg
import pygrametl

load_dotenv()

from pygrametl import ConnectionWrapper
from pygrametl.datasources import (
    AsyncBackgroundSource,
    AsyncMergeJoiningSource,
    AsyncSQLSource,
    AsyncTransformingSource,
)
from pygrametl.parallel import (
    getsharedsequencefactory,
    shareconnectionwrapper,
)
from pygrametl.tables import (
    BulkFactTable,
    CachedDimension,
    DecoupledDimension,
    DecoupledFactTable,
    DimensionPartitioner,
    SlowlyChangingDimension,
)

from helpers.helpers import (
    convertsize,
    datehandling,
    extractdomaininfo,
    extractserverinfo,
)

BATCHSIZE = 500


def create_etl():
    username = os.getenv("USERNAME")
    source_database = os.getenv("SOURCE_DATABASE")
    dw_database = os.getenv("DW_DATABASE")
    source_host = os.getenv("SOURCE_HOST", "localhost")
    source_port_value = os.getenv("SOURCE_PORT", "5432")
    dw_host = os.getenv("DW_HOST", "localhost")
    dw_port_value = os.getenv("DW_PORT", "5432")

    try:
        source_port = int(source_port_value)
    except ValueError:
        raise ValueError(
            f"SOURCE_PORT must be an integer, got {source_port_value!r}"
        )

    try:
        dw_port = int(dw_port_value)
    except ValueError:
        raise ValueError(f"DW_PORT must be an integer, got {dw_port_value!r}")

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
        with open(filename, "rb") as f, pgconn.cursor().copy(sql) as copy:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                copy.write(chunk)

    shrdconn = shareconnectionwrapper(
        ConnectionWrapper(pgconn), 10, (pgcopybulkloader,)
    )
    shrdconn.execute("set search_path to pygrametlexa")

    # Create all multiprocessing-backed target objects before asyncio starts.
    # pygrametl.parallel uses fork, so this avoids forking a running event loop
    # or already-open async source connections.
    idfactory = getsharedsequencefactory(0)

    def getpagediminstances():
        for i in range(2):
            yield DecoupledDimension(
                SlowlyChangingDimension(
                    name="page",
                    key="pageid",
                    attributes=[
                        "url",
                        "size",
                        "domain",
                        "topleveldomain",
                        "serverversion",
                        "server",
                        "validfrom",
                        "validto",
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

    pagedim = DimensionPartitioner([pd for pd in getpagediminstances()])

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

    return (
        shrdconn,
        source_connection_args,
        pagedim,
        datedim,
        testdim,
        facttbl,
    )


async def run_etl(
    shrdconn,
    source_connection_args,
    pagedim,
    datedim,
    testdim,
    facttbl,
    get_int=pygrametl.getint,
):
    sourceconn1 = await psycopg.AsyncConnection.connect(**source_connection_args)
    sourceconn2 = await psycopg.AsyncConnection.connect(**source_connection_args)

    try:
        downloadlog = AsyncBackgroundSource(
            AsyncSQLSource(
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
            ),
            batchsize=BATCHSIZE,
            queuesize=10,
        )

        testresults = AsyncBackgroundSource(
            AsyncSQLSource(
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
            ),
            batchsize=BATCHSIZE,
            queuesize=10,
        )

        joineddata = AsyncMergeJoiningSource(
            downloadlog, "localfile", testresults, "localfile"
        )

        transformeddata = AsyncTransformingSource(
            joineddata,
            extractdomaininfo,
            extractserverinfo,
            convertsize,
        )

        # Async counterpart to the previous ProcessSource stage. Unlike a
        # process, this is cooperative concurrency and therefore only runs while
        # the event loop is allowed to make progress.
        inputdata = AsyncBackgroundSource(
            transformeddata,
            batchsize=BATCHSIZE,
            queuesize=10,
        )

        async for row in inputdata:
            row["size"] = get_int(row["size"])

            fact = {"errors": row["errors"]}
            fact["pageid"] = pagedim.scdensure(row)
            fact["dateid"] = datedim.ensure(row, {"date": "downloaddate"})
            fact["testid"] = testdim.lookup(row, {"testname": "test"})
            facttbl.insert(fact)

        shrdconn.commit()
    finally:
        # Explicitly close and join the processes of the decoupled tables
        # before closing the core database connections.
        try:
            pagedim.close()
        except Exception:
            pass

        try:
            facttbl.close()
        except Exception:
            pass

        await sourceconn1.close()
        await sourceconn2.close()
        shrdconn.close()


def main():
    # All fork-based target-side setup happens before asyncio.run().
    etl = create_etl()
    asyncio.run(run_etl(*etl))


if __name__ == "__main__":
    main()
