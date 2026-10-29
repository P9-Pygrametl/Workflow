import os

from dotenv import load_dotenv
import psycopg
import pygrametl

from pygrametl import PipelinedConnectionWrapper
from pygrametl.datasources import (
    MergeJoiningSource,
    ProcessSource,
    SQLSource,
    TransformingSource,
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

load_dotenv()

BATCHSIZE = 500

def pgcopybulkloader(
                name,
                atts,
                fieldsep,
                rowsep,
                nullval,
                filename,
            ):
    sql = (
        f"COPY {name}({', '.join(atts)}) FROM STDIN "
        f"WITH (FORMAT text, DELIMITER '{fieldsep}', NULL '{nullval}')"
    )
    conn = pygrametl.getdefaulttargetconnection()

    with open(filename, "rb") as f, conn.cursor().copy(sql) as copy:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            copy.write(chunk)

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
    except ValueError as exc:
        raise ValueError(
            f"SOURCE_PORT must be an integer, got {source_port_value!r}"
        ) from exc

    try:
        dw_port = int(dw_port_value)
    except ValueError as exc:
        raise ValueError(
            f"DW_PORT must be an integer, got {dw_port_value!r}"
        ) from exc

    pgconn = psycopg.connect(
        host=dw_host,
        port=dw_port,
        dbname=dw_database,
        user=username,
    )

    pipeline_wrapper = PipelinedConnectionWrapper(pgconn)
    shrdconn = shareconnectionwrapper(
        pipeline_wrapper, 10, (pgcopybulkloader,)
    )
    shrdconn.execute("set search_path to pygrametlexa")

    sourceconn1 = psycopg.connect(
        host=source_host,
        port=source_port,
        dbname=source_database,
        user=username,
    )

    sourceconn2 = psycopg.connect(
        host=source_host,
        port=source_port,
        dbname=source_database,
        user=username,
    )

    # Sequence factory is created local to this call run to ensure thread-safety
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

    downloadlog = SQLSource(
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
    )

    testresults = SQLSource(
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
    )

    joineddata = MergeJoiningSource(
        downloadlog, "localfile", testresults, "localfile"
    )

    transformeddata = TransformingSource(joineddata, extractdomaininfo, 
                                     extractserverinfo, convertsize)

    # ProcessSource reads in a separate process (or a thread when the GIL is off).
    inputdata = ProcessSource(transformeddata, batchsize=BATCHSIZE, queuesize=10)

    return (
        shrdconn,
        sourceconn1,
        sourceconn2,
        inputdata,
        pagedim,
        datedim,
        testdim,
        facttbl,
    )


def run_etl(
    shrdconn,
    sourceconn1,
    sourceconn2,
    inputdata,
    pagedim,
    datedim,
    testdim,
    facttbl,
    get_int=pygrametl.getint,
):
    try:
        for row in inputdata:
            row["size"] = get_int(row["size"])

            fact = {"errors": row["errors"]}
            fact["pageid"] = pagedim.scdensure(row)
            fact["dateid"] = datedim.ensure(row, {"date": "downloaddate"})
            fact["testid"] = testdim.lookup(row, {"testname": "test"})
            facttbl.insert(fact)
    finally:
        # Stop the decoupled workers before closing the connections.
        for part in pagedim.parts:
            part.shutdowndecoupled()
        facttbl.shutdowndecoupled()

        # Commit the transaction
        shrdconn.commit()

        # Close all data sources and connections
        sourceconn1.close()
        sourceconn2.close()
        shrdconn.close()


def main():
    etl = create_etl()
    run_etl(*etl)


if __name__ == "__main__":
    main()
