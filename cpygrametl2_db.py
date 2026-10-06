import datetime
import os
import time

from dotenv import load_dotenv
import psycopg
import pygrametl

load_dotenv()

from pygrametl import PicklableConnectionWrapper
from pygrametl.datasources import (
    MergeJoiningSource,
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
    datehandling,
    extractdomaininfo,
    extractserverinfo,
    convertsize,
)

BATCHSIZE = 500
_connRef = []


def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filename):
    connection = pygrametl.getdefaulttargetconnection()
    if connection is None:
        raise RuntimeError("DW connection wrapper is not initialized")

    sql = (
        f"COPY {name}({', '.join(atts)}) FROM STDIN "
        f"WITH (FORMAT text, DELIMITER '{fieldsep}', NULL '{nullval}')"
    )
    with open(filename, "rb") as f, connection.cursor().copy(sql) as copy:
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
    except ValueError:
        raise ValueError(
            f"SOURCE_PORT must be an integer, got {source_port_value!r}"
        )

    try:
        dw_port = int(dw_port_value)
    except ValueError:
        raise ValueError(f"DW_PORT must be an integer, got {dw_port_value!r}")

    pgconn = PicklableConnectionWrapper(
        {
            "host": dw_host,
            "port": dw_port,
            "dbname": dw_database,
            "user": username,
        }
    )
    _connRef.append(pgconn)

    shrdconn = shareconnectionwrapper(
        pgconn, 10, (pgcopybulkloader,)
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

    # ProcessSource will also spawn worker threads.
    inputdata = transformeddata

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
    extract_domain=extractdomaininfo,
    extract_server=extractserverinfo,
    get_int=pygrametl.getint,
):
    print(time.asctime())
    try:
        for row in inputdata:
            extract_domain(row)
            extract_server(row)
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

        # Close all data sources and connections
        sourceconn1.close()
        sourceconn2.close()
        shrdconn.close()

    print(time.asctime())


def main():
    etl = create_etl()
    run_etl(*etl)


if __name__ == "__main__":
    main()
