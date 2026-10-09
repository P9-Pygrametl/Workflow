import os
from pathlib import Path
import sys
import time

from dotenv import load_dotenv
import psycopg
import pygrametl

ROOT = Path(__file__).resolve().parent.parent

# Allow imports from the repository root when running this file directly.
sys.path.insert(0, str(ROOT))

from pygrametl import PicklableConnectionWrapper
from pygrametl.datasources import (
    CSVSource,
    MergeJoiningSource,
    ProcessSource,
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

BATCHSIZE = 500
_connRef = []
load_dotenv()


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
    dw_database = os.getenv("DW_DATABASE")
    dw_host = os.getenv("DW_HOST", "localhost")
    dw_port_value = os.getenv("DW_PORT", "5432")

    try:
        dw_port = int(dw_port_value)
    except ValueError:
        raise ValueError(f"DW_PORT must be an integer, got {dw_port_value!r}")

    # Connection to target DW
    pgconn = psycopg.connect(
        host=dw_host,
        port=dw_port,
        dbname=dw_database,
        user=username,
    )
    picklable_connection = PicklableConnectionWrapper(pgconn)
    _connRef.append(picklable_connection)

    shrdconn = shareconnectionwrapper(
        picklable_connection, 10, (pgcopybulkloader,)
    )
    shrdconn.execute("set search_path to pygrametlexa")

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

    downloadlog = CSVSource(open("DownloadLog.csv", "r", 16384), delimiter="\t")

    testresults = CSVSource(open("TestResults.csv", "r", 16384), delimiter="\t")

    joineddata = MergeJoiningSource(
        downloadlog, "localfile", testresults, "localfile"
    )

    transformeddata = TransformingSource(
        joineddata, extractdomaininfo, extractserverinfo, convertsize
    )

    inputdata = ProcessSource(
        transformeddata, batchsize=BATCHSIZE, queuesize=10
    )

    def export_dw_to_csv():
        tables = ["page", "test", "date", "testresults"]
        print("Exporting DW tables to CSV...")
        for table in tables:
            filename = f"./nogilCSV/{table}_export.csv"
            sql = f"COPY pygrametlexa.{table} TO STDOUT WITH (FORMAT CSV, HEADER)"
            with pgconn.cursor().copy(sql) as copy:
                with open(filename, "wb") as f:
                    for chunk in copy:
                        f.write(chunk)
            print(f"Saved: {filename}")

    return (
        shrdconn,
        inputdata,
        pagedim,
        datedim,
        testdim,
        facttbl,
        export_dw_to_csv,
    )


def run_etl(
    shrdconn,
    inputdata,
    pagedim,
    datedim,
    testdim,
    facttbl,
    export_dw_to_csv,
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

            # Use a separate dictionary for the fact record
            fact = {"errors": row["errors"]}
            fact["pageid"] = pagedim.scdensure(row)
            fact["dateid"] = datedim.ensure(
                row,
                {"date": "downloaddate"},
            )
            fact["testid"] = testdim.lookup(
                row,
                {"testname": "test"},
            )

            facttbl.insert(fact)

        shrdconn.commit()

    finally:
        # Flush and join decoupled dimension/fact table workers first
        try:
            pagedim.close()
        except Exception:
            pass

        try:
            facttbl.close()
        except Exception:
            pass

        try:
            datedim.close()
        except Exception:
            pass

        try:
            testdim.close()
        except Exception:
            pass

        export_dw_to_csv()
        shrdconn.close()

    print(time.asctime())


def main():
    etl = create_etl()
    run_etl(*etl)


if __name__ == "__main__":
    main()