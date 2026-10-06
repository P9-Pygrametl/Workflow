import os
import time

import psycopg
import pygrametl
from dotenv import load_dotenv

from pygrametl import ConnectionWrapper
from pygrametl.datasources import MergeJoiningSource, SQLSource
from pygrametl.tables import (
    BulkFactTable,
    CachedDimension,
    SlowlyChangingDimension,
)

from helpers.helpers import (
    datehandling,
    extractdomaininfo,
    extractserverinfo,
)

load_dotenv()

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
        raise ValueError(
            f"DW_PORT must be an integer, got {dw_port_value!r}"
        )

    # Connection to target DW
    pgconn = psycopg.connect(
        host=dw_host,
        port=dw_port,
        dbname=dw_database,
        user=username,
    )

    

    def pgcopybulkloader(
            name,
            atts,
            fieldsep,
            rowsep,
            nullval,
            filehandle,
        ):
            sql = (
                f"COPY {name}({', '.join(atts)}) FROM STDIN "
                f"WITH (FORMAT text, DELIMITER '{fieldsep}', NULL '{nullval}')"
            )
            raw = getattr(filehandle, "buffer", filehandle)  # binary if possible
            with pgconn.cursor().copy(sql) as copy:
                for chunk in iter(lambda: raw.read(1 << 20), b""):
                    copy.write(chunk)

    connection = ConnectionWrapper(pgconn)
    connection.setasdefault()
    connection.execute("SET search_path TO pygrametlexa")

    # Connections to source database
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

    

    # Dimension and fact table objects
    pagedim = SlowlyChangingDimension(
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
        attributes=[
            "date",
            "day",
            "month",
            "year",
            "week",
            "weekyear",
        ],
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

    # Data sources
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

    inputdata = MergeJoiningSource(
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


def run_etl(
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
    print(time.asctime())

    try:
        for row in inputdata:
            extract_domain(row)
            extract_server(row)

            row["size"] = get_int(row["size"])

            row["pageid"] = pagedim.scdensure(row)
            row["dateid"] = datedim.ensure(
                row,
                {"date": "downloaddate"},
            )
            row["testid"] = testdim.lookup(
                row,
                {"testname": "test"},
            )

            facttbl.insert(row)

        connection.commit()

    finally:
        sourceconn1.close()
        sourceconn2.close()
        connection.close()

    print(time.asctime())


def main():
    etl = create_etl()
    run_etl(*etl)


if __name__ == "__main__":
    main()