import time
import psycopg
import pygrametl
import os
from dotenv import load_dotenv
import sys
from pathlib import Path

from pygrametl import ConnectionWrapper
from pygrametl.datasources import CSVSource, MergeJoiningSource
from pygrametl.tables import CachedDimension,\
    SlowlyChangingDimension, BulkFactTable

ROOT = Path(__file__).resolve().parent.parent

# Allow imports from the repository root when running this file directly.
sys.path.insert(0, str(ROOT))

from helpers.helpers import (
    datehandling,
    extractdomaininfo,
    extractserverinfo,
)

load_dotenv()

def create_etl():
    username = os.getenv("USERNAME")
    dw_database = os.getenv("DW_DATABASE")
    dw_host = os.getenv("DW_HOST", "localhost")
    dw_port_value = os.getenv("DW_PORT", "5432")

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

    downloadlog = CSVSource(open('DownloadLog.csv', 'r', 16384),
                        delimiter='\t')

    testresults = CSVSource(open('TestResults.csv', 'r', 16384),
                        delimiter='\t')


    inputdata = MergeJoiningSource(
        downloadlog,
        "localfile",
        testresults,
        "localfile",
    )

    def export_dw_to_csv():
        tables = ['page', 'test', 'date', 'testresults']
        print("Exporting DW tables to CSV...")
        for table in tables:
            filename = f"./baselineCSV/{table}_export.csv"
            sql = f"COPY pygrametlexa.{table} TO STDOUT WITH (FORMAT CSV, HEADER)"
            with pgconn.cursor().copy(sql) as copy:
                with open(filename, "wb") as f:
                    for chunk in copy:
                        f.write(chunk)
            print(f"Saved: {filename}")

    return (
        connection,
        inputdata,
        pagedim,
        datedim,
        testdim,
        facttbl,
        export_dw_to_csv,
    )



def run_etl(
    connection,
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
        connection.close()

    print(time.asctime())


def main():
    etl = create_etl()
    run_etl(*etl)


if __name__ == "__main__":
    main()