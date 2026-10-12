import datetime
import os
import time

import psycopg
import pygrametl
from dotenv import load_dotenv

from pygrametl.datasources import MergeJoiningSource, SQLSource
from pygrametl.tables import (
    BulkFactTable,
    CachedDimension,
    SlowlyChangingDimension,
)

load_dotenv()


class PipelinedConnectionWrapper(pygrametl.ConnectionWrapper):
    def __init__(self, connection, **kw):
        super().__init__(connection, **kw)
        self._ctx = connection.pipeline()
        self._pipe = self._ctx.__enter__()

    def execute(self, stmt, *a, **k):
        super().execute(stmt, *a, **k)
        if stmt.lstrip()[:6].upper() == "SELECT":
            self._pipe.sync()

    def commit(self):
        self._pipe.sync()
        super().commit()

    def close(self):
        # Gracefully shut down the pipeline before closing the socket
        try:
            self._ctx.__exit__(None, None, None)
        except Exception:
            pass
        super().close()


def datehandling(row, namemapping):
    date = pygrametl.getvalue(row, "date", namemapping)
    (
        year, month, day, hour, minute, second, weekday, dayinyear, dst,
    ) = time.strptime(date, "%Y-%m-%d")

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
        dw_port = int(dw_port_value)
    except ValueError as e:
        raise ValueError(f"Port must be an integer: {e}")

    # Connection to target DW
    pgconn = psycopg.connect(
        host=dw_host, port=dw_port, dbname=dw_database, user=username,
    )

    # Use the Pipelined Wrapper instead of the standard ConnectionWrapper
    connection = PipelinedConnectionWrapper(pgconn)
    connection.setasdefault()
    connection.execute("SET search_path TO pygrametlexa")

    # Connections to source database
    sourceconn1 = psycopg.connect(
        host=source_host, port=source_port, dbname=source_database, user=username,
    )
    sourceconn2 = psycopg.connect(
        host=source_host, port=source_port, dbname=source_database, user=username,
    )

    def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filehandle):
        # 1. Fully exit pipeline mode (this automatically syncs pending writes)
        connection._ctx.__exit__(None, None, None)
        
        sql = (
            f"COPY {name}({', '.join(atts)}) FROM STDIN "
            f"WITH (FORMAT text, DELIMITER '{fieldsep}', NULL '{nullval}')"
        )
        raw = getattr(filehandle, "buffer", filehandle)
        with pgconn.cursor().copy(sql) as copy:
            for chunk in iter(lambda: raw.read(1 << 20), b""):
                copy.write(chunk)
                
        # 2. Re-enter pipeline mode for any subsequent dimension writes
        connection._ctx = pgconn.pipeline()
        connection._pipe = connection._ctx.__enter__()

    pagedim = SlowlyChangingDimension(
        name="page",
        key="pageid",
        attributes=[
            "url", "size", "domain", "topleveldomain",
            "serverversion", "server", "validfrom", "validto", "version",
        ],
        lookupatts=["url"],
        versionatt="version",
        fromatt="validfrom",
        toatt="validto",
        srcdateatt="lastmoddate",
        cachesize=1000,
        prefill=False,
    )

    testdim = CachedDimension(
        name="test",
        key="testid",
        attributes=["testname", "testauthor"],
        lookupatts=["testname"],
        defaultidvalue=-1,
    )

    datedim = CachedDimension(
        name="date",
        key="dateid",
        attributes=["date", "day", "month", "year", "week", "weekyear"],
        lookupatts=["date"],
        rowexpander=datehandling,
    )

    facttbl = BulkFactTable(
        name="testresults",
        keyrefs=["pageid", "testid", "dateid"],
        measures=["errors"],
        bulkloader=pgcopybulkloader,
        bulksize=250000,
    )

    downloadlog = SQLSource(
        connection=sourceconn1,
        query="""
            SELECT localfile, url, serverversion, size::text AS size,
                   downloaddate::text AS downloaddate, lastmoddate::text AS lastmoddate
            FROM downloadlog ORDER BY localfile
        """,
        cursorarg="downloadlog_cursor",
        fetchsize=5000,
    )

    testresults = SQLSource(
        connection=sourceconn2,
        query="""
            SELECT localfile, test, errors::text AS errors
            FROM testresults ORDER BY localfile, test
        """,
        cursorarg="testresults_cursor",
        fetchsize=5000,
    )

    inputdata = MergeJoiningSource(downloadlog, "localfile", testresults, "localfile")

    return (
        connection, sourceconn1, sourceconn2, inputdata, pagedim, datedim, testdim, facttbl,
    )


def run_etl(
    connection, sourceconn1, sourceconn2, inputdata, pagedim, datedim, testdim, facttbl,
    extract_domain=extractdomaininfo, extract_server=extractserverinfo, get_int=pygrametl.getint,
):
    print("Start:", time.asctime())

    try:
        for row in inputdata:
            extract_domain(row)
            extract_server(row)
            row["size"] = get_int(row["size"])

            row["pageid"] = pagedim.scdensure(row)
            row["dateid"] = datedim.ensure(row, {"date": "downloaddate"})
            row["testid"] = testdim.lookup(row, {"testname": "test"})

            facttbl.insert(row)

        connection.commit()

    finally:
        sourceconn1.close()
        sourceconn2.close()
        connection.close()

    print("End:", time.asctime())


def main():
    etl = create_etl()
    run_etl(*etl)


if __name__ == "__main__":
    main()