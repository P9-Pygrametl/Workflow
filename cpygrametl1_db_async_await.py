import asyncio
import datetime
import os
import time

import psycopg
import pygrametl
from dotenv import load_dotenv

from pygrametl.asyncconn import AsyncConnectionWrapper
from pygrametl.datasources import MergeJoiningSource, SQLSource
from pygrametl.asyncawaittables import (
    BulkFactTable,
    CachedDimension,
    SlowlyChangingDimension,
)


load_dotenv()


def datehandling(row, namemapping):
    # This method is called from ensure(row) when the lookup of a date fails.
    # We have to calculate all date related fields and add them to the row.
    date = pygrametl.getvalue(row, "date", namemapping)

    (
        year,
        month,
        day,
        hour,
        minute,
        second,
        weekday,
        dayinyear,
        dst,
    ) = time.strptime(date, "%Y-%m-%d")

    isoyear, isoweek, isoweekday = datetime.date(
        year,
        month,
        day,
    ).isocalendar()

    row["day"] = day
    row["month"] = month
    row["year"] = year
    row["week"] = isoweek
    row["weekyear"] = isoyear
    row["dateid"] = dayinyear + 366 * (year - 1990)

    return row


def extractdomaininfo(row):
    # Take the 'www.domain.org' part from 'http://www.domain.org/page.html'.
    domaininfo = row["url"].split("/")[-2]
    row["domain"] = domaininfo

    # Take the top level which is the last part of the domain.
    row["topleveldomain"] = domaininfo.split(".")[-1]


def extractserverinfo(row):
    # Find the server name from a string like "ServerName/Version".
    row["server"] = row["serverversion"].split("/")[0]


async def create_etl():
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
    connection = await AsyncConnectionWrapper.connect(
        host=dw_host,
        port=dw_port,
        dbname=dw_database,
        user=username,
    )
    await connection.execute("SET search_path TO pygrametlexa")

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

    async def pgcopybulkloader(
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
        await connection.copyfrom(sql, raw, chunksize=1 << 20)

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
        prefill=False,
        targetconnection=connection,
    )

    testdim = CachedDimension(
        name="test",
        key="testid",
        attributes=["testname", "testauthor"],
        lookupatts=["testname"],
        prefill=False,
        defaultidvalue=-1,
        targetconnection=connection,
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
        prefill=False,
        targetconnection=connection,
    )

    facttbl = BulkFactTable(
        name="testresults",
        keyrefs=["pageid", "testid", "dateid"],
        measures=["errors"],
        bulkloader=pgcopybulkloader,
        bulksize=250000,
        targetconnection=connection,
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
    print(time.asctime())

    try:
        for row in inputdata:
            extract_domain(row)
            extract_server(row)

            row["size"] = get_int(row["size"])

            row["pageid"] = await pagedim.scdensure(row)
            row["dateid"] = await datedim.ensure(
                row,
                {"date": "downloaddate"},
            )
            row["testid"] = await testdim.lookup(
                row,
                {"testname": "test"},
            )

            await facttbl.insert(row)

        await connection.commit()

    finally:
        sourceconn1.close()
        sourceconn2.close()
        await connection.close()

    print(time.asctime())


async def async_main():
    etl = await create_etl()
    await run_etl(*etl)


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
