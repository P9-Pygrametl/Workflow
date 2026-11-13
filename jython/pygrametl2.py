#  Copyright (c) 2011 Christian Thomsen (chr@cs.aau.dk)
#
#  This file is free software: you may copy, redistribute and/or modify it
#  under the terms of the GNU General Public License version 2
#  as published by the Free Software Foundation.
#
#  This file is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


import datetime
import os
import sys
import time


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.helpers import (
    convertsize,
    datehandling,
    extractdomaininfo,
    extractserverinfo,
)
import pygrametl
from jython_dw import (
    downloadlog_source,
    loadenv,
    testresults_source,
)
from pygrametl.JDBCConnectionWrapper import JDBCConnectionWrapper
from pygrametl.datasources import MergeJoiningSource, ProcessSource, TransformingSource
from pygrametl.tables import (
    CachedDimension,
    SlowlyChangingDimension,
    BulkFactTable,
    DecoupledDimension,
    DecoupledFactTable,
    DimensionPartitioner,
)
from pygrametl.parallel import shareconnectionwrapper, getsharedsequencefactory

import java.lang


loadenv()


BATCHSIZE = int(os.environ.get("JYTHON_BATCHSIZE", "500"))
QUEUESIZE = int(os.environ.get("JYTHON_QUEUESIZE", "10"))
PAGE_PARTS = int(os.environ.get("JYTHON_PAGE_PARTS", "2"))
CACHESIZE = int(os.environ.get("JYTHON_CACHESIZE", "-1"))
PROCESS_BATCHSIZE = int(os.environ.get("JYTHON_PROCESS_BATCHSIZE", str(BATCHSIZE)))
PROCESS_QUEUESIZE = int(os.environ.get("JYTHON_PROCESS_QUEUESIZE", str(QUEUESIZE)))


def pgbulkloader(name, atts, fieldsep, rowsep, nullval, filename):
    sql = "COPY %s(%s) FROM STDIN WITH DELIMITER '%s'" % (
        name,
        ", ".join(atts),
        fieldsep,
    )
    global rawconn
    copymgr = rawconn.getCopyAPI()
    with open(filename, "r") as filehandle:
        copymgr.copyIn(sql, filehandle)


# Connection to target DW:
dwhost = os.environ.get("DW_HOST", "localhost")
dwport = os.environ.get("DW_PORT", "55433")
dwdb = os.environ.get("DW_DATABASE", "pygrametl_dw")
dwuser = os.environ.get("USERNAME")

java.lang.Class.forName("org.postgresql.Driver")
rawconn = java.sql.DriverManager.getConnection(
    "jdbc:postgresql://%s:%s/%s?user=%s" % (dwhost, dwport, dwdb, dwuser)
)
rawconn.setAutoCommit(False)
shrdconn = shareconnectionwrapper(JDBCConnectionWrapper(rawconn), 10, (pgbulkloader,))
shrdconn.execute("set search_path to pygrametlexa")


def getpagediminstances():
    idfactory = getsharedsequencefactory(0)
    for i in range(PAGE_PARTS):
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
                cachesize=CACHESIZE,
                prefill=True,
                idfinder=idfactory(),
                targetconnection=shrdconn.copy(),
            ),
            batchsize=BATCHSIZE,
            queuesize=QUEUESIZE,
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
        bulkloader=shrdconn.copy().pgbulkloader,
        usefilename=True,
    ),
    batchsize=BATCHSIZE,
    queuesize=QUEUESIZE,
    consumes=pagedim.parts,
    returnvalues=False,
)


# Data sources
downloadlog = downloadlog_source()
testresults = testresults_source()


joineddata = MergeJoiningSource(downloadlog, "localfile", testresults, "localfile")

transformeddata = TransformingSource(
    joineddata, extractdomaininfo, extractserverinfo, convertsize
)


inputdata = ProcessSource(
    transformeddata, batchsize=PROCESS_BATCHSIZE, queuesize=PROCESS_QUEUESIZE
)


def main():
    try:
        for row in inputdata:
            fact = {"errors": row["errors"]}
            fact["pageid"] = pagedim.scdensure(row)
            fact["dateid"] = datedim.ensure(row, {"date": "downloaddate"})
            fact["testid"] = testdim.lookup(row, {"testname": "test"})
            facttbl.insert(fact)
        shrdconn.commit()
    finally:
        # Jython runs pygrametl 2.6, where shutdown is explicit.
        try:
            facttbl.shutdowndecoupled()
            for part in pagedim.parts:
                part.shutdowndecoupled()
        finally:
            shrdconn.close()


if __name__ == "__main__":
    main()
