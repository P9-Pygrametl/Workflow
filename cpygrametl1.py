import time
import psycopg
import pygrametl
import os
from dotenv import load_dotenv
load_dotenv() 

from pygrametl import ConnectionWrapper
from pygrametl.datasources import CSVSource, MergeJoiningSource
from pygrametl.tables import CachedDimension, SnowflakedDimension,\
    SlowlyChangingDimension, BulkFactTable
from helpers.helpers import datehandling, extractdomaininfo, extractserverinfo, pgcopybulkloader

pgconn = psycopg.connect(
    host="localhost",
    dbname=os.getenv("DW_DATABASE"),
    user=os.getenv("USERNAME"),
)
connection = ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to pygrametlexa')

# Dimension and fact table objects
pagedim = SlowlyChangingDimension(
    name='page', 
    key='pageid',
    attributes=['url', 'size', 'domain', 'topleveldomain', 'serverversion',
                'server', 'validfrom', 'validto', 'version'],
    lookupatts=['url'],
    versionatt='version', 
    fromatt='validfrom',
    toatt='validto', 
    srcdateatt='lastmoddate',  
    cachesize=-1,
    prefill=True)


testdim = CachedDimension(
    name='test', 
    key='testid',
    attributes=['testname', 'testauthor'],
    lookupatts=['testname'], 
    prefill=True, 
    defaultidvalue=-1)

datedim = CachedDimension(
    name='date', 
    key='dateid',
    attributes=['date', 'day', 'month', 'year', 'week', 'weekyear'],
    lookupatts=['date'],
    rowexpander=datehandling,
    prefill=True)

facttbl = BulkFactTable(
    name='testresults', 
    keyrefs=['pageid', 'testid', 'dateid'],
    measures=['errors'], 
    bulkloader=pgcopybulkloader,
    targetconnection=pgconn,
    bulksize=250000)


# Data sources
downloadlog = CSVSource(open('DownloadLog.csv', 'r', 16384),
                        delimiter='\t')

testresults = CSVSource(open('TestResults.csv', 'r', 16384),
                        delimiter='\t')

inputdata = MergeJoiningSource(downloadlog, 'localfile', testresults,
                               'localfile')

def main():
    print (time.asctime())
    for row in inputdata:
        extractdomaininfo(row)
        extractserverinfo(row)
        row['size'] = pygrametl.getint(row['size']) # Convert to an int
        # Add the data to the dimension tables and the fact table
        row['pageid'] = pagedim.scdensure(row)
        row['dateid'] = datedim.ensure(row, {'date':'downloaddate'})
        row['testid'] = testdim.lookup(row, {'testname':'test'})
        facttbl.insert(row)
    connection.commit()
    connection.close()
    print (time.asctime())

if __name__ == '__main__':
    main()
