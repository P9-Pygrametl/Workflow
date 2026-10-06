import psycopg
import os
from dotenv import load_dotenv 
load_dotenv() 

from pygrametl import ConnectionWrapper
from pygrametl.datasources import CSVSource, MergeJoiningSource, ProcessSource,\
    TransformingSource
from pygrametl.tables import CachedDimension,\
    SlowlyChangingDimension, BulkFactTable, \
    DecoupledDimension, DecoupledFactTable, DimensionPartitioner
from pygrametl.parallel import shareconnectionwrapper,\
     getsharedsequencefactory

from helpers.helpers import datehandling, extractdomaininfo, \
    extractserverinfo, convertsize


pgconn = psycopg.connect(
    host="localhost",
    dbname=os.getenv("DW_DATABASE"),
    user=os.getenv("USERNAME"),
)

BATCHSIZE = 500


# Methods
def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filename):
    sql = (
        f"COPY {name}({', '.join(atts)}) FROM STDIN "
        f"WITH (FORMAT text, DELIMITER '{fieldsep}', NULL '{nullval}')"
    )
    with open(filename, 'rb') as f, pgconn.cursor().copy(sql) as copy:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            copy.write(chunk)

pgconn.autocommit = False

shrdconn = shareconnectionwrapper(ConnectionWrapper(pgconn), 10,
                                   (pgcopybulkloader,))
shrdconn.execute('set search_path to pygrametlexa')

# Dimension and fact table objects

def getpagediminstances():
    global shrdconn
    idfactory = getsharedsequencefactory(0)
    for i in range(2):
        yield DecoupledDimension(
            SlowlyChangingDimension(
            name='page', 
            key='pageid',
            attributes=['url', 'size', 'domain', 'topleveldomain', 
                        'serverversion', 'server', 
                        'validfrom', 'validto', 'version'],
            lookupatts=['url'],
            versionatt='version', 
            fromatt='validfrom',
            toatt='validto', 
            srcdateatt='lastmoddate', 
            cachesize=-1,
            prefill=True,
            idfinder=idfactory(),
            targetconnection=shrdconn.copy()),
            batchsize=BATCHSIZE, queuesize=10
            )

pagedim = DimensionPartitioner([pd for pd in getpagediminstances()])

testdim = CachedDimension(
    name='test', 
    key='testid',
    attributes=['testname', 'testauthor'],
    lookupatts=['testname'], 
    prefill=True,
    defaultidvalue=-1,
    targetconnection=shrdconn.copy())

datedim = CachedDimension(
    name='date', 
    key='dateid',
    attributes=['date', 'day', 'month', 'year', 'week', 'weekyear'],
    lookupatts=['date'], 
    rowexpander=datehandling,
    prefill=True,
    targetconnection=shrdconn.copy())

facttbl = DecoupledFactTable(
    BulkFactTable(
        name='testresults', 
        keyrefs=['pageid', 'testid', 'dateid'],
        measures=['errors'], 
        bulksize=250000,
        bulkloader=shrdconn.copy().pgcopybulkloader,
        targetconnection=None,  # The target connection is not used in the bulkloader function
        usefilename=True),
    batchsize=BATCHSIZE, queuesize=10,
    consumes=pagedim.parts,
    returnvalues=False
    )


# Data sources - change the path if you have your files somewhere else
downloadlog = CSVSource(open('DownloadLog.csv', 'r'),
                        delimiter='\t')
    

testresults = CSVSource(open('TestResults.csv', 'r'),
                        delimiter='\t')


joineddata = MergeJoiningSource(downloadlog, 'localfile', testresults,
                                'localfile')

transformeddata = TransformingSource(joineddata, extractdomaininfo, 
                                     extractserverinfo, convertsize)


inputdata = ProcessSource(transformeddata, batchsize=BATCHSIZE, queuesize=10)

def main():
    for row in inputdata:
        fact = {'errors':row['errors']}
        fact['pageid'] = pagedim.scdensure(row)
        fact['dateid'] = datedim.ensure(row, {'date':'downloaddate'})
        fact['testid'] = testdim.lookup(row, {'testname':'test'})
        facttbl.insert(fact)
    shrdconn.commit()

if __name__ == '__main__':
    main()
