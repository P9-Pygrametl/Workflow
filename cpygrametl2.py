import datetime
import time
import psycopg
import pygrametl
import os
from dotenv import load_dotenv, dotenv_values 
load_dotenv() 

from pygrametl import ConnectionWrapper
from pygrametl.datasources import CSVSource, MergeJoiningSource, ProcessSource,\
    TransformingSource
from pygrametl.tables import CachedDimension,\
    SlowlyChangingDimension, BulkFactTable, \
    DecoupledDimension, DecoupledFactTable, DimensionPartitioner
from pygrametl.parallel import shareconnectionwrapper,\
     getsharedsequencefactory


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



def datehandling(row, namemapping):
    # This method is called from ensure(row) when the lookup of a date fails.
    # We have to calculate all date related fields and add them to the row.
    date = pygrametl.getvalue(row, 'date', namemapping)
    (year, month, day, hour, minute, second, weekday, dayinyear, dst) = \
        time.strptime(date, "%Y-%m-%d")
    (isoyear, isoweek, isoweekday) = \
        datetime.date(year, month, day).isocalendar()
    # We could use row[namemapping.get('day') or 'day'] = X to support name map.
    row['day'] = day
    row['month'] = month
    row['year'] = year
    row['week'] = isoweek
    row['weekyear'] = isoyear
    row['dateid'] = dayinyear + 366 * (year - 1990) #Allow dates from 1990-01-01
    return row
    

def extractdomaininfo(row):
    # Take the 'www.domain.org' part from 'http://www.domain.org/page.html'
    # We also the host name ('www') in the domain in this example.
    domaininfo = row['url'].split('/')[-2]
    row['domain'] = domaininfo
    # Take the top level which is the last part of the domain
    row['topleveldomain'] = domaininfo.split('.')[-1]

def extractserverinfo(row):
    # Find the server name from a string like "ServerName/Version"
    row['server'] = row['serverversion'].split('/')[0]


def convertsize(row):
    row['size'] = pygrametl.getint(row['size'])

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
