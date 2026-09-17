import datetime
import time
import psycopg2
import pygrametl
import os
from dotenv import load_dotenv, dotenv_values 
load_dotenv() 

from pygrametl import ConnectionWrapper
from pygrametl.datasources import CSVSource, MergeJoiningSource
from pygrametl.tables import CachedDimension, SnowflakedDimension,\
    SlowlyChangingDimension, BulkFactTable

pgconn = psycopg2.connect(
    host="localhost",
    dbname=os.getenv("DW_DATABASE"),
    user=os.getenv("USERNAME"),
)
connection = ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to pygrametlexa')

# Methods
def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filehandle):
    cursor = pgconn.cursor()
    cursor.copy_from(file=filehandle, table=name, sep=fieldsep,
                     null=str(nullval), columns=atts)

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
