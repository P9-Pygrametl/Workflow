# A program for generating example data.

#  
#  Copyright (c) 2009-2011 Christian Thomsen (chr@cs.aau.dk)
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

import argparse
import importlib
import random

try:
    params = importlib.import_module("params")
except ModuleNotFoundError:
    params = None


def read_param(name, default):
    if params is None:
        return default

    return getattr(params, name, default)


toplevels = read_param("toplevels", 5)
domains = read_param("domains", 5)
pages = read_param("pages", 10)
months = read_param("months", 12)
changeprob = read_param("changeprob", 50)
startyear = read_param("startyear", 2008)
tests = read_param("tests", 5)

def generateurls():
    toplevellist = ["tl%d" % (i,) for i in range(toplevels)]
    domainlist = ["domain%d" % (i,) for i in range(domains)] # in each toplevel
    pagelist = ["page%d.html" % (i,) for i in range(pages)]# in each domain

    for t in toplevellist:
        for d in domainlist:
            for p in pagelist:
                yield "http://www.%s.%s/%s" % (d, t, p)

def generatedownloadlog(outfile):
    servers = ['SomeServer/1.0', 'SomeServer/2.0', 'SuperServer/3.0']
    cache = {}
    rnd = random.Random()
    rnd.seed(1) # We want to be able to recreate results
    
    i = 0
    year = startyear
    for month in range(1, months + 1):
        urls = generateurls()
        if month > 1 and month % 12 == 1: # It is January in a new year
            year += 1  
        downloaddate = "%d-%02d-01" % (year,
                                       month % 12 == 0 and 12 or month % 12)
        for url in urls:
            i += 1
            localfile = "%08d.tmp" % (i,)
            # With probability 100 - changeprob, the page wasn't changed
            tmp = rnd.randint(1,100)
            if tmp <= 100 - changeprob and url in cache:
                line = cache.get(url)
                line[0] = localfile
                line[4] = downloaddate
                writeline(outfile, line)
                continue
            # Else simulate some changes...

            # A given domain is always using the same server in this example.
            # We extract the number in the domain and use that to determine the
            # server.
            tmp = int(url.split(".tl")[0].split("domain")[1])
            server = servers[tmp % len(servers)]
            size = rnd.randint(100, 15000)

            tmp = rnd.randint(2,28)
            if month % 12 == 1:
                lastmoddate = "%d-12-%02d" % (year - 1, tmp)
            elif month % 12 == 0:
                lastmoddate = "%d-11-%02d" % (year, tmp)
            else:
                lastmoddate = "%d-%02d-%02d" % (year, month % 12 - 1, tmp)
            line = [localfile, url, server, size, downloaddate, lastmoddate]
            cache[url] = line
            writeline(outfile, line)



def generatetestresults(logfile, outfile):
    testlist = ['Test%d' % (i,) for i in range(tests)]
    logfile.readline() # Skip headers
    for logline in logfile:
        logfields = logline.split('\t')
        localfile = logfields[0]
        for test in testlist:
            # We "calculate" the errors (instead of taking a random number)
            # such that we get the same results when we consider an
            # unchanged file
            no1 = int(logfields[3]) # size
            no2 = int(test.split('t')[-1])  # The test number
            no3 = int(logfields[5].split('-')[-1]) # day af lastmoddate
            errors = (no2 * no1) % no3
            line = (localfile, test, errors)
            writeline(outfile, line)


def writeline(outfile, fields):
    outfile.write("\t".join([str(f) for f in fields]))
    outfile.write("\n")



import sys


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate CSV source data for the ETL benchmark."
        )
    )

    parser.add_argument(
        "--pages",
        type=int,
        help=(
            "Number of pages per domain. "
            "Defaults to the module configuration."
        ),
    )

    args = parser.parse_args()

    global pages

    if args.pages is not None:
        if args.pages < 1:
            raise ValueError(
                f"--pages must be a positive integer, got {args.pages}"
            )

        pages = args.pages

    downloadlog = open('DownloadLog.csv', 'w+', 16384)
    testresults = open('TestResults.csv', 'w', 16384)
    writeline(downloadlog, ['localfile', 'url', 'serverversion', 'size',
                            'downloaddate', 'lastmoddate'])
    writeline(testresults, ['localfile', 'test', 'errors'])
    generatedownloadlog(downloadlog)
    downloadlog.seek(0)
    generatetestresults(downloadlog, testresults)
    downloadlog.close()
    testresults.close()


if __name__ == "__main__":
    main()
