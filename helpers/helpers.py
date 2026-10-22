import datetime
import time

import pygrametl

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


def convertsize(row):
    row["size"] = pygrametl.getint(row["size"])
