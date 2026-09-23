"""Pure row transformations for parallel_etl_example.py.

These functions live in an importable module so Python 3.14 can reconstruct
them inside the subinterpreters used by parallel.createflow().
"""

from urllib.parse import urlsplit


def extractdomaininfo(row):
    hostname = urlsplit(row["url"]).hostname
    if hostname is None:
        raise ValueError(f"URL has no hostname: {row['url']!r}")
    row["domain"] = hostname
    row["topleveldomain"] = hostname.rsplit(".", 1)[-1]


def extractserverinfo(row):
    row["server"] = row["serverversion"].split("/", 1)[0]


def convertmeasures(row):
    row["size"] = int(row["size"])
    row["errors"] = int(row["errors"])
