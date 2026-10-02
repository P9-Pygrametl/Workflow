import io
import os
import random

import psycopg
from dotenv import load_dotenv

from datagenerator import datagenerator


load_dotenv()


DOWNLOAD_COLUMNS = (
    "localfile",
    "url",
    "serverversion",
    "size",
    "downloaddate",
    "lastmoddate",
)

TEST_COLUMNS = (
    "localfile",
    "test",
    "errors",
)

# How many rows to buffer before flushing to PostgreSQL. 
ROWS_PER_FLUSH = 100000


def generate_download_rows():
    servers = ["SomeServer/1.0", "SomeServer/2.0", "SuperServer/3.0"]
    cache = {}

    rnd = random.Random()
    rnd.seed(1)

    i = 0
    year = datagenerator.startyear

    for month in range(1, datagenerator.months + 1):
        urls = datagenerator.generateurls()

        if month > 1 and month % 12 == 1:
            year += 1

        month_number = 12 if month % 12 == 0 else month % 12
        downloaddate = "%d-%02d-01" % (year, month_number)

        for url in urls:
            i += 1
            localfile = "%08d.tmp" % i

            tmp = rnd.randint(1, 100)

            if tmp <= 100 - datagenerator.changeprob and url in cache:
                line = cache[url].copy()
                line[0] = localfile
                line[4] = downloaddate
                cache[url] = line

                yield tuple(line)
                continue

            domain_number = int(url.split(".tl")[0].split("domain")[1])
            server = servers[domain_number % len(servers)]

            size = rnd.randint(100, 15000)

            tmp = rnd.randint(2, 28)

            if month % 12 == 1:
                lastmoddate = "%d-12-%02d" % (year - 1, tmp)
            elif month % 12 == 0:
                lastmoddate = "%d-11-%02d" % (year, tmp)
            else:
                lastmoddate = "%d-%02d-%02d" % (
                    year,
                    month % 12 - 1,
                    tmp,
                )

            line = [
                localfile,
                url,
                server,
                size,
                downloaddate,
                lastmoddate,
            ]

            cache[url] = line

            yield tuple(line)


class BulkInserter:
    """Buffers rows for one table and loads them in bulk with COPY."""

    def __init__(self, table, columns, rows_per_flush=ROWS_PER_FLUSH):
        self.table = table
        self.columns = columns
        self.rows_per_flush = rows_per_flush
        self.buffer = io.StringIO()
        self.count = 0

    def add(self, cursor, row):
        self.buffer.write("\t".join(str(value) for value in row))
        self.buffer.write("\n")
        self.count += 1

        if self.count >= self.rows_per_flush:
            self.flush(cursor)

    def flush(self, cursor):
        if not self.buffer.tell():
            return

        self.buffer.seek(0)
        cursor.copy_from(self.buffer, self.table, columns=self.columns)
        self.buffer.seek(0)
        self.buffer.truncate(0)
        self.count = 0


def test_rows_for_download(download_row):
    localfile = download_row[0]
    size = download_row[3]
    lastmoddate = download_row[5]

    day = int(lastmoddate.split("-")[-1])

    for test_number in range(datagenerator.tests):
        test = "Test%d" % test_number
        errors = (test_number * size) % day

        yield (localfile, test, errors)


def insert_source_rows(
    connection,
    download_rows,
    rows_per_flush=ROWS_PER_FLUSH,
):
    download_inserter = BulkInserter(
        "downloadlog",
        DOWNLOAD_COLUMNS,
        rows_per_flush,
    )
    test_inserter = BulkInserter(
        "testresults",
        TEST_COLUMNS,
        rows_per_flush,
    )

    with connection.cursor() as cursor:
        with cursor.copy(
            "COPY testresults (localfile, test, errors) FROM STDIN"
        ) as copy:
            for row in rows:
                copy.write_row(row)

    connection.commit()


def create_primary_keys(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            "ALTER TABLE downloadlog "
            "ADD PRIMARY KEY (localfile)"
        )

        cursor.execute(
            "ALTER TABLE testresults "
            "ADD PRIMARY KEY (localfile, test)"
        )


def generate(pages=None):
    if pages is not None:
        if pages < 1:
            raise ValueError(
                f"pages must be a positive integer, got {pages}"
            )

        datagenerator.pages = pages

    username = os.getenv("USERNAME")
    source_database = os.getenv("SOURCE_DATABASE", "pygrametl_source")
    source_host = os.getenv("SOURCE_HOST", "localhost")
    source_port_value = os.getenv("SOURCE_PORT", "5432")

    try:
        source_port = int(source_port_value)
    except ValueError:
        raise ValueError(
            f"SOURCE_PORT must be an integer, got {source_port_value!r}"
        )

    connection = psycopg.connect(
        host=source_host,
        port=source_port,
        dbname=source_database,
        user=username,
    )

    try:
        print("Connected successfully")

        recreate_source_tables(connection)

        print("Generating source data...")

        download_rows = generate_download_rows()
        insert_source_rows(connection, download_rows)

        # Build the primary keys once the data has been loaded.
        create_primary_keys(connection)
        connection.commit()

        print("Source rows inserted successfully")

    finally:
        connection.close()
