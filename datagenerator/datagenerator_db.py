import os
import random
from itertools import islice

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

import datagenerator


load_dotenv()


def create_source_tables(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS downloadlog (
                localfile TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                serverversion TEXT NOT NULL,
                size INTEGER NOT NULL,
                downloaddate DATE NOT NULL,
                lastmoddate DATE NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS testresults (
                localfile TEXT NOT NULL,
                test TEXT NOT NULL,
                errors INTEGER NOT NULL,
                PRIMARY KEY (localfile, test)
            )
        """)

    connection.commit()


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


def insert_download_rows(connection, rows):
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO downloadlog (
                localfile,
                url,
                serverversion,
                size,
                downloaddate,
                lastmoddate
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows,
        )

    connection.commit()


def generate_test_rows(download_rows):
    for download_row in download_rows:
        localfile = download_row[0]
        size = download_row[3]
        lastmoddate = download_row[5]

        day = int(lastmoddate.split("-")[-1])

        for test_number in range(datagenerator.tests):
            test = "Test%d" % test_number
            errors = (test_number * size) % day

            yield (localfile, test, errors)


def insert_test_rows(connection, rows, batch_size=10000):
    with connection.cursor() as cursor:
        while True:
            batch = list(islice(rows, batch_size))

            if not batch:
                break

            execute_values(
                cursor,
                """
                INSERT INTO testresults (
                    localfile,
                    test,
                    errors
                )
                VALUES %s
                """,
                batch,
            )

    connection.commit()


def clear_source_tables(connection):
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE testresults, downloadlog")

    connection.commit()


def main():
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

    connection = psycopg2.connect(
        host=source_host,
        port=source_port,
        dbname=source_database,
        user=username,
    )

    print("Connected successfully")

    create_source_tables(connection)
    clear_source_tables(connection)

    print("Generating download data...")

    download_rows = generate_download_rows()
    insert_download_rows(connection, download_rows)

    print("Download rows inserted successfully")

    print("Generating test result data...")

    # Fresh generator because the previous one was consumed
    download_rows = generate_download_rows()
    test_rows = generate_test_rows(download_rows)

    insert_test_rows(connection, test_rows)

    print("Test result rows inserted successfully")

    connection.close()


if __name__ == "__main__":
    main()