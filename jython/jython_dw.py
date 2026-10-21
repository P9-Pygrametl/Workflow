"""Database helpers for the Jython pipeline using JDBC."""

import os

import java.sql.DriverManager


HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DEFAULT_SCHEMA = os.path.join(ROOT, 'starschema.sql')
DEFAULT_ENV = os.path.join(ROOT, '.env')


def loadenv(path=None):
    """Load simple KEY=value pairs from .env when running under Jython."""
    if path is None:
        path = DEFAULT_ENV
    if not os.path.exists(path):
        return

    for line in open(path):
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue

        key, value = line.split('=', 1)
        os.environ.setdefault(
            key.strip(),
            value.strip().strip('"').strip("'"),
        )


DOWNLOAD_QUERY = """
    SELECT
        localfile,
        url,
        serverversion,
        size::text AS size,
        downloaddate::text AS downloaddate,
        lastmoddate::text AS lastmoddate
    FROM downloadlog
    ORDER BY localfile
"""

TESTRESULTS_QUERY = """
    SELECT
        localfile,
        test,
        errors::text AS errors
    FROM testresults
    ORDER BY localfile, test
"""


def reset_warehouse(connection, schemafile=None):
    """Drop and recreate the warehouse schema using starschema.sql."""
    if schemafile is None:
        schemafile = DEFAULT_SCHEMA
    if not os.path.exists(schemafile):
        raise IOError("Schema file not found: %s" % schemafile)

    schema = open(schemafile, 'r').read()
    statement = connection.createStatement()

    try:
        for command in schema.split(';'):
            command = command.strip()
            if command:
                statement.execute(command)
    except:
        connection.rollback()
        raise
    finally:
        statement.close()


def source_connection():
    """Open a JDBC connection to the PostgreSQL source database."""
    host = os.environ.get('SOURCE_HOST', 'localhost')
    port = os.environ.get('SOURCE_PORT', '55432')
    database = os.environ.get('SOURCE_DATABASE', 'pygrametl_source')
    user = os.environ.get('USERNAME', 'mertz')

    connection = java.sql.DriverManager.getConnection(
        "jdbc:postgresql://%s:%s/%s?user=%s" % (host, port, database, user)
    )
    connection.setAutoCommit(False)
    return connection


def fetch_rows(connection, query):
    """Yield query results as dictionaries of strings."""
    statement = connection.createStatement()
    statement.setFetchSize(5000)
    results = statement.executeQuery(query)

    try:
        metadata = results.getMetaData()
        names = [
            metadata.getColumnLabel(i + 1)
            for i in range(metadata.getColumnCount())
        ]

        while results.next():
            yield dict(
                (name, results.getString(i + 1))
                for i, name in enumerate(names)
            )
    finally:
        results.close()
        statement.close()


def downloadlog_source():
    """Yield download log rows from the source database."""
    connection = source_connection()

    try:
        for row in fetch_rows(connection, DOWNLOAD_QUERY):
            yield row
    finally:
        connection.close()


def testresults_source():
    """Yield test result rows from the source database."""
    connection = source_connection()

    try:
        for row in fetch_rows(connection, TESTRESULTS_QUERY):
            yield row
    finally:
        connection.close()
