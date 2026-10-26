"""Compare SQLSource and AsyncSQLSource, including optional batch prefetching.

Run from the workflow repository root:
    python experiments/check_async_sql.py
"""

import asyncio
import os
from contextlib import aclosing

import psycopg
from dotenv import load_dotenv

from pygrametl.datasources import AsyncSQLSource, SQLSource


load_dotenv()

PREFETCH_DEPTHS = (0, 1, 2, 4)
FETCH_SIZES = (1, 5, 5000, 0, -1)

QUERY = """
    SELECT *
    FROM downloadlog
    ORDER BY localfile
    LIMIT 100
"""


def connection_settings():
    return {
        "host": os.getenv("SOURCE_HOST", "localhost"),
        "port": os.getenv("SOURCE_PORT", "5432"),
        "dbname": os.getenv("SOURCE_DATABASE"),
        "user": os.getenv("USERNAME"),
    }


def get_sync_rows(query=QUERY, fetchsize=5, names=(), parameters=None):
    with psycopg.connect(**connection_settings()) as connection:
        source = SQLSource(
            connection=connection,
            query=query,
            names=names,
            parameters=parameters,
            cursorarg="sync_test_cursor",
            fetchsize=fetchsize,
        )
        return list(source)


async def get_async_rows(
    query=QUERY, fetchsize=5, names=(), parameters=None, prefetch=0
):
    async with await psycopg.AsyncConnection.connect(
        **connection_settings()
    ) as connection:
        source = AsyncSQLSource(
            connection=connection,
            query=query,
            names=names,
            parameters=parameters,
            cursorarg="async_test_cursor",
            fetchsize=fetchsize,
            prefetch=prefetch,
        )
        return [row async for row in source]


async def compare_sources(
    description,
    query=QUERY,
    fetchsize=5,
    names=(),
    parameters=None,
    expected_count=None,
    prefetch=0,
):
    sync_rows = get_sync_rows(
        query=query,
        fetchsize=fetchsize,
        names=names,
        parameters=parameters,
    )
    async_rows = await get_async_rows(
        query=query,
        fetchsize=fetchsize,
        names=names,
        parameters=parameters,
        prefetch=prefetch,
    )
    assert sync_rows == async_rows, (
        f"{description}: synchronous and asynchronous rows differ"
    )
    if expected_count is not None:
        assert len(async_rows) == expected_count, (
            f"{description}: expected {expected_count} rows, got {len(async_rows)}"
        )
    print(f"PASS: {description} ({len(async_rows)} rows)")
    return async_rows


async def test_fetch_sizes():
    for prefetch in PREFETCH_DEPTHS:
        for fetchsize in FETCH_SIZES:
            await compare_sources(
                description=f"prefetch={prefetch}, fetchsize={fetchsize}",
                fetchsize=fetchsize,
                prefetch=prefetch,
                expected_count=100,
            )


async def test_custom_names():
    query = """
        SELECT localfile, size
        FROM downloadlog
        ORDER BY localfile
        LIMIT 10
    """
    for prefetch in PREFETCH_DEPTHS:
        rows = await compare_sources(
            description=f"custom column names, prefetch={prefetch}",
            query=query,
            names=("filename", "filesize"),
            prefetch=prefetch,
            expected_count=10,
        )
        assert list(rows[0]) == ["filename", "filesize"]


async def test_parameters():
    query = """
        SELECT localfile, size
        FROM downloadlog
        WHERE size > %(minimum_size)s
        ORDER BY localfile
        LIMIT 20
    """
    for prefetch in PREFETCH_DEPTHS:
        await compare_sources(
            description=f"parameterised query, prefetch={prefetch}",
            query=query,
            parameters={"minimum_size": 10000},
            prefetch=prefetch,
            expected_count=20,
        )


async def test_empty_results():
    query = "SELECT * FROM downloadlog WHERE FALSE"
    for prefetch in PREFETCH_DEPTHS:
        await compare_sources(
            description=f"empty result set, prefetch={prefetch}",
            query=query,
            prefetch=prefetch,
            expected_count=0,
        )


async def test_invalid_names():
    query = "SELECT localfile, size FROM downloadlog LIMIT 1"
    invalid_names = ("filename",)

    try:
        get_sync_rows(query=query, names=invalid_names)
    except ValueError:
        pass
    else:
        raise AssertionError("SQLSource should reject incorrect names")

    for prefetch in PREFETCH_DEPTHS:
        try:
            await get_async_rows(
                query=query, names=invalid_names, prefetch=prefetch
            )
        except ValueError:
            pass
        else:
            raise AssertionError(
                f"AsyncSQLSource should reject incorrect names, prefetch={prefetch}"
            )
        print(f"PASS: invalid names, prefetch={prefetch}")


class ConnectionProxy:
    """Track real Psycopg cursors without modifying read-only cursor methods."""

    def __init__(self, connection):
        self.connection = connection
        self.created_cursor = None

    def cursor(self, name=None):
        self.created_cursor = self.connection.cursor(name)
        return self.created_cursor


async def test_cursor_cleanup():
    for prefetch in PREFETCH_DEPTHS:
        async with await psycopg.AsyncConnection.connect(
            **connection_settings()
        ) as connection:
            proxy = ConnectionProxy(connection)
            source = AsyncSQLSource(
                connection=proxy,
                query="SELECT localfile FROM downloadlog ORDER BY localfile LIMIT 5",
                cursorarg="cleanup_test_cursor",
                fetchsize=2,
                prefetch=prefetch,
            )
            rows = [row async for row in source]
            assert len(rows) == 5
            assert proxy.created_cursor is not None
            assert proxy.created_cursor.closed, (
                f"Cursor still open after full iteration, prefetch={prefetch}"
            )
        print(f"PASS: cursor cleanup, prefetch={prefetch}")


async def test_early_termination():
    for prefetch in PREFETCH_DEPTHS:
        async with await psycopg.AsyncConnection.connect(
            **connection_settings()
        ) as connection:
            proxy = ConnectionProxy(connection)
            source = AsyncSQLSource(
                connection=proxy,
                query=QUERY,
                cursorarg="early_termination_cursor",
                fetchsize=5,
                prefetch=prefetch,
            )
            async with aclosing(source.__aiter__()) as iterator:
                first_row = await anext(iterator)
                assert "localfile" in first_row

            assert proxy.created_cursor is not None
            assert proxy.created_cursor.closed, (
                f"Cursor still open after early termination, prefetch={prefetch}"
            )
        print(f"PASS: early termination and cleanup, prefetch={prefetch}")


async def test_database_errors():
    """Execution errors must reach the caller and not leak the cursor."""
    for prefetch in PREFETCH_DEPTHS:
        async with await psycopg.AsyncConnection.connect(
            **connection_settings()
        ) as connection:
            proxy = ConnectionProxy(connection)
            source = AsyncSQLSource(
                connection=proxy,
                query="SELECT * FROM table_that_does_not_exist_for_async_test",
                cursorarg="error_test_cursor",
                fetchsize=5,
                prefetch=prefetch,
            )
            try:
                await asyncio.wait_for(
                    _consume(source), timeout=10
                )
            except psycopg.errors.UndefinedTable:
                pass
            else:
                raise AssertionError(
                    f"Expected UndefinedTable, prefetch={prefetch}"
                )
            assert proxy.created_cursor is not None
            assert proxy.created_cursor.closed, (
                f"Cursor still open after query error, prefetch={prefetch}"
            )
        print(f"PASS: SQL execution error and cleanup, prefetch={prefetch}")


async def _consume(source):
    return [row async for row in source]


async def test_producer_fetch_error():
    """A failure after a successful batch must propagate without hanging.

    Uses a tiny fake asynchronous cursor to inject an error specifically from
    fetchmany(), which exercises the prefetch producer rather than execute().
    """

    class BrokenCursor:
        description = (("value",),)

        def __init__(self):
            self.fetches = 0
            self.closed = False

        async def execute(self, query, parameters=None):
            return None

        async def fetchmany(self, size):
            self.fetches += 1
            if self.fetches == 1:
                return [(1,), (2,)]
            raise RuntimeError("simulated fetch failure")

        async def close(self):
            self.closed = True

    class FakeConnection:
        def __init__(self):
            self.created_cursor = None

        def cursor(self, name=None):
            self.created_cursor = BrokenCursor()
            return self.created_cursor

    for prefetch in PREFETCH_DEPTHS:
        connection = FakeConnection()
        source = AsyncSQLSource(
            connection=connection,
            query="SELECT value",
            fetchsize=2,
            prefetch=prefetch,
        )
        iterator = source.__aiter__()
        values = []
        try:
            async with aclosing(iterator):
                try:
                    async with asyncio.timeout(5):
                        async for row in iterator:
                            values.append(row)
                except RuntimeError as exc:
                    assert str(exc) == "simulated fetch failure"
                else:
                    raise AssertionError(
                        f"Expected fetch error, prefetch={prefetch}"
                    )
        finally:
            assert connection.created_cursor is not None
            assert connection.created_cursor.closed
        assert values == [{"value": 1}, {"value": 2}], values
        print(f"PASS: producer fetch error, prefetch={prefetch}")


async def main():
    print("Checking SQLSource against AsyncSQLSource (with prefetching)...\n")
    await test_fetch_sizes()
    await test_custom_names()
    await test_parameters()
    await test_empty_results()
    await test_invalid_names()
    await test_cursor_cleanup()
    await test_early_termination()
    await test_database_errors()
    await test_producer_fetch_error()
    print("\nAll correctness checks passed!")


if __name__ == "__main__":
    asyncio.run(main())
