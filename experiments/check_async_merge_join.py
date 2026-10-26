"""Compare synchronous merge joins with asynchronous joins using source prefetching."""

import asyncio
import os

import psycopg
from dotenv import load_dotenv

from pygrametl.datasources import (
    SQLSource,
    AsyncSQLSource,
    MergeJoiningSource,
    AsyncMergeJoiningSource,
)


load_dotenv()


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
    LIMIT 100
"""

TEST_QUERY = """
    SELECT
        localfile,
        test,
        errors::text AS errors
    FROM testresults
    WHERE localfile IN (
        SELECT localfile
        FROM downloadlog
        ORDER BY localfile
        LIMIT 100
    )
    ORDER BY localfile, test
"""

PREFETCH_DEPTHS = (0, 1, 2, 4)
FETCH_SIZES = (1, 5, 500, 5000)


def connection_settings():
    return {
        "host": os.getenv("SOURCE_HOST", "localhost"),
        "port": os.getenv("SOURCE_PORT", "5432"),
        "dbname": os.getenv("SOURCE_DATABASE"),
        "user": os.getenv("USERNAME"),
    }


def get_sync_rows(fetchsize):
    settings = connection_settings()

    with (
        psycopg.connect(**settings) as conn1,
        psycopg.connect(**settings) as conn2,
    ):
        downloadlog = SQLSource(
            connection=conn1,
            query=DOWNLOAD_QUERY,
            cursorarg="sync_download_cursor",
            fetchsize=fetchsize,
        )

        testresults = SQLSource(
            connection=conn2,
            query=TEST_QUERY,
            cursorarg="sync_tests_cursor",
            fetchsize=fetchsize,
        )

        source = MergeJoiningSource(
            downloadlog,
            "localfile",
            testresults,
            "localfile",
        )

        return list(source)


async def get_async_rows(fetchsize, prefetch=0):
    settings = connection_settings()

    async with (
        await psycopg.AsyncConnection.connect(**settings) as conn1,
        await psycopg.AsyncConnection.connect(**settings) as conn2,
    ):
        downloadlog = AsyncSQLSource(
            connection=conn1,
            query=DOWNLOAD_QUERY,
            cursorarg="async_download_cursor",
            fetchsize=fetchsize,
            prefetch=prefetch,
        )

        testresults = AsyncSQLSource(
            connection=conn2,
            query=TEST_QUERY,
            cursorarg="async_tests_cursor",
            fetchsize=fetchsize,
            prefetch=prefetch,
        )

        source = AsyncMergeJoiningSource(
            downloadlog,
            "localfile",
            testresults,
            "localfile",
        )

        return [row async for row in source]


async def compare_sources(fetchsize, prefetch, sync_rows):
    async_rows = await get_async_rows(fetchsize, prefetch)

    assert sync_rows == async_rows, (
        f"Merge join differs for fetchsize={fetchsize}, prefetch={prefetch}"
    )

    assert async_rows, (
        f"No joined rows produced for fetchsize={fetchsize}, prefetch={prefetch}"
    )

    print(
        f"PASS: fetchsize={fetchsize}, "
        f"prefetch={prefetch}, joined rows={len(async_rows)}"
    )


async def main():
    print("Comparing synchronous and asynchronous merge joins with prefetching...\n")

    for fetchsize in FETCH_SIZES:
        # Reuse the synchronous reference result for each prefetch depth.
        sync_rows = get_sync_rows(fetchsize)
        assert sync_rows, f"No synchronous joined rows for fetchsize={fetchsize}"

        for prefetch in PREFETCH_DEPTHS:
            await compare_sources(fetchsize, prefetch, sync_rows)

    print("\nAll merge join correctness checks passed!")


if __name__ == "__main__":
    asyncio.run(main())
