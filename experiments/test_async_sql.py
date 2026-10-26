
import asyncio
import os

import psycopg
from dotenv import load_dotenv

from pygrametl.datasources import AsyncSQLSource

load_dotenv()


async def main():
    async with await psycopg.AsyncConnection.connect(
        host=os.getenv("SOURCE_HOST", "localhost"),
        port=os.getenv("SOURCE_PORT", "5432"),
        dbname=os.getenv("SOURCE_DATABASE"),
        user=os.getenv("USERNAME"),
    ) as connection:

        source = AsyncSQLSource(
            connection=connection,
            query="SELECT * FROM downloadlog LIMIT 12",
            cursorarg="test_async_cursor",
            fetchsize=5,
        )

        async for row in source:
            print(row)


asyncio.run(main())
