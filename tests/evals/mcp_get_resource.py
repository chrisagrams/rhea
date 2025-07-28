import asyncio
import os
import time
import csv
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager
from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client
from mcp.client.sse import sse_client
from mcp.shared.context import RequestContext
from pydantic import AnyUrl


async def run():
    async with sse_client("http://localhost:3001/sse") as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            resource_content = await session.read_resource(
                AnyUrl("proxystore://128108fa-3d2a-4838-b30f-f6d31ed5c766")
            )
            print(resource_content.contents)


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
