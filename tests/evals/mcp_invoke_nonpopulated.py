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


async def run():
    async with sse_client("http://localhost:3001/sse") as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Get available tools
            tools = await session.list_tools()
            print(f"Available tools: {[t.name for t in tools.tools]}")

            # Call 'CSV to Tabular' tool (without RAG step)
            tool_result = await session.call_tool(
                "csv_to_tabular",
                arguments={
                    "input1": "8ac59c17-7a79-44c2-8b0b-be9f70a5662c",
                    "sep": ",",
                    "header": True,
                },
            )
            assert tool_result.structuredContent is not None
            rc = tool_result.structuredContent.get("return_code")
            assert str(rc) == "0"
            return tool_result


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
