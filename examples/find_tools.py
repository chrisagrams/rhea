# find_tools.py

import asyncio
from rhea.client import RheaClient
from argparse import ArgumentParser
from urllib.parse import urlparse
from mcp.types import Tool

parser = ArgumentParser(
    description="Find relevant tools based on natural language query."
)
parser.add_argument("query", help="Natural language query")
parser.add_argument("--url", help="URL of MCP server", default="http://localhost:3001")

args = parser.parse_args()


async def main():
    parsed_url = urlparse(args.url)
    protocol = parsed_url.scheme
    host = parsed_url.hostname
    port = parsed_url.port
    secure = protocol == "https"

    async with RheaClient(host, port, secure) as client:  # (1)!
        await client.find_tools(args.query)  # (2)!
        tools: list[Tool] = await client.list_tools()  # (3)!
        print(tools)


if __name__ == "__main__":
    asyncio.run(main())  # (4)!
