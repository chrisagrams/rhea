# upload_file.py

import asyncio
from rhea.client import RheaClient
from argparse import ArgumentParser
from urllib.parse import urlparse

parser = ArgumentParser(description="Upload files to Rhea MCP server")
parser.add_argument("input_file", help="Input file")
parser.add_argument("--url", help="URL of MCP server", default="http://localhost:3001")
parser.add_argument("--name", required=False, help="Name for uploaded file.")

args = parser.parse_args()


async def main():
    parsed_url = urlparse(args.url)
    protocol = parsed_url.scheme
    host = parsed_url.hostname
    port = parsed_url.port
    secure = protocol == "https"

    async with RheaClient(host, port, secure) as client:  # (1)!
        result: dict = await client.upload_file(args.input_file, args.name)  # (2)!
        print(result)
        print(result["key"])  # (3)!


if __name__ == "__main__":
    asyncio.run(main())  # (4)!
