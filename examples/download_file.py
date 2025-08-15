# download_file.py

import asyncio
from rhea.client import RheaClient
from argparse import ArgumentParser
from pathlib import Path
from urllib.parse import urlparse

parser = ArgumentParser(description="Download files from Rhea MCP server")
parser.add_argument("key", help="Key of desired file")
parser.add_argument("--url", help="URL of MCP server", default="http://localhost:3001")
parser.add_argument("--output-directory", help="Output directory", default=Path.cwd())
parser.add_argument("--output-name", help="Name of output file")

args = parser.parse_args()


async def main():
    parsed_url = urlparse(args.url)
    protocol = parsed_url.scheme
    host = parsed_url.hostname
    port = parsed_url.port
    secure = protocol == "https"

    async with RheaClient(host, port, secure) as client:  # (1)!
        await client.download_file(args.key, args.output_directory)  # (2)!


if __name__ == "__main__":
    asyncio.run(main())  # (3)!
