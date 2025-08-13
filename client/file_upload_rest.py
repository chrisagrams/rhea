import os
import json
import requests
from requests import Response
from argparse import ArgumentParser

parser = ArgumentParser(description="Upload files to Rhea MCP server")
parser.add_argument("input_file", help="Input file")
parser.add_argument("--url", help="URL of MCP server", default="http://localhost:3001")
parser.add_argument("--name", required=False, help="Name for uploaded file.")

args = parser.parse_args()


def upload_file_rest(path: str, url: str, name: str | None = None) -> dict:
    size = os.path.getsize(path)
    name = name or os.path.basename(path)

    headers = {
        "Content-Type": "application/octet-stream",
        "x-filename": name,
        "Content-Length": str(size),
    }

    with open(path, "rb") as f:
        r: Response = requests.post(
            f"{url}/upload", data=f, headers=headers, timeout=300
        )
    r.raise_for_status()
    return json.loads(r.text)


if __name__ == "__main__":
    res = upload_file_rest(args.input_file, args.url, args.name)
    print(res)
    print(res["key"])
