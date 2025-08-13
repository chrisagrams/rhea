import os
import re
import json
import requests
from requests import Response
from argparse import ArgumentParser

parser = ArgumentParser(description="Download files from Rhea MCP server")
parser.add_argument("key", help="Key of desired file")
parser.add_argument("--url", help="URL of MCP server", default="http://localhost:3001")
parser.add_argument("--output-path", help="Output path", default=os.getcwd())
parser.add_argument("--output-name", help="Name of output file")
parser.add_argument("--chunk-size", type=int, default=1 << 20)
parser.add_argument("--insecure", action="store_true")

args = parser.parse_args()


def infer_filename(res: Response, key: str, explicit: str | None) -> str:
    if explicit:
        return explicit
    cd = res.headers.get("Content-Disposition", "")
    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
    if m:
        return os.path.basename(m.group(1))
    raise ValueError("Could not find filename in Content-Disposition")


def download_file_rest(
    key: str,
    url: str,
    output_path: str = os.getcwd(),
    timeout: int = 300,
    chunk_size: int = 1 << 20,
    insecure: bool = True,
):
    resp = requests.get(
        f"{url}/download",
        params={"key": key},
        stream=True,
        timeout=timeout,
        verify=not insecure,
    )
    fname = infer_filename(resp, key, None)
    full_path = os.path.join(output_path, fname)
    written = 0
    with open(full_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                written += len(chunk)
    print(f"Saved {fname} to {full_path}")


if __name__ == "__main__":
    download_file_rest(
        args.key,
        args.url,
        insecure=args.insecure,
        chunk_size=args.chunk_size,
        output_path=args.output_path,
    )
