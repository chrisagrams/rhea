from __future__ import annotations
import re
import json
import asyncio
from pathlib import Path
from urllib.parse import urlunparse, urljoin

import httpx
from prometheus_client.parser import text_string_to_metric_families

from .base import RheaRESTClientBase


class RheaRESTClient(RheaRESTClientBase):
    def __init__(self, hostname: str, port: int, secure: bool = False):
        self.hostname = hostname
        self.port = port
        self.secure = secure
        scheme = "https" if secure else "http"
        netloc = f"{hostname}:{port}"
        self.base_url = urlunparse((scheme, netloc, "", "", "", ""))
        self._client: httpx.AsyncClient | None = None

    def _url(self, path: str) -> str:
        return urljoin(self.base_url, path.lstrip("/"))

    async def __aenter__(self) -> RheaRESTClient:
        self._client = httpx.AsyncClient(base_url=self.base_url, verify=self.secure)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def upload_file(
        self,
        path: str,
        name: str | None = None,
        timeout: int = 300,
        chunk_size: int = 1 << 20,
    ) -> dict:
        if self._client is None:
            raise RuntimeError(
                "Client not initialized. Use 'async with RheaRESTClient(...)'."
            )
        p = Path(path)
        if not p.is_file():
            raise ValueError(f"{path} does not exist or is not a file.")
        size_bytes = p.stat().st_size
        name = name or p.name
        headers = {
            "Content-Type": "application/octet-stream",
            "x-filename": name,
            "Content-Length": str(size_bytes),
        }

        async def gen():
            with p.open("rb") as f:
                while True:
                    chunk = await asyncio.to_thread(f.read, chunk_size)
                    if not chunk:
                        break
                    yield chunk

        r = await self._client.post(
            self._url("upload"), content=gen(), headers=headers, timeout=timeout
        )
        r.raise_for_status()
        return json.loads(r.text)

    async def download_file(
        self,
        key: str,
        output_directory: Path = Path.cwd(),
        timeout: int = 300,
        chunk_size: int = 1 << 20,
    ) -> int:
        if self._client is None:
            raise RuntimeError(
                "Client not initialized. Use 'async with RheaRESTClient(...)'."
            )

        def infer_filename(res: httpx.Response) -> str:
            cd = res.headers.get("Content-Disposition", "")
            m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
            if m:
                return Path(m.group(1)).name
            raise ValueError("Could not find filename in Content-Disposition")

        async with self._client.stream(
            "GET",
            self._url("download"),
            params={"key": key},
            timeout=timeout,
        ) as r:
            r.raise_for_status()
            fname = infer_filename(r)
            p = Path(output_directory) / Path(fname)
            written = 0
            with p.open("wb") as f:
                async for chunk in r.aiter_bytes(chunk_size=chunk_size):
                    if chunk:
                        await asyncio.to_thread(f.write, chunk)
                        written += len(chunk)
        return written

    async def metrics(self) -> dict[str, list[dict]]:
        if self._client is None:
            raise RuntimeError(
                "Client not initialized. Use 'async with RheaRESTClient(...)'."
            )
        r = await self._client.get(self._url("metrics"), timeout=5)
        r.raise_for_status()
        result: dict[str, list[dict]] = {}
        for fam in text_string_to_metric_families(r.text):
            bucket = result.setdefault(fam.name, [])
            for (name, labels, value, *_rest) in fam.samples:
                bucket.append({"sample": name, "labels": labels, "value": value})
        return result
