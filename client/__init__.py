from __future__ import annotations
from pydantic import AnyUrl
from typing import Optional, Type, Any
from pathlib import Path

from client.mcp import RheaMCPClient
from client.rest import RheaRESTClient

from mcp.types import Tool, Resource, TextResourceContents, BlobResourceContents


class RheaClient:
    def __init__(self, hostname: str, port: int, secure: bool = False):
        self._hostname = hostname
        self._port = port
        self._secure = secure

        self._mcp_ctx: Optional[RheaMCPClient] = None
        self.mcp_client: Optional[RheaMCPClient] = None

        self._rest_ctx: Optional[RheaRESTClient] = None
        self.rest_client: Optional[RheaRESTClient] = None

    async def __aenter__(self) -> RheaClient:
        self._mcp_ctx = RheaMCPClient(self._hostname, self._port, self._secure)
        self.mcp_client = await self._mcp_ctx.__aenter__()

        self._rest_ctx = RheaRESTClient(self._hostname, self._port, self._secure)
        self.rest_client = await self._rest_ctx.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb
    ) -> None:
        if self._mcp_ctx is not None:
            await self._mcp_ctx.__aexit__(exc_type, exc, tb)
            self._mcp_ctx = None
            self.mcp_client = None
        if self._rest_ctx is not None:
            await self._rest_ctx.__aexit__(exc_type, exc, tb)
            self._rest_ctx = None
            self.rest_client = None

    def __enter__(self):
        raise RuntimeError("Use 'async with RheaClient(...)'")

    def __exit__(self, *args):
        pass

    async def list_tools(self) -> list[Tool]:
        if self.mcp_client is not None:
            return await self.mcp_client.list_tools()
        raise RuntimeError("`mcp_client` is None")

    list_tools.__doc__ = RheaMCPClient.list_tools.__doc__

    async def find_tools(self, query: str) -> list[dict]:
        if self.mcp_client is not None:
            return await self.mcp_client.find_tools(query)
        raise RuntimeError("`mcp_client` is None")

    find_tools.__doc__ = RheaMCPClient.find_tools.__doc__

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> dict | None:
        if self.mcp_client is not None:
            return await self.mcp_client.call_tool(name, arguments)
        raise RuntimeError("`mcp_client` is None")

    call_tool.__doc__ = RheaMCPClient.call_tool.__doc__

    async def list_resources(self) -> list[Resource]:
        if self.mcp_client is not None:
            return await self.mcp_client.list_resources()
        raise RuntimeError("`mcp_client` is None")

    list_resources.__doc__ = RheaMCPClient.list_resources.__doc__

    async def read_resource(
        self, uri: AnyUrl
    ) -> list[TextResourceContents | BlobResourceContents]:
        if self.mcp_client is not None:
            return await self.mcp_client.read_resource(uri)
        raise RuntimeError("`mcp_clien` is None")

    read_resource.__doc__ = RheaMCPClient.list_resources.__doc__

    async def upload_file(
        self,
        path: str,
        name: str | None = None,
        timeout: int = 300,
        chunk_size: int = 1 << 20,
    ) -> dict:
        if self.rest_client is not None:
            return await self.rest_client.upload_file(path, name, timeout, chunk_size)
        raise RuntimeError("`rest_client` is None")

    upload_file.__doc__ = RheaRESTClient.upload_file.__doc__

    async def download_file(
        self,
        key: str,
        output_directory: Path = Path.cwd(),
        timeout: int = 300,
        chunk_size: int = 1 << 20,
    ) -> int:
        if self.rest_client is not None:
            return await self.rest_client.download_file(
                key, output_directory, timeout, chunk_size
            )
        raise RuntimeError("`rest_client` is None")

    download_file.__doc__ = RheaRESTClient.download_file.__doc__

    async def metrics(self) -> dict[str, list[dict]]:
        if self.rest_client is not None:
            return await self.rest_client.metrics()
        raise RuntimeError("`rest_client` is None")

    metrics.__doc__ = RheaRESTClient.metrics.__doc__
