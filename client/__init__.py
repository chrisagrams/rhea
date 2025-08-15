from __future__ import annotations
from pydantic import AnyUrl
from typing import Optional, Type, Any
from pathlib import Path

from .mcp import RheaMCPClient
from .rest import RheaRESTClient
from .base import RheaMCPClientBase, RheaRESTClientBase

from mcp.types import Tool, Resource, TextResourceContents, BlobResourceContents


class RheaClient(RheaMCPClientBase, RheaRESTClientBase):
    """
    A client class to interact with both the Rhea Model Context Protocol (MCP) server and REST backend.

    The client class provides similar high-level interface to the one found within the Python MCP SDK.

    The class also provides additional utilities to interact with the REST backend such as file upload and downloads.

    Example:
    ```python
    async with RheaClient('localhost', 3001) as client:
        # MCP call
        tools = await client.find_tools('I need a tool to convert FASTA to FASTQ')
        # REST call
        key = await client.upload_file('test.txt')
    ```
    """

    def __init__(self, hostname: str, port: int, secure: bool = False):
        self._hostname = hostname
        self._port = port
        self._secure = secure

        self._mcp_ctx: Optional[RheaMCPClient] = None
        self._mcp_client: Optional[RheaMCPClient] = None

        self._rest_ctx: Optional[RheaRESTClient] = None
        self._rest_client: Optional[RheaRESTClient] = None

    async def __aenter__(self) -> RheaClient:
        self._mcp_ctx = RheaMCPClient(self._hostname, self._port, self._secure)
        self._mcp_client = await self._mcp_ctx.__aenter__()

        self._rest_ctx = RheaRESTClient(self._hostname, self._port, self._secure)
        self._rest_client = await self._rest_ctx.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb
    ) -> None:
        if self._mcp_ctx is not None:
            await self._mcp_ctx.__aexit__(exc_type, exc, tb)
            self._mcp_ctx = None
            self._mcp_client = None
        if self._rest_ctx is not None:
            await self._rest_ctx.__aexit__(exc_type, exc, tb)
            self._rest_ctx = None
            self._rest_client = None

    def __enter__(self):
        raise RuntimeError("Use 'async with RheaClient(...)'")

    def __exit__(self, *args):
        pass

    async def list_tools(self) -> list[Tool]:
        if self._mcp_client is not None:
            return await self._mcp_client.list_tools()
        raise RuntimeError("`mcp_client` is None")

    async def find_tools(self, query: str) -> list[dict]:
        if self._mcp_client is not None:
            return await self._mcp_client.find_tools(query)
        raise RuntimeError("`mcp_client` is None")

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> dict | None:
        if self._mcp_client is not None:
            return await self._mcp_client.call_tool(name, arguments)
        raise RuntimeError("`mcp_client` is None")

    async def list_resources(self) -> list[Resource]:
        if self._mcp_client is not None:
            return await self._mcp_client.list_resources()
        raise RuntimeError("`mcp_client` is None")

    async def read_resource(
        self, uri: AnyUrl
    ) -> list[TextResourceContents | BlobResourceContents]:
        if self._mcp_client is not None:
            return await self._mcp_client.read_resource(uri)
        raise RuntimeError("`mcp_clien` is None")

    async def upload_file(
        self,
        path: str,
        name: str | None = None,
        timeout: int = 300,
        chunk_size: int = 1 << 20,
    ) -> dict:
        if self._rest_client is not None:
            return await self._rest_client.upload_file(path, name, timeout, chunk_size)
        raise RuntimeError("`rest_client` is None")

    async def download_file(
        self,
        key: str,
        output_directory: Path = Path.cwd(),
        timeout: int = 300,
        chunk_size: int = 1 << 20,
    ) -> int:
        if self._rest_client is not None:
            return await self._rest_client.download_file(
                key, output_directory, timeout, chunk_size
            )
        raise RuntimeError("`rest_client` is None")

    async def metrics(self) -> dict[str, list[dict]]:
        if self._rest_client is not None:
            return await self._rest_client.metrics()
        raise RuntimeError("`rest_client` is None")
