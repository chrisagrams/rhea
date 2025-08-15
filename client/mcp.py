from __future__ import annotations
from pydantic import BaseModel, PrivateAttr, AnyUrl
from typing import Any

# MCP imports
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import (
    CallToolResult,
    ListToolsResult,
    ListResourcesResult,
    ReadResourceResult,
    Tool,
    Resource,
    BlobResourceContents,
    TextResourceContents,
)

from .base import RheaMCPClientBase

from urllib.parse import urlunparse, urljoin


class RheaMCPClient(RheaMCPClientBase):
    """
    A client class to interact with the Rhea Model Context Protocol (MCP) service.

    This class provides a high-level interface for connecting to the Rhea MCP server,
    similar to the one found within the Python MCP SDK (FastMCP).

    """

    def __init__(self, hostname: str, port: int, secure: bool = False):

        self.hostname = hostname
        self.port = port
        self.secure = secure

        scheme = "https" if secure else "http"
        netloc = f"{hostname}:{port}"
        self.base_url = urlunparse((scheme, netloc, "", "", "", ""))

        self.http_client = None
        self.read = self.write = None
        self.session = None

    def _url(self, path: str) -> str:
        """Return full URL for a given path, safely joined to base_url."""
        return urljoin(self.base_url, path.lstrip("/"))

    async def __aenter__(self):
        self.http_client = streamablehttp_client(self._url("mcp"))
        self.read, self.write, _ = await self.http_client.__aenter__()
        self.session = ClientSession(self.read, self.write)
        await self.session.__aenter__()
        await self.session.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.__aexit__(exc_type, exc, tb)
        if self.http_client:
            await self.http_client.__aexit__(exc_type, exc, tb)

    async def list_tools(self) -> list[Tool]:
        if not self.session:
            raise RuntimeError("Client session was never initialized.")

        res: ListToolsResult = await self.session.list_tools()

        return res.tools

    async def find_tools(self, query: str) -> list[dict]:
        if not self.session:
            raise RuntimeError("Client session was never initialized.")

        res: CallToolResult = await self.session.call_tool(
            "find_tools", arguments={"query": query}
        )

        if res.structuredContent is None:
            print(res.content)
            return []

        return res.structuredContent.get("result", [])

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> dict | None:
        if not self.session:
            raise RuntimeError("Client session was never initialized.")

        res: CallToolResult = await self.session.call_tool(name, arguments)

        if res.isError:
            print(f"Error occured calling tool: {res.content}")

        return res.structuredContent

    async def list_resources(self) -> list[Resource]:
        if not self.session:
            raise RuntimeError("Client session was never initialized.")

        res: ListResourcesResult = await self.session.list_resources()

        return res.resources

    async def read_resource(
        self, uri: AnyUrl
    ) -> list[TextResourceContents | BlobResourceContents]:
        if not self.session:
            raise RuntimeError("Client session was never initialized.")

        res: ReadResourceResult = await self.session.read_resource(uri)

        return res.contents
