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

from client.rest import RheaRESTClient

from urllib.parse import urlunparse, urljoin


class RheaMCPClient:
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
        """
        Async context manager entry point.

        Allows using this client as an async context manager with the 'async with' statement.

        Example:
        ```
            async with RheaMCPClient(url="http://localhost:3001/mcp") as client:
                tool_list = await client.find_tools("I need a tool to convert FASTA to FASTQ")
        ```

        Returns:
            RheaMCPClient: The initialized client instance.
        """
        self.http_client = streamablehttp_client(self._url("mcp"))
        self.read, self.write, _ = await self.http_client.__aenter__()
        self.session = ClientSession(self.read, self.write)
        await self.session.__aenter__()
        await self.session.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        Async context manager exit point.

        Ensures proper cleanup when exiting the 'async with' context.
        """
        if self.session:
            await self.session.__aexit__(exc_type, exc, tb)
        if self.http_client:
            await self.http_client.__aexit__(exc_type, exc, tb)

    async def list_tools(self) -> list[Tool]:
        """
        List the currently available tools on the server for this session.
        """
        if not self.session:
            raise RuntimeError("Client session was never initialized.")

        res: ListToolsResult = await self.session.list_tools()

        return res.tools

    async def find_tools(self, query: str) -> list[dict]:
        """
        Find available tools on the MCP server that match the query.

        This method searches for tools matching the provided query string
        and returns their descriptions.

        Args:
            query (str): The search query to find relevant tools.

        Returns:
            list[dict]: A list of tool descriptions matching the query.

        Raises:
            RuntimeError: If the client session fails to initialize.
        """
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
        """
        Call a specific tool on the MCP server with the given arguments.

        Args:
            name (str): The name of the tool to call.
            arguments (dict[str, Any]): The arguments to pass to the tool.

        Returns:
            dict | None: The structured content of the tool's response,
                         or None if there is no structured content.

        Raises:
            RuntimeError: If the client session fails to initialize.
        """

        if not self.session:
            raise RuntimeError("Client session was never initialized.")

        res: CallToolResult = await self.session.call_tool(name, arguments)

        if res.isError:
            print(f"Error occured calling tool: {res.content}")

        return res.structuredContent

    async def list_resources(self) -> list[Resource]:
        """
        List all available resources from the Rhea MCP server.
        This asynchronous method retrieves a list of all resources accessible through
        the initialized Rhea client. The client must have an active session before
        calling this method.
        Returns:
            ListResourcesResult: A result object containing the list of available resources.
        Raises:
            RuntimeError: If the client session has not been initialized.
        """
        if not self.session:
            raise RuntimeError("Client session was never initialized.")

        res: ListResourcesResult = await self.session.list_resources()

        return res.resources

    async def read_resource(
        self, uri: AnyUrl
    ) -> list[TextResourceContents | BlobResourceContents]:
        """
        Read a specific resource from the Rhea MCP server by its URI.

        This method retrieves the contents of a resource identified by the provided URI.
        The resource contents can be either text or binary data.

        Args:
            uri (AnyUrl): The URI of the resource to read.

        Returns:
            list[TextResourceContents | BlobResourceContents]: A list of resource contents,
                which can be either text or binary data.

        Raises:
            RuntimeError: If the client session has not been initialized.
        """
        if not self.session:
            raise RuntimeError("Client session was never initialized.")

        res: ReadResourceResult = await self.session.read_resource(uri)

        return res.contents
