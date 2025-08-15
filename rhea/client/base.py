from __future__ import annotations
from abc import ABC, abstractmethod

from pathlib import Path
from mcp.types import Resource, Tool, BlobResourceContents, TextResourceContents
from typing import Any
from pydantic import AnyUrl


class RheaMCPClientBase(ABC):
    """Base interface for MCP client"""

    @abstractmethod
    async def __aenter__(self) -> RheaMCPClientBase:
        """
        Async context manager entry point.

        Allows using this client as an async context manager with the 'async with' statement.

        Example:
        ```
            async with RheaMCPClient("localhost", 3001) as client:
                tool_list = await client.find_tools("I need a tool to convert FASTA to FASTQ")
        ```

        Returns:
            RheaMCPClient: The initialized client instance.
        """
        pass

    @abstractmethod
    async def __aexit__(self, exc_type, exc, tb):
        """
        Async context manager exit point.

        Ensures proper cleanup when exiting the 'async with' context.
        """
        pass

    @abstractmethod
    async def list_tools(self) -> list[Tool]:
        """
        List the currently available tools on the server for this session.

        Returns:
            list[Tool]: A list of MCP tool definitions.
        """
        pass

    @abstractmethod
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
            RuntimeError: If the client session fails to initialize or used outside of a context manager.
        """
        pass

    @abstractmethod
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
            RuntimeError: If the client session fails to initialize or used outside of a context manager.
        """
        pass

    @abstractmethod
    async def list_resources(self) -> list[Resource]:
        """
        List all available resources from the Rhea MCP server.

        This asynchronous method retrieves a list of all resources accessible through
        the initialized Rhea client. The client must have an active session before
        calling this method.

        Returns:
            ListResourcesResult: A result object containing the list of available resources.
        Raises:
            RuntimeError: If the client session fails to initialize or used outside of a context manager.
        """
        pass

    @abstractmethod
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
            RuntimeError: If the client session fails to initialize or used outside of a context manager.
        """
        pass


class RheaRESTClientBase(ABC):
    """Base interface for REST client"""

    @abstractmethod
    async def __aenter__(self) -> RheaRESTClientBase:
        """
        Async context manager entry point.

        Allows using this client as an async context manager with the 'async with' statement.

        Example:
        ```
            async with RheaRESTClient("localhost", 3001) as client:
                result = await client.upload_file("test.txt")
        ```
        """
        pass

    @abstractmethod
    async def __aexit__(self, exc_type, exc, tb):
        """
        TODO: write docstring
        """
        pass

    @abstractmethod
    async def upload_file(
        self,
        path: str,
        name: str | None = None,
        timeout: int = 300,
        chunk_size: int = 1 << 20,
    ) -> dict:
        """
        Upload a file from local directory to Rhea MCP server.

        Args:
            path (str): Path of local file to upload.
            name (str, optional): Optional filename to use on the server side. Defaults to source filename.
            timeout (int, optional): Request timeout in seconds. Defaults to `300` seconds.
            chunk_size (int, optional): Chunk size to read and upload file. Defaults to 1MB.

        Returns:
            dict: Server response

        Raises:
            RuntimeError: If the client session fails to initialize or used outside of a context manager.
        """
        pass

    @abstractmethod
    async def download_file(
        self,
        key: str,
        output_directory: Path = Path.cwd(),
        timeout: int = 300,
        chunk_size: int = 1 << 20,
    ) -> int:
        """
        Download a file to local directory from Rhea MCP server.

        Args:
            key (str): File key of desired file to download.
            output_directory (Path, optional): Output directory to write to. Defaults to current working directory.
            timeout (int, optional): Request timeout in seconds. Defaults to `300` seconds.
            chunk_size (int, optional): Chunk size for download stream. Defaults to 1MB.

        Returns:
            int: Size of downloaded file in bytes.

        Raises:
            RuntimeError: If the client session fails to initialize or used outside of a context manager.
        """
        pass

    @abstractmethod
    async def metrics(self) -> dict[str, list[dict]]:
        """
        Get Prometheus metrics from Rhea MCP server.

        See: [Prometheus Specification](https://prometheus.io/docs/specs/om/open_metrics_spec/)

        Returns:
            dict[str, list[dict]]: Server metrics

        Raises:
            RuntimeError: If the client session fails to initialize or used outside of a context manager.
        """
        pass
