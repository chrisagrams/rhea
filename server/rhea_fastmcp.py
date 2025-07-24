from mcp.server.fastmcp.tools import ToolManager
from typing import Any
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.server import Context
from mcp.shared.context import LifespanContextT, RequestT
from mcp.server.session import ServerSessionT
from mcp.types import AnyFunction, ToolAnnotations
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.auth.provider import OAuthAuthorizationServerProvider, TokenVerifier
from mcp.server.streamable_http import EventStore
from mcp.server.fastmcp.tools import Tool
from server.utils import create_tool
from utils.schema import Tool as GalaxyTool


class RheaFastMCP(FastMCP):
    def __init__(
        self,
        name: str | None = None,
        instructions: str | None = None,
        auth_server_provider: (
            OAuthAuthorizationServerProvider[Any, Any, Any] | None
        ) = None,
        token_verifier: TokenVerifier | None = None,
        event_store: EventStore | None = None,
        *,
        tools: list[Tool] | None = None,
        **settings: Any,
    ):
        super().__init__(
            name=name,
            instructions=instructions,
            auth_server_provider=auth_server_provider,
            token_verifier=token_verifier,
            event_store=event_store,
            tools=tools,
            **settings,
        )
        self._tool_manager = RheaToolManager(
            tools=tools, warn_on_duplicate_tools=self.settings.warn_on_duplicate_tools
        )


class RheaToolManager(ToolManager):
    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any],
        context: Context[ServerSessionT, LifespanContextT, RequestT] | None = None,
        convert_result: bool = False,
    ):
        if context is None:
            raise RuntimeError(f"'context' is None")
        tool = self.get_tool(name)
        if not tool:
            try:
                galaxy_tools: dict[str, GalaxyTool] = context.request_context.lifespan_context.galaxy_tools  # type: ignore
                galaxy_tool_lookup: dict[str, str] = context.request_context.lifespan_context.galaxy_tool_lookup  # type: ignore
                tool_key = galaxy_tool_lookup[name]
                t: GalaxyTool = galaxy_tools[tool_key]
                tool = create_tool(t, ctx=context)
                self._tools[tool.name] = tool
            except KeyError:
                raise ToolError(f"Unknown tool: { name }")
        return await tool.run(arguments, context=context, convert_result=convert_result)
