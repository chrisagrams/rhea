from mcp.server.fastmcp.tools import ToolManager
from typing import Any, List
from collections.abc import Callable
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.server import Context
from mcp.shared.context import LifespanContextT, RequestT
from mcp.server.session import ServerSessionT
from mcp.types import AnyFunction, ToolAnnotations
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.auth.provider import OAuthAuthorizationServerProvider, TokenVerifier
from mcp.server.streamable_http import EventStore
from mcp.server.fastmcp.tools import Tool
from mcp.server.fastmcp.resources.base import Resource
from mcp.types import Tool as MCPTool
from mcp.server.fastmcp.utilities.logging import get_logger
from server.utils import create_tool
from utils.schema import Tool as GalaxyTool
from redis import Redis
from pydantic import BaseModel
from server.client_manager import ClientManager, ClientState

logger = get_logger(__name__)


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
        self._mcp_server.notification_options.resources_changed = True
        self._mcp_server.notification_options.tools_changed = True

    def _setup_handlers(self) -> None:
        super()._setup_handlers()
        self._mcp_server.list_tools()(self.list_tools)  # Override list_tools

    def add_tool_to_context(
        self,
        fn: AnyFunction,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        annotations: ToolAnnotations | None = None,
        structured_output: bool | None = None,
    ) -> None:
        context = self.get_context()
        if context is None:
            raise RuntimeError("Context is None in `add_tool_to_context()`")
        else:
            self._tool_manager.add_tool_to_context(
                fn,
                context=context,
                name=name,
                title=title,
                description=description,
                annotations=annotations,
                structured_output=structured_output,
            )

    async def list_tools(self) -> list[MCPTool]:
        context = self.get_context()
        if context is None:
            raise RuntimeError("Context is None in `list_tools()`")
        tools = self._tool_manager.list_tools(context=context)
        return [
            MCPTool(
                name=info.name,
                title=info.title,
                description=info.description,
                inputSchema=info.parameters,
                outputSchema=info.output_schema,
                annotations=info.annotations,
            )
            for info in tools
        ]


class RheaToolManager(ToolManager):
    def add_tool_to_context(
        self,
        fn: Callable[..., Any],
        context: Context[ServerSessionT, LifespanContextT, RequestT] | None = None,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        annotations: ToolAnnotations | None = None,
        structured_output: bool | None = None,
    ) -> Tool:
        tool = Tool.from_function(
            fn,
            name=name,
            title=title,
            description=description,
            annotations=annotations,
            structured_output=structured_output,
        )

        # If client context is available, add the tool only to that client's context.
        if (
            context is not None
            and context.request_context.request is not None
            and (request := context.request_context.request) is not None
            and (headers := request.headers) is not None  # type: ignore
            and (session_id := headers.get("mcp-session-id")) is not None
        ):
            client_manager: ClientManager = context.request_context.lifespan_context.client_manager  # type: ignore
            client_state: ClientState = client_manager.get_client_state(session_id)
            existing = client_state._tools.get(tool.name)
            if existing:
                if self.warn_on_duplicate_tools:
                    logger.warning(f"Tool already exists: {tool.name}")
                return existing
            client_state._tools[tool.name] = tool
            client_manager.set_client_state(
                session_id, tools=client_state._tools, resources=client_state._resources
            )
        else:
            existing = self._tools.get(tool.name)
            if existing:
                if self.warn_on_duplicate_tools:
                    logger.warning(f"Tool already exists: {tool.name}")
                return existing
            self._tools[tool.name] = tool

        return tool

    def list_tools(
        self, context: Context[ServerSessionT, LifespanContextT, RequestT] | None = None
    ) -> List[Tool]:
        # Get tools available to all clients
        tools: List[Tool] = list(self._tools.values())

        # Get client specific tools
        if context is not None:
            request: Any | None = context.request_context.request

            if request is not None:
                # Get session ID (if exists)
                headers: dict = request.headers
                session_id: str | None = headers.get("mcp-session-id")

                if session_id is not None:
                    client_manager: ClientManager = context.request_context.lifespan_context.client_manager  # type: ignore
                    client_state: ClientState = client_manager.get_client_state(
                        session_id
                    )
                    tools = tools + (list(client_state._tools.values()))

        return tools

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
            except KeyError:
                raise ToolError(f"Unknown tool: { name }")
        return await tool.run(arguments, context=context, convert_result=convert_result)
