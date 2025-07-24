import parsl
import pickle
import debugpy
import logging
import chromadb
import anyio
from typing import cast
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from mcp.server.fastmcp import Context
from mcp.server.fastmcp.resources.types import TextResource
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.server.sse import SseServerTransport
from mcp.server.fastmcp.tools import Tool as FastMCPTool
from academy.exchange import UserExchangeClient
from academy.exchange.redis import RedisExchangeFactory
from academy.logging import init_logging
from chromadb.utils import embedding_functions
from chromadb.api.types import EmbeddingFunction, Embeddable
from server.rhea_fastmcp import RheaFastMCP
from utils.models import Base
from utils.schema import Tool
from server.schema import (
    AppContext,
    MCPTool,
    Settings,
    PBSSettings,
)
from server.utils import create_tool
from manager.parsl_config import generate_parsl_config
from parsl.errors import ConfigurationError, NoDataFlowKernelError
from manager.launch_agent import launch_agent
from typing import List, Optional
from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from pydantic.networks import AnyUrl
from pydantic import ValidationError
from argparse import ArgumentParser
from pathlib import Path


parser = ArgumentParser()
parser.add_argument(
    "--transport",
    choices=("stdio", "sse"),
    default="stdio",
    help="Transport protocol to run (stdio or sse)",
)
args = parser.parse_args()

settings = Settings()

pbs_settings = None
if Path(".env_pbs").exists():
    try:
        pbs_settings = PBSSettings()  # type: ignore
    except ValidationError:
        pbs_settings = None


if settings.debug_port is not None:
    debugpy.listen(("0.0.0.0", int(settings.debug_port)))
    print(f"Waiting for VS Code to attach on port {int(settings.debug_port)}")
    debugpy.wait_for_client()


@asynccontextmanager
async def app_lifespan(server: RheaFastMCP) -> AsyncIterator[AppContext]:
    logger = init_logging(logging.INFO)
    academy_client: Optional[UserExchangeClient] = None

    try:
        # Prevent double-loading DFK
        try:
            parsl.load(
                generate_parsl_config(
                    backend=settings.parsl_container_backend,
                    network=settings.parsl_container_network,
                    provider=settings.parsl_provider,
                    max_workers_per_node=settings.parsl_max_workers_per_node,
                    init_blocks=settings.parsl_init_blocks,
                    min_blocks=settings.parsl_min_blocks,
                    max_blocks=settings.parsl_max_blocks,
                    nodes_per_block=settings.parsl_nodes_per_block,
                    parallelism=settings.parsl_parallelism,
                    debug=settings.parsl_container_debug,
                    pbs_settings=pbs_settings,
                )
            )
        except ConfigurationError:
            pass

        chroma_client = chromadb.HttpClient(
            host=settings.chroma_host, port=settings.chroma_port
        )
        openai_ef = embedding_functions.OpenAIEmbeddingFunction(
            api_key=settings.vllm_key,
            api_base=settings.vllm_url,
            model_name=settings.model,
        )

        ef = cast(EmbeddingFunction[Embeddable], openai_ef)

        if settings.chroma_collection is not None and ef is not None:
            collection = chroma_client.get_or_create_collection(
                name=settings.chroma_collection,
                embedding_function=ef,
            )
        else:
            raise ValueError(
                "CHROMA_COLLECTION must be set (got None); cannot initialize ChromaDB collection"
            )

        factory = RedisExchangeFactory(settings.redis_host, settings.redis_port)
        academy_client = await factory.create_user_client(name="rhea-manager")

        connector = RedisConnector(settings.redis_host, settings.redis_port)
        output_store = Store("rhea-output", connector=connector, register=True)

        with open(settings.pickle_file, "rb") as f:
            galaxy_tools = pickle.load(f)

            galaxy_tool_lookup = {}
            for tool_id, tool in galaxy_tools.items():
                galaxy_tool_lookup[tool.name] = tool_id

        yield AppContext(
            settings=settings,
            logger=logger,
            chroma_client=chroma_client,
            openai_ef=openai_ef,
            collection=collection,
            factory=factory,
            connector=connector,
            output_store=output_store,
            academy_client=academy_client,
            galaxy_tools=galaxy_tools,
            galaxy_tool_lookup=galaxy_tool_lookup,
            agents={},
        )
    except Exception as e:
        logger.error(e)

    finally:  # Application shutdown
        if academy_client is not None:
            await academy_client.close()
        parsl.clear()
        try:
            parsl.dfk().cleanup()
        except NoDataFlowKernelError:
            pass


mcp = RheaFastMCP("Rhea", lifespan=app_lifespan)

# Manually set notification options
lowlevel_server: Server = mcp._mcp_server


@mcp.tool(name="find_tools", title="Find Tools")
async def find_tools(query: str, ctx: Context) -> List[MCPTool]:
    """A tool that will find and populate relevant tools given a query. Once called, the server will populate tools for you."""

    # Clear previous tools (except find_tools)
    keep = "find_tools"
    for t in list(mcp._tool_manager._tools.keys()):
        if t != keep:
            mcp._tool_manager._tools.pop(t)

    # Clear previous tool documentations
    for r in list(mcp._resource_manager._resources.keys()):
        if "Documentation" in r:
            mcp._resource_manager._resources.pop(r)

    # Perform RAG
    res = ctx.request_context.lifespan_context.collection.query(
        query_texts=[query],
        n_results=10,
    )
    retrieved = res["ids"][0]

    result = []

    # Populate tools
    for t in retrieved:
        try:
            tool: Tool = ctx.request_context.lifespan_context.galaxy_tools[t]
            tool_function: FastMCPTool = create_tool(tool, ctx)
        except KeyError as e:
            continue

        # Add tool to MCP server
        mcp.add_tool(
            fn=tool_function.fn,
            name=tool_function.name,
            title=tool_function.title,
            description=tool_function.description,
        )

        # Add documentation resource to MCP server
        mcp.add_resource(
            resource=TextResource(
                uri=AnyUrl(url=f"resource://documentation/{tool.name}"),
                name=f"{tool.name} Documentation",
                description=f"Full documentation for {tool.name}",
                text=(
                    tool.documentation
                    if tool.documentation is not None
                    else f"Documentation for '{tool.name}' is not available."
                ),
                mime_type="text/markdown",
            )
        )

        # Add MCPTool to result
        result.append(MCPTool.from_rhea(tool))

    await ctx.request_context.session.send_tool_list_changed()  # notifiactions/tools/list_changed
    await ctx.request_context.session.send_resource_list_changed()  # notifications/resources/list_changed

    return result


async def serve_stdio():
    async with stdio_server() as (r, w):
        init_opts = lowlevel_server.create_initialization_options(
            lowlevel_server.notification_options, {}
        )
        await lowlevel_server.run(r, w, init_opts)


async def serve_sse():
    import uvicorn
    from starlette.applications import Starlette
    from starlette.routing import Route, Mount
    from starlette.responses import Response

    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        async with sse.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await lowlevel_server.run(
                streams[0],
                streams[1],
                lowlevel_server.create_initialization_options(
                    lowlevel_server.notification_options
                ),
            )
        return Response()

    app = Starlette(
        routes=[
            Route("/sse", endpoint=handle_sse, methods=["GET"]),
            Mount("/messages/", app=sse.handle_post_message),
        ]
    )
    config = uvicorn.Config(app, host=settings.host, port=settings.port)
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    match args.transport:
        case "stdio":
            await serve_stdio()
        case "sse":
            await serve_sse()


if __name__ == "__main__":
    anyio.run(main)
