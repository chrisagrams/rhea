import debugpy
import logging
import anyio
import uuid
import time
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Optional, Any


# MCP SDK imports
from mcp.server.fastmcp import Context
from mcp.server.fastmcp.resources.types import TextResource
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.server.sse import SseServerTransport
from mcp.server.fastmcp.tools import Tool as FastMCPTool

# Starlette imports
from starlette.requests import Request
from starlette.responses import Response, JSONResponse, StreamingResponse

# Parsl imports
import parsl
from parsl import DataFlowKernel

# Academy imports
from academy.exchange import UserExchangeClient
from academy.exchange.redis import RedisExchangeFactory
from academy.logging import init_logging

# Embedding imports
from openai import OpenAI

# Helper imports
from rhea.server.rhea_fastmcp import RheaFastMCP
from rhea.server.client_manager import LocalClientManager, ClientManager
from rhea.server.schema import AppContext, MCPTool, Settings, PBSSettings, K8Settings
from rhea.server.utils import create_tool
import rhea.server.metrics as metrics
from rhea.utils.schema import Tool
from rhea.utils.embedding import get_embedding, get_l2_distance
from rhea.utils.proxy import RheaFileHandle, RheaFileProxy
from rhea.manager.parsl_config import generate_parsl_config

# ProxyStore imports
from proxystore.connectors.redis import RedisConnector, RedisKey
from proxystore.store import StoreConfig, get_or_create_store
from proxystore.store.config import ConnectorConfig
from proxystore.store.exceptions import StoreExistsError
import cloudpickle

# Pydantic + SQLAlchemy imports
from pydantic.networks import AnyUrl
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)

# Prometheus imports
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.core import REGISTRY


parser = ArgumentParser()
parser.add_argument(
    "--transport",
    choices=("stdio", "sse", "streamable-http"),
    default="stdio",
    help="Transport protocol to run (stdio, sse, or streamable-http)",
)
args = parser.parse_args()

settings = Settings()

pbs_settings: PBSSettings | None = None
k8_settings: K8Settings | None = None

if Path(".env_pbs").exists():
    try:
        pbs_settings = PBSSettings()  # type: ignore
    except ValidationError:
        pbs_settings = None

if Path(".env_k8").exists():
    try:
        k8_settings = K8Settings()
    except ValidationError:
        k8_settings = None


if settings.debug_port is not None:
    debugpy.listen(("0.0.0.0", int(settings.debug_port)))
    print(f"Waiting for VS Code to attach on port {int(settings.debug_port)}")
    debugpy.wait_for_client()


# A 'run_id', generated on the begining of application startup, to keep track of which handles are stale
run_id = str(uuid.uuid4())

connector = RedisConnector(settings.redis_host, settings.redis_port)

input_store = get_or_create_store(
    StoreConfig(
        name="rhea-input",
        connector=ConnectorConfig(kind="redis", options=connector.config()),
        serializer=cloudpickle.dumps,
        deserializer=cloudpickle.loads,
        cache_size=16,
        metrics=True,
        populate_target=True,
        auto_register=True,
    )
)

output_store = get_or_create_store(
    StoreConfig(
        name="rhea-output",
        connector=ConnectorConfig(kind="redis", options=connector.config()),
        serializer=cloudpickle.dumps,
        deserializer=cloudpickle.loads,
        cache_size=16,
        metrics=True,
        populate_target=True,
        auto_register=True,
    )
)

client_manager = LocalClientManager(client_ttl=settings.client_ttl)

factory = RedisExchangeFactory(settings.redis_host, settings.redis_port)

engine: AsyncEngine = create_async_engine(
    settings.database_url, echo=False, future=True
)
AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)

REGISTRY.register(metrics.RedisHashCollector(connector._redis_client, "conda_envs"))


@asynccontextmanager
async def app_lifespan(server: RheaFastMCP) -> AsyncIterator[AppContext]:
    # Initialize on each new connection
    logger = init_logging(logging.INFO)

    academy_client: Optional[UserExchangeClient] = None
    try:
        embedding_client = OpenAI(
            base_url=settings.embedding_url, api_key=settings.embedding_key
        )

        academy_client = await factory.create_user_client(
            name=f"rhea-manager-{str(uuid.uuid4())}"
        )

        yield AppContext(
            settings=settings,
            logger=logger,
            embedding_client=embedding_client,
            db_sessionmaker=AsyncSessionLocal,
            factory=factory,
            connector=connector,
            output_store=output_store,
            academy_client=academy_client,
            agents={},
            client_manager=client_manager,
            resource_manager=mcp._resource_manager,
            run_id=run_id,
        )

    except Exception as e:
        logger.error(e)

    finally:  # Application shutdown
        if academy_client is not None:
            await academy_client.close()


mcp = RheaFastMCP(
    "Rhea",
    lifespan=app_lifespan,
    host=settings.host,
    port=settings.port,
)

# Manually set notification options
lowlevel_server: Server = mcp._mcp_server


@mcp.tool(name="find_tools", title="Find Tools")
async def find_tools(query: str, ctx: Context) -> List[MCPTool]:
    """A tool that will find and populate relevant tools given a query. Once called, the server will populate tools for you."""

    # Increment Prometheus counter metric
    metrics.find_tools_request_count.inc()

    start_time = time.time()

    # Get session ID (if exists)
    session_id: str | None = None
    request: Any | None = ctx.request_context.request

    if request is not None:
        headers: dict = request.headers
        session_id: str | None = headers.get("mcp-session-id")

    # Get ClientManager
    client_manager: ClientManager = ctx.request_context.lifespan_context.client_manager

    # Clear previous tools (except find_tools)
    keep = "find_tools"
    for t in list(mcp._tool_manager._tools.keys()):
        if t != keep:
            mcp._tool_manager._tools.pop(t)
    if session_id is not None:
        client_manager.clear_client_tools(session_id)

    # Clear previous tool documentations
    for r in list(mcp._resource_manager._resources.keys()):
        if "Documentation" in r:
            mcp._resource_manager._resources.pop(r)

    # Get embedding of user query
    query_vector: List[float] = get_embedding(
        query, ctx.request_context.lifespan_context.embedding_client, settings.model
    )

    # Perform RAG
    db_sessionmaker: async_sessionmaker[AsyncSession] = (
        ctx.request_context.lifespan_context.db_sessionmaker
    )

    async with db_sessionmaker() as session:
        tools: List[Tool] = await get_l2_distance(query_vector, session, limit=10)

    result = []

    # Populate tools
    for t in tools:
        tool_function: FastMCPTool = create_tool(t, ctx)

        # Add tool to MCP server
        mcp.add_tool_to_context(
            fn=tool_function.fn,
            name=tool_function.name,
            title=tool_function.title,
            description=tool_function.description,
        )

        # Add documentation resource to MCP server
        mcp.add_resource_to_context(
            resource=TextResource(
                uri=AnyUrl(url=f"resource://documentation/{t.name}"),
                name=f"{t.name} Documentation",
                description=f"Full documentation for {t.name}",
                text=(
                    t.documentation
                    if t.documentation is not None
                    else f"Documentation for '{t.name}' is not available."
                ),
                mime_type="text/markdown",
            )
        )

        # Add MCPTool to result
        result.append(MCPTool.from_rhea(t))

    await ctx.request_context.session.send_tool_list_changed()  # notifiactions/tools/list_changed
    await ctx.request_context.session.send_resource_list_changed()  # notifications/resources/list_changed

    metrics.find_tool_request_latency.observe(
        time.time() - start_time
    )  # Log request latency

    return result


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


@mcp.custom_route("/metrics", methods=["GET"])
async def metrics_endpoint(request: Request):
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@mcp.custom_route("/upload", methods=["POST"])
async def upload(request: Request):
    metrics.upload_requests.inc()  # Update upload metric

    file_handle: RheaFileHandle = RheaFileHandle(r=connector._redis_client)

    async for chunk in request.stream():
        file_handle.append(chunk)

    format = file_handle.filetype()
    filename = request.headers.get("x-filename", file_handle.key)

    filesize = len(file_handle)

    proxy = RheaFileProxy(
        name=filename,
        format=format,
        filename=filename,
        filesize=filesize,
        file_key=file_handle.key,
    )

    key = proxy.to_proxy(input_store)

    response = proxy.model_dump()
    response["key"] = key

    metrics.upload_size.observe(filesize)  # Observe uploaded filesize metric

    return JSONResponse(response)


@mcp.custom_route("/download", methods=["GET"])
async def download(request: Request):
    metrics.download_requests.inc()  # Upload download metric

    key = request.query_params.get("key")
    if not key:
        return JSONResponse({"error": "Missing 'key' parameter"}, status_code=400)

    try:
        proxy: RheaFileProxy = RheaFileProxy.from_proxy(
            RedisKey(redis_key=key), input_store
        )
    except ValueError:
        proxy: RheaFileProxy = RheaFileProxy.from_proxy(
            RedisKey(redis_key=key), output_store
        )

    file_handle: RheaFileHandle = proxy.open(connector._redis_client)

    metrics.download_size.observe(
        len(file_handle)
    )  # Observe downloaded filesize metric

    async def file_iterator():
        for chunk in file_handle.iter_chunks(8192):
            yield chunk

    return StreamingResponse(
        file_iterator(),
        media_type=proxy.format,
        headers={"Content-Disposition": f'attachment; filename="{proxy.filename}"'},
    )


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
    try:
        dfk: DataFlowKernel = parsl.load(
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
                k8_settings=k8_settings,
            )
        )

        # Register Prometheus Parsl collector
        REGISTRY.register(metrics.ParslCollector(dfk))

        match args.transport:
            case "stdio":
                await serve_stdio()
            case "sse":
                await serve_sse()
            case "streamable-http":
                await mcp.run_streamable_http_async()  # TODO: Fix notification options
    finally:
        parsl.dfk().cleanup()
        print("Application shutdown complete.")


if __name__ == "__main__":
    anyio.run(main)
