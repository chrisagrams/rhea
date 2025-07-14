import parsl
import os
import pickle
import debugpy
import logging
import chromadb
import anyio
from typing import cast
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.fastmcp.resources.types import TextResource
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from academy.exchange import UserExchangeClient
from academy.handle import UnboundRemoteHandle, RemoteHandle
from academy.exchange.redis import RedisExchangeFactory
from academy.logging import init_logging
from chromadb.utils import embedding_functions
from chromadb.api.types import EmbeddingFunction, Embeddable
from utils.models import Base
from utils.schema import Tool, Inputs
from server.schema import AppContext, MCPOutput, Settings
from agent.schema import RheaParam, RheaOutput
from manager.parsl_config import config
from manager.launch_agent import launch_agent
from inspect import Signature, Parameter
from typing import List, Optional
from proxystore.connectors.redis import RedisKey
from pydantic.networks import AnyUrl

settings = Settings()

if settings.debug_port is not None:
    debugpy.listen(("0.0.0.0", int(settings.debug_port)))
    print(f"Waiting for VS Code to attach on port {int(settings.debug_port)}")
    debugpy.wait_for_client()


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    logger = init_logging(logging.INFO)
    academy_client: Optional[UserExchangeClient] = None

    try:
        chroma_client = chromadb.HttpClient(host=settings.chroma_host, port=settings.chroma_port)
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

        with open(settings.pickle_file, "rb") as f:
            galaxy_tools = pickle.load(f)

        yield AppContext(
            logger=logger,
            chroma_client=chroma_client,
            openai_ef=openai_ef,
            collection=collection,
            factory=factory,
            academy_client=academy_client,
            galaxy_tools=galaxy_tools,
            agents = {}
        )
    except Exception as e:
        logger.error(e)

    finally: # Application shutdown
        if academy_client is not None:
            await academy_client.close()
        parsl.dfk().cleanup()



mcp = FastMCP("Rhea", lifespan=app_lifespan)

# Manually set notification options
lowlevel_server: Server = mcp._mcp_server
lowlevel_server.notification_options.resources_changed = True
lowlevel_server.notification_options.tools_changed = True
    

def construct_params(inputs: Inputs) -> List[Parameter]:
    res = []
    for param in inputs.params:
        res.append(
            param.to_python_parameter()
        )
    return res


def process_user_inputs(inputs: Inputs, args: dict) -> List[RheaParam]:
    res = []

    for param in inputs.params:
        a = args.get(param.name, None)
    
        if a is not None:
            if param.type == "data":
                res.append(RheaParam.from_param(param, RedisKey(a)))
            else:
                res.append(RheaParam.from_param(param, a))

    return res


@mcp.tool(
        name="find_tools",
        title="Find Tools"
)
async def find_tools(query: str, ctx: Context) -> str:
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


    # Populate tools
    for t in retrieved:
        try:
            tool: Tool = ctx.request_context.lifespan_context.galaxy_tools[t]
        except KeyError as e:
            continue

        params = construct_params(tool.inputs)

        # Add Context to tool params
        params.append(
            Parameter(
                "ctx",
                kind=Parameter.POSITIONAL_OR_KEYWORD,
                annotation=Context
            )
        )

        sig = Signature(parameters=params, return_annotation=MCPOutput)

        def make_wrapper(tool_id, param_names):
            async def wrapper(*args, **kwargs):
                # Get context
                ctx: Context = kwargs.pop("ctx")
                ctx.info(f"Launching tool {tool_id}")

                tool: Tool = ctx.request_context.lifespan_context.galaxy_tools[tool_id]

                await ctx.report_progress(0, 1)

                for name, value in zip(param_names, args):
                    kwargs.setdefault(name, value)

                # Construct RheaParams 
                rhea_params = process_user_inputs(tool.inputs, kwargs)

                await ctx.report_progress(0.05, 1)

                # Launch agent
                future_handle = launch_agent(
                    tool,
                    redis_host=settings.agent_redis_host,
                    redis_port=settings.agent_redis_port,
                    minio_endpoint=settings.minio_endpoint,
                    minio_access_key=settings.minio_access_key,
                    minio_secret_key=settings.minio_secret_key,
                    minio_secure=False,
                )

                unbound_handle: UnboundRemoteHandle = future_handle.result()
               
                handle: RemoteHandle = unbound_handle.bind_to_client(ctx.request_context.lifespan_context.academy_client)

                ctx.info(f"Lanched agent {handle.agent_id}")

                ctx.request_context.lifespan_context.agents[tool_id] = handle.agent_id

                ctx.info(f"Executing tool {tool_id} in {handle.agent_id}")
                await ctx.report_progress(0.1, 1)

                # Execute tool
                tool_result: RheaOutput = await ( await handle.run_tool(rhea_params) )

                ctx.info(f"Tool {tool_id} finished in {handle.agent_id}")
                await ctx.report_progress(1, 1)

                result = MCPOutput.from_rhea(tool_result)

                return result

            return wrapper

        # Create tool.call()
        safe_name = tool.name.lower().replace(" ", "_") # Normalize tool name
        fn = make_wrapper(tool.id, [name for name in params])
        fn.__name__ = safe_name
        fn.__doc__ = tool.description
        fn.__signature__ = sig  # type: ignore[attr-defined]

        fn.__annotations__ = {p.name: p.annotation for p in params}
        fn.__annotations__["return"] = MCPOutput

        # Add tool to MCP server
        mcp.add_tool(fn, name=safe_name, title=tool.name, description=tool.description)

        # Add documentation resource to MCP server
        mcp.add_resource(
            resource = TextResource(
                uri=AnyUrl(url=f"resource://documentation/{tool.name}"),
                name=f"{tool.name} Documentation",
                description=f"Full documentation for {tool.name}",
                text=tool.documentation if tool.documentation is not None else f"Documentation for '{tool.name}' is not available.",
                mime_type="text/markdown",
            )
        )

    await ctx.request_context.session.send_tool_list_changed() # notifiactions/tools/list_changed
    await ctx.request_context.session.send_resource_list_changed() # notifications/resources/list_changed
    return f"Populated {len(retrieved)} tools."


async def main():
    async with stdio_server() as (r, w):
        init_opts = lowlevel_server.create_initialization_options(
            lowlevel_server.notification_options,
            {}
        )
        await lowlevel_server.run(r, w, init_opts)

if __name__ == "__main__":
    anyio.run(main)
