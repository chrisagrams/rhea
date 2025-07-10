from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import cast
from mcp.server.fastmcp import FastMCP, Context
from academy.exchange import UserExchangeClient
from academy.handle import UnboundRemoteHandle, RemoteHandle
from academy.exchange.redis import RedisExchangeFactory
from academy.identifier import AgentId
from academy.logging import init_logging
import chromadb
from chromadb.utils import embedding_functions
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
from chromadb.api import ClientAPI
from chromadb.api.models.Collection import Collection
from chromadb.api.types import EmbeddingFunction, Embeddable
from utils.models import Tool as GalaxyTool
from utils.models import Base
from utils.schema import Tool, Param, Inputs
from agent.tool import RheaToolAgent, RheaParam, RheaOutput, RheaDataOutput
from manager.parsl_config import config
from manager.launch_agent import launch_agent
from inspect import Signature, Parameter
from typing import List, Optional, Annotated
from pydantic import BaseModel, Field
from proxystore.connectors.redis import RedisKey
from dotenv import load_dotenv
import parsl
import os
import pickle
import debugpy
import logging
from logging import Logger

load_dotenv()

debug_port = os.environ.get("DEBUG_PORT")
pickle_file = os.environ.get("PICKLE_FILE", "../tools_dict.pkl")
redis_host = os.environ.get("REDIS_HOST", "localhost")
redis_port = int(os.environ.get("REDIS_PORT", "6379"))
vllm_key = os.environ.get("VLLM_KEY", "abc123")
vllm_url = os.environ.get("VLLM_URL", "http://localhost:8000/v1")
model = os.environ.get("MODEL", "Qwen/Qwen3-Embedding-0.6B")
chroma_host = os.environ.get("CHROMA_HOST", "localhost")
chroma_port = int(os.environ.get("CHROMA_PORT", "8001"))
chroma_collection = os.environ.get("CHROMA_COLLECTION")
agent_redis_host = os.environ.get("AGENT_REDIS_HOST", "localhost")
agent_redis_port = int(os.environ.get("AGENT_REDIS_PORT", "6379"))
minio_endpoint = os.environ.get("MINIO_ENDPOINT", "localhost")
minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

if debug_port:
    debugpy.listen(("0.0.0.0", int(debug_port)))
    print(f"Waiting for VS Code to attach on port {int(debug_port)}")
    debugpy.wait_for_client()

@dataclass
class AppContext:
    logger: Logger
    chroma_client: ClientAPI
    openai_ef: OpenAIEmbeddingFunction
    collection: Collection
    factory: RedisExchangeFactory
    academy_client: UserExchangeClient
    galaxy_tools: dict[str, Tool]
    agents: dict[str, AgentId[RheaToolAgent]]
  

@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    logger = init_logging(logging.INFO)
    academy_client: Optional[UserExchangeClient] = None

    try:
        chroma_client = chromadb.HttpClient(host=chroma_host, port=chroma_port)
        openai_ef = embedding_functions.OpenAIEmbeddingFunction(
            api_key=vllm_key,
            api_base=vllm_url,
            model_name=model,
        )

        ef = cast(EmbeddingFunction[Embeddable], openai_ef)

        if chroma_collection is not None and ef is not None:
            collection = chroma_client.get_or_create_collection(
                name=chroma_collection,
                embedding_function=ef,
            )
        else:
            raise ValueError(
                "CHROMA_COLLECTION must be set (got None); cannot initialize ChromaDB collection"
            ) 
        
        factory = RedisExchangeFactory(redis_host, redis_port)
        academy_client = await factory.create_user_client(name="rhea-manager")

        with open(pickle_file, "rb") as f:
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


class MCPDataOutput(BaseModel):
    key: str
    size: int
    filename: str
    name: Optional[str] = None

    @classmethod
    def from_rhea(cls, p: RheaDataOutput):
        return cls(
            key=p.key.redis_key,
            size=p.size,
            filename=p.filename,
            name=p.name
        )


class MCPOutput(BaseModel):
    return_code: int
    stdout: str
    stderr: str
    files: Optional[List[MCPDataOutput]] = None

    @classmethod
    def from_rhea(cls, p: RheaOutput):
        files = None
        if p.files is not None:
            files = []
            for f in p.files:
                files.append(MCPDataOutput.from_rhea(f))
        return cls(
            return_code=p.return_code,
            stdout=p.stdout,
            stderr=p.stderr,
            files=files
        )
    


def construct_params(inputs: Inputs) -> List[Parameter]:
    res = []
    for param in inputs.params:
        if (param.name is None or param.name == "") and param.argument is not None:
            param.name = param.argument.replace("--", "")

        if param.name is None:
            continue

        if param.type == "text":
            res.append(
                Parameter(
                    param.name,
                    kind=Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Annotated[
                        Optional[str] if param.optional else str,
                        Field(description=param.description)
                    ]
                )
            )
        elif param.type == "select":
            res.append(
                Parameter(
                    param.name,
                    kind=Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Annotated[
                        Optional[str] if param.optional else str,
                        Field(description=param.description)
                    ]
                )
            )
        elif param.type == "boolean":
            res.append(
                Parameter(
                    param.name,
                    kind=Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Annotated[
                        Optional[bool] if param.optional else bool,
                        Field(description=param.description)
                    ],
                )
            )
        elif param.type == "data":
            res.append(
                Parameter(
                    param.name,
                    kind=Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Annotated[
                        Optional[str] if param.optional else str,
                        Field(description=param.description)
                    ],
                )
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


@mcp.tool()
async def find_tools(query: str, ctx: Context) -> str:
    """A tool that will find and populate relevant tools given a query. Once called, the server will populate tools for you."""

    # Clear previous tools (except find_tools)
    keep = "find_tools"
    for t in list(mcp._tool_manager._tools.keys()):
        if t != keep:
            mcp._tool_manager._tools.pop(t)

    # Perform RAG
    res = ctx.request_context.lifespan_context.collection.query(
        query_texts=[query],
        n_results=10,
    )
    retrieved = res["ids"][0]


    # Populate tools
    for t in retrieved:
        try:
            tool = ctx.request_context.lifespan_context.galaxy_tools[t]
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
                    redis_host=agent_redis_host,
                    redis_port=agent_redis_port,
                    minio_endpoint=minio_endpoint,
                    minio_access_key=minio_access_key,
                    minio_secret_key=minio_secret_key,
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
        fn = make_wrapper(tool.id, [name for name in params])
        fn.__name__ = tool.name
        fn.__doc__ = tool.description
        fn.__signature__ = sig  # type: ignore[attr-defined]

        fn.__annotations__ = {p.name: p.annotation for p in params}
        fn.__annotations__["return"] = MCPOutput

        # Add tool to MCP server
        mcp.add_tool(fn, name=tool.name, description=tool.description)

    await ctx.request_context.session.send_tool_list_changed() # notifiactions/tools/list_changed
    return f"Populated {len(retrieved)} tools."


if __name__ == "__main__":
    mcp.run()
