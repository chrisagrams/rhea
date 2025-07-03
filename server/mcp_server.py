from mcp.server.fastmcp import FastMCP, Context
from lib.academy.academy.exchange.redis import RedisExchangeFactory
from lib.academy.academy.identifier import AgentId
import chromadb
from chromadb.utils import embedding_functions
from sqlalchemy.orm import sessionmaker
from utils.models import Tool as GalaxyTool
from utils.models import Base
from utils.schema import Tool, Param, Inputs
from agent.tool import RheaToolAgent, RheaParam, RheaOutput, RheaDataOutput
from manager.parsl_config import config
from manager.launch_agent import launch_agent
from sqlalchemy import create_engine
from inspect import Signature, Parameter
from typing import List, Optional, Annotated
from pydantic import BaseModel, Field
from proxystore.connectors.redis import RedisKey
import pickle
import debugpy

debugpy.listen(("0.0.0.0", 5681))
print("Waiting for VS Code to attach on port 5681")
debugpy.wait_for_client()

DB_PATH = "/Users/chrisgrams/Notes/Argonne/Galaxy-Tools-DB/db/Galaxy_Tools_filtered.db"

engine = create_engine(f"sqlite:///{DB_PATH}")
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

galaxy_tools: dict[str, Tool] = {}

with open("../tools_dict.pkl", "rb") as f:
    galaxy_tools = pickle.load(f)

mcp = FastMCP("Rhea")

factory = RedisExchangeFactory("localhost", 6379)
client = factory.bind_as_client(name="mcp-manager")

agents: dict[str, AgentId[RheaToolAgent]] = {}

openai_ef = embedding_functions.OpenAIEmbeddingFunction(
    api_key="aaaa",
    api_base="http://localhost:8000/v1",
    model_name="Qwen/Qwen3-Embedding-0.6B",
)

chroma_client = chromadb.HttpClient(host="localhost", port=8001)
collection = chroma_client.get_or_create_collection(
    name="rhea-tools-v1.3",
    embedding_function=openai_ef, # type: ignore
)



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
    res = collection.query(
        query_texts=[query],
        n_results=10,
    )
    retrieved = res["ids"][0]

    session = SessionLocal()
    tools = session.query(GalaxyTool).filter(GalaxyTool.id.in_(retrieved)).all()

    # Populate tools
    for t in tools:
        try:
            tool = galaxy_tools[str(t.id)]
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
                tool: Tool = galaxy_tools[tool_id]

                # Get context
                ctx: Context = kwargs.pop("ctx")
                ctx.info(f"Launching tool {tool_id}")
                await ctx.report_progress(0, 1)

                for name, value in zip(param_names, args):
                    kwargs.setdefault(name, value)

                # Construct RheaParams 
                rhea_params = process_user_inputs(tool.inputs, kwargs)

                ctx.info(f"Launching agent {agents[tool_id]}")
                await ctx.report_progress(0.05, 1)

                # Launch agent
                launch_agent(
                    agents[tool.id],
                    tool,
                    redis_host="host.docker.internal",
                    redis_port=6379,
                    minio_endpoint="host.docker.internal:9000",
                    minio_access_key="admin",
                    minio_secret_key="password",
                    minio_secure=False,
                )

                # Get agent handle
                handle = client.get_handle(agents[tool.id])

                ctx.info(f"Executing tool {tool_id} in {agents[tool_id]}")
                await ctx.report_progress(0.1, 1)

                # Execute tool
                tool_result: RheaOutput = handle.run_tool(rhea_params).result()

                ctx.info(f"Tool {tool_id} finished in {agents[tool_id]}")
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

        # Register agent
        agents[tool.id] = client.register_agent(RheaToolAgent, name=tool.name)

        # Add tool to MCP server
        mcp.add_tool(fn, name=str(t.name), description=str(t.description))

    await ctx.request_context.session.send_tool_list_changed() # notifiactions/tools/list_changed
    session.close()
    return f"Populated {len(retrieved)} tools."


if __name__ == "__main__":
    mcp.run()
