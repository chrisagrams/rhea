from mcp.server.fastmcp import FastMCP
import chromadb
from chromadb.utils import embedding_functions
from sqlalchemy.orm import sessionmaker
from utils.models import Tool as GalaxyTool
from utils.models import Base
from utils.schema import Tool, Param, Inputs
from agent.tool import RheaParam
from sqlalchemy import create_engine
from inspect import Signature, Parameter
from typing import List, Optional
import pickle


DB_PATH = "/Users/chrisgrams/Notes/Argonne/Galaxy-Tools-DB/db/Galaxy_Tools_filtered.db"

engine = create_engine(f"sqlite:///{DB_PATH}")
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

galaxy_tools: dict[str, Tool] = {}

with open("../tools_dict.pkl", "rb") as f:
    galaxy_tools = pickle.load(f)

mcp = FastMCP("Rhea")

openai_ef = embedding_functions.OpenAIEmbeddingFunction(
    api_key="aaaa",
    api_base="http://localhost:8000/v1",
    model_name="Qwen/Qwen3-Embedding-0.6B",
)

chroma_client = chromadb.HttpClient(host="localhost", port=8001)
collection = chroma_client.get_or_create_collection(
    name="rhea-tools-v1.3",
    embedding_function=openai_ef,
)


def dummy_function(input: str) -> str:
    return "echo"


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
                    annotation=Optional[str] if param.optional else str,
                )
            )
        elif param.type == "select":
            res.append(
                Parameter(
                    param.name,
                    kind=Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Optional[str] if param.optional else str,
                )
            )
        elif param.type == "boolean":
            res.append(
                Parameter(
                    param.name,
                    kind=Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Optional[bool] if param.optional else bool,
                )
            )
        elif param.type == "data":
            res.append(
                Parameter(
                    param.name,
                    kind=Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Optional[str] if param.optional else str,
                )
            )
    return res


def process_user_inputs(inputs: Inputs, args: dict) -> List[RheaParam]:
    res = []

    for param in inputs.params:
        a = args.get(param.name, None)
        if a is not None:
            res.append(RheaParam.from_param(param, a))

    return res


@mcp.tool()
def find_tools(query: str) -> str:
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
        sig = Signature(parameters=params, return_annotation=List[str])

        def make_wrapper(tool_id, param_names):
            def wrapper(*args, **kwargs):
                for name, value in zip(param_names, args):
                    kwargs.setdefault(name, value)

                rhea_params = process_user_inputs(tool.inputs, kwargs)

                # TODO: Call agent here
                return ["Hello world!"]

            return wrapper

        fn = make_wrapper(tool.id, [name for name in params])
        fn.__name__ = tool.name
        fn.__doc__ = tool.description
        fn.__signature__ = sig  # type: ignore[attr-defined]

        fn.__annotations__ = {p.name: p.annotation for p in params}
        fn.__annotations__["return"] = List[str]
        mcp.add_tool(fn, name=str(t.name), description=str(t.description))

    session.close()
    return f"Populated {len(retrieved)} tools."


if __name__ == "__main__":
    mcp.run()
