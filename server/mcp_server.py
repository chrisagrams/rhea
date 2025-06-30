from mcp.server.fastmcp import FastMCP
import chromadb
from chromadb.utils import embedding_functions
from sqlalchemy.orm import sessionmaker
from utils.models import Tool as GalaxyTool 
from utils.models import Base
from utils.schema import Tool, Param
from sqlalchemy import create_engine
from inspect import Signature, Parameter
import pickle


DB_PATH = "/Users/chrisgrams/Notes/Argonne/Galaxy-Tools-DB/db/Galaxy_Tools_filtered.db"

engine = create_engine(f"sqlite:///{DB_PATH}")
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

with open("tools_dict.pkl", "rb") as f:
    tools = pickle.load(f)

mcp = FastMCP("Rhea")

openai_ef = embedding_functions.OpenAIEmbeddingFunction(
    api_key='aaaa',
    api_base='http://localhost:8000/v1',
    model_name='Qwen/Qwen3-Embedding-0.6B',
)

chroma_client = chromadb.HttpClient(host="localhost", port=8001)
collection = chroma_client.get_or_create_collection(
    name='rhea-tools-v1.3',
    embedding_function=openai_ef,
)


def dummy_function(input: str) -> str:
    return "echo"


@mcp.tool()
def find_tools(query: str) -> str:
    """A tool that will find and populate relevant tools given a query. Once called, the server will populate tools for you."""
    
    # Clear previous tools (except find_tools)
    keep = 'find_tools'
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
    tools = (
        session
        .query(GalaxyTool)
        .filter(GalaxyTool.id.in_(retrieved))
        .all()
    )

    # Populate tools
    for tool in tools:
        def dummy_function(*args, tool_id=tool.id, **kwargs):
            return f"Called tool {tool_id} with {args} {kwargs}"

        mcp.add_tool(
            dummy_function,
            name=tool.name,
            description=tool.description,
        )

    session.close()
    return f"Populated {len(retrieved)} tools."

if __name__ == "__main__":
    mcp.run()
