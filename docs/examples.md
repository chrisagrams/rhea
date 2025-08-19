# Examples
Rhea is a MCP-compliant server that can be used across a variety of LLM clients and libraries to perform dynamic discovery and execution of *thousands* of tools. While all MCP functionality can be performed with any MCP-compatible client libary, we provide a client libary to provide a simple interface with Rhea's tool workflow and file management.

The client can be installed from PyPI using the `uv` package manager:

```bash
uv add rhea-mcp
```

Or with `pip`:

``` bash
pip install rhea-mcp
```

## Tool Calling
Tool calling with Rhea is performed exclusively over the MCP protocol to provide universal support across many LLM clients. The Rhea client library provides several helper functions on-top of the FastMCP client libary for ease of use.

### Finding Tools
A major feature of Rhea is the ability to dynamically propogate the tools made available on the server via Retrieval Augmented Generation (RAG). This is achieved by providing a natural language query to the `find_tools()` MCP tool which will perform a server-side RAG and populate the tools for this client session.

#### Using Rhea Client Library

The following script accepts a natural language query, such as *"I need a tool to convert FASTA to FASTQ"*, and returns the relevant tools for this query.

This script can be found in [examples/find_tools.py](https://github.com/chrisagrams/rhea/blob/main/examples/find_tools.py)

``` python
--8<-- "examples/find_tools.py"
```

1. Open a connection with the MCP server with an asynchronous context manager.
2. Call `find_tools()` with a natural language query string.
3. List the tools avaiable on the server for this client.
4. Make sure to run as a coroutine.


#### Using FastMCP

``` python
import asyncio
from argparse import ArgumentParser
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import Tool

parser = ArgumentParser(
    description="Find relevant tools based on natural language query."
)
parser.add_argument("query", help="Natural language query")
parser.add_argument("--url", help="URL of MCP server", default="http://localhost:3001")

args = parser.parse_args()

async def main():
    async with streamablehttp_client(f"{args.url}/mcp") as (
        read,
        write,
        get_session_id_callback,
    ):
        async with ClientSession(read, write) as session:
            await session.initialize()
            await http_client_session.call_tool(
                "find_tools", {"query": "I need a tool to convert FASTA to FASTQ"}
            )
            tools: list[Tool] = await client.list_tools()
            print(tools)


if __name__ == "__main__":
    asyncio.run(main())

```


## File Management
For many tools, you will need to manage input and output files for your tool calls. To assist with file management, the Rhea server exposes a REST API that allows for file uploads/downloads to the backend server.

Files stored within Rhea are keyed as UUIDv4 strings and are accepted as input for tool calls requiring input files. 

### Uploading a File
The following script lets you upload a file from your local directory to the Rhea MCP server. 

The script will output a UUIDv4 string representing the file string.

This script can be found in [examples/upload_file.py](https://github.com/chrisagrams/rhea/blob/main/examples/upload_file.py)

``` python
--8<-- "examples/upload_file.py"
```

1. Open a connection with the MCP server with an asynchronous context manager.
2. Call `upload_file()` with the path of your file.
3. This is the file key for your uploaded file to be used as input for tools.
4. Make sure to run as a coroutine.

Usage:
``` bash
python upload_file.py /path/to/file --url http://localhost:3001
```

### Downloading a File
The following scripts lets you download a file from the Rhea MCP server to a local directory.

This script can be found in [examples/download_file.py](https://github.com/chrisagrams/rhea/blob/main/examples/download_file.py)

```python
--8<-- "examples/download_file.py"
```

1. Open a connection with the MCP server with an asynchronous context manager. 
2. Call `download_file()` with your desired file key and output path.
3. Make sure to run as a coroutine.

Usage:
```bash
python download_file.py file_key /path/to/output/directory
```