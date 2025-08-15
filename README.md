# Rhea
A scalable MCP (Model Context Protocol) tool framework to serve *thousands* of biomedical tools for Large Language Models.

Example executions with Claude:

- [A multi-step conversion from FASTA -> FASTQ -> CSV](https://claude.ai/share/37e6cf45-7139-405c-9280-61a469edf81f)
- [A simple CSV to Tabular conversion](https://claude.ai/share/ce922736-ab3a-4e1a-8901-8ca26cfb59cb)


## How it works? 
The MCP server first provides a single tool, `find_tools` which accepts a natural language query to select relevant tools. For example, an LLM (or user) can provide a query of "I need a tool to convert a CSV file to tabular.", which the server will then perform a RAG on a collection of tools to populate the server with `n` most relevant tools to the query. Once the server is populated, the LLM/user will have access to those relevant tools, along with their parameter annotations and documentations.

To provide file inputs/outputs with the tool agents, we utilize ProxyStore with a Redis backend, providing keys to the tool when a file input is required. 

When a tool is called, the server utilizes Parsl to spawn an Academy agent that creates an environment for the tool, installing necessary requirements and pulling program files from a S3 object store. The agent will be provided the arguments provided by the MCP server, and return its stdout/stderr along with any output files as ProxyStore keys.

## Setup

### Requirements
- `uv` - Package manager for Python projects.
- `docker` - To run tools locally (external executor comming soon!)

Additionally, the server will need to point to an existing OpenAI-like endpoint (for embedding), Postgres, Redis, and MinIO server. Documentation coming soon.

### Instalation
After cloning the repository, use `uv` to configure the virtual environment. 

``` bash
uv sync
```

### Configuring Environment
An example `.env` file is provided in `.env.example`.
Datasets coming soon!

### Installing into Claude Desktop
In your `claude_desktop_config.json` file, add the following entry (Make sure to replace `location_of_repo` with the actual location!):


#### macOS/Linux
```json
{
  "mcpServers": {
    "Rhea": {
        "command": "bash",
        "args": [
            "-lc",
            "cd location_of_repo && uv run -m server.mcp_server"
        ]
    }
  }
}
```

#### Windows (WSL)
```json
{
  "mcpServers": {
    "Rhea": {
        "command": "wsl.exe",
        "args": [
            "bash",
            "-lc",
            "cd location_of_repo && uv run -m server.mcp_server"
        ]
    }
  }
}
```

### Testing with MCP Inspector
To test the tools with MCP Inspector:

```
npx @modelcontextprotocol/inspector
```

And set the following configuration parameters:

| Parameter | Value |
| --------- | ----- |
| Transport Type | STDIO |
| Command | uv |
| Arguments | run -m server.mcp_server |
| Request Timeout | 600000 |
| Reset Timeouts on Progress | True |
| Maximum Total Timeout | 600000 | 
| Proxy Session Token | Token provided within CLI | 


> **Note:** The timeouts are a temporary workaround to make sure the MCP client does not timeout during long tool executions. Better progress reporting is coming soon.

### Running with SSE Transport
By default, the MCP server will start with STDIO transport for use with Claude Desktop. To enable SSE transport layer:

```bash
uv run -m server.mcp_server --transport sse
```

### Running with Streamable HTTP 
*Work in progress!*