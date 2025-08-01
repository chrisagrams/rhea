import asyncio
from tests.evals.helpers import log_time
from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client
from mcp.client.sse import sse_client
from mcp.shared.context import RequestContext


# server_params = StdioServerParameters(
#     command="uv",
#     args=["run", "-m", "server.mcp_server"],
#     env={"UV_INDEX": os.environ.get("UV_INDEX", "")},
#     cwd=".",
# )


async def call_rag(session: ClientSession, i: str):
    with log_time(f"find_tools_{i}"):
        res = await session.call_tool(
            "find_tools", arguments={"query": "CSV to tabular"}
        )
    assert res.structuredContent is not None
    assert len(res.structuredContent.get("result") or []) > 1
    return res


async def call_csv(session: ClientSession, i: str):
    with log_time(f"tool_call_{i}"):
        res = await session.call_tool(
            "csv_to_tabular",
            arguments={
                "input1": "8ac59c17-7a79-44c2-8b0b-be9f70a5662c",
                "sep": ",",
                "header": True,
            },
        )
    assert res.structuredContent is not None
    rc = res.structuredContent.get("return_code")
    assert str(rc) == "0"
    return res


async def run():
    async with sse_client("http://localhost:3001/sse") as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Get available tools
            with log_time("init_list_tools"):
                tools = await session.list_tools()
            print(f"Available tools: {[t.name for t in tools.tools]}")

            # Perform RAG
            with log_time("find_tools"):
                rag_result = await session.call_tool(
                    "find_tools", arguments={"query": "CSV to tabular"}
                )
            print(f"RAG result: {rag_result.structuredContent}")

            # Perform RAG 10 times (serial)
            for i in range(10):
                rag_result = await call_rag(session, f"serial-{i}")

            # Perform RAG 10 times (parallel)
            await asyncio.gather(
                *(call_rag(session, f"parallel-{i}") for i in range(10))
            )

            # Call 'CSV to Tabular' tool (initialization)
            with log_time("tool_call"):
                tool_result = await session.call_tool(
                    "csv_to_tabular",
                    arguments={
                        "input1": "8ac59c17-7a79-44c2-8b0b-be9f70a5662c",
                        "sep": ",",
                        "header": True,
                    },
                )

            # Call 'CSV to Tabular' 10 times (sequentialy):
            for i in range(10):
                tool_result = await call_csv(session, f"serial-{i}")

            # Call 'CSV to Tabular' 10 times (parallel):
            results = await asyncio.gather(
                *(call_csv(session, f"parallel-{i}") for i in range(10))
            )


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
