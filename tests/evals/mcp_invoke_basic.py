import asyncio
import os
import time
import csv
from pathlib import Path
from contextlib import contextmanager
from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client
from mcp.shared.context import RequestContext

_TIMING_CSV = Path("results/timings.csv")


@contextmanager
def log_time(label: str, csv_path: Path = _TIMING_CSV):
    start_wall = time.time()
    start_perf = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start_perf
        header_needed = not csv_path.exists()
        with csv_path.open("a", newline="") as f:
            w = csv.writer(f)
            if header_needed:
                w.writerow(["label", "start_unix", "end_unix", "elapsed_s"])
            w.writerow([label, start_wall, start_wall + elapsed, f"{elapsed:.6f}"])


server_params = StdioServerParameters(
    command="uv",
    args=["run", "-m", "server.mcp_server"],
    env={"UV_INDEX": os.environ.get("UV_INDEX", "")},
    cwd=".",
)


async def run():
    async with stdio_client(server_params) as (read, write):
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

            # Call 'CSV to Tabular' 10 times (sequentialy!):
            for i in range(10):
                with log_time(f"tool_call_{i}"):
                    tool_result = await session.call_tool(
                        "csv_to_tabular",
                        arguments={
                            "input1": "8ac59c17-7a79-44c2-8b0b-be9f70a5662c",
                            "sep": ",",
                            "header": True,
                        },
                    )


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
