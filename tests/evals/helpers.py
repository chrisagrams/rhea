import os
import time
import csv
import logging
import urllib3
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from rhea.utils.models import get_galaxytool_by_id
from rhea.utils.process import process_inputs, process_outputs
from rhea.utils.schema import Tool
from rhea.server.schema import MCPOutput
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import CallToolResult
from proxystore.connectors.redis import RedisConnector, RedisKey
from rhea.agent.schema import RheaParam, RheaOutput, RheaMultiSelectParam
from minio import Minio
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

minio_bucket = "dev"


_TIMING_CSV = (
    Path("results") / f"timings_{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
)


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


def unwrap_user_inputs(rhea_params: List[RheaParam]) -> Dict[str, Any]:
    args: Dict[str, Any] = {}
    for rp in rhea_params:
        # Get the original param name and its stored value
        name = rp.name
        if isinstance(rp, RheaMultiSelectParam):
            res = []
            for v in rp.values:
                res.append(v.value)
            val = ",".join(res)
        else:
            val = rp.value  # type: ignore

        # If it was a data param, unwrap the RedisKey back to its string
        if rp.type == "data" and isinstance(val, RedisKey):
            args[name] = rp.value.redis_key  # type: ignore
        else:
            args[name] = val

    return args


async def run_tool_tests(
    tool_id: str,
    db_session: AsyncSession,
    mcp_url: str = "http://localhost:3001/mcp/",
    redis_host: str = "localhost",
    redis_port: int = 6379,
    minio_endpoint: str = "localhost:9000",
    minio_access: str = "admin",
    minio_secret: str = "password",
    http_client: urllib3.PoolManager | None = None,
) -> List[MCPOutput | None]:
    if http_client is None:
        http_client = urllib3.PoolManager(
            num_pools=1,
            maxsize=50,
            timeout=urllib3.Timeout(connect=5.0, read=30.0),
            retries=urllib3.Retry(total=3),
        )

    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access,
        secret_key=minio_secret,
        secure=False,
        http_client=http_client,
    )
    connector = RedisConnector(redis_host, redis_port)

    async with streamablehttp_client(mcp_url) as (
        read,
        write,
        get_session_id_callback,
    ):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tool: Tool | None = await get_galaxytool_by_id(db_session, tool_id)

            if tool is None:
                raise RuntimeError(f"Could not get tool {tool_id}")

            tool_name: str = tool.name or tool.user_provided_name

            logger.info(f"🛠️ Executing tool {tool_name}")

            test_results: List[MCPOutput | None] = []
            for test in tool.tests.tests:
                for input_param in tool.inputs.params:
                    if input_param.name is None and input_param.argument is not None:
                        input_param.name = input_param.argument.replace("--", "")

                tool_params: List[RheaParam] = process_inputs(
                    tool, test, connector, minio_client, minio_bucket
                )

                mcp_args: Dict[str, Any] = unwrap_user_inputs(tool_params)

                logger.info(f"🗒️ Arguments for {tool_name}: {mcp_args}")

                result: CallToolResult = await session.call_tool(
                    tool_name, arguments=mcp_args
                )
                logger.info(
                    f"Tool result for tool {tool_name}: {result.structuredContent}"
                )
                if result.structuredContent is not None:
                    mcp_output: MCPOutput = MCPOutput(**result.structuredContent)
                    rhea_output: RheaOutput = mcp_output.to_rhea()
                    test_result = process_outputs(tool, test, connector, rhea_output)
                    if test_result:
                        logger.info("✅ Test passed")
                    else:
                        logger.info("🟨 Tool executed. Tests did not pass.")
                    test_results.append(mcp_output)
                else:
                    logger.info(f"❌ Tool execution failed: {result.content}")
                    test_results.append(None)
            return test_results
