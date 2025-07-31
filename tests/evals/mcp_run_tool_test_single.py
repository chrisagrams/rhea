import asyncio
import logging
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)
from proxystore.connectors.redis import RedisConnector
from minio import Minio
from argparse import ArgumentParser
from typing import List, Dict, Any
from proxystore.connectors.redis import RedisKey
from agent.schema import RheaParam, RheaOutput
from utils.models import get_galaxytool_by_id
from utils.process import process_inputs, process_outputs
from utils.schema import Tool
from server.schema import MCPOutput
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import CallToolResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


parser = ArgumentParser(description="Run tool test via MCP server")
parser.add_argument("tool_id", help="Tool ID to test")
parser.add_argument(
    "-u",
    "--url",
    type=str,
    default="http://localhost:3001/mcp",
    help="URL of streamable-http MCP server",
)
args = parser.parse_args()

minio_bucket = "dev"

minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password",
    secure=False,
)

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/rhea"

connector = RedisConnector("localhost", 6379)

engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


def unwrap_user_inputs(rhea_params: List[RheaParam]) -> Dict[str, Any]:
    args: Dict[str, Any] = {}
    for rp in rhea_params:
        # Get the original param name and its stored value
        name = rp.name
        val = rp.value  # type: ignore

        # If it was a data param, unwrap the RedisKey back to its string
        if rp.type == "data" and isinstance(val, RedisKey):
            args[name] = rp.value.redis_key  # type: ignore
        else:
            args[name] = val

    return args


async def main():
    async with AsyncSessionLocal() as db_session:
        async with streamablehttp_client(args.url) as (
            read,
            write,
            get_session_id_callback,
        ):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tool: Tool | None = await get_galaxytool_by_id(db_session, args.tool_id)

                if tool is None:
                    raise RuntimeError(f"Could not get tool {args.tool_id}")

                tool_name: str = tool.name or tool.user_provided_name

                logger.info(f"🛠️ Executing tool {tool_name}")

                for test in tool.tests.tests:
                    for input_param in tool.inputs.params:
                        if (
                            input_param.name is None
                            and input_param.argument is not None
                        ):
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
                        test_result = process_outputs(
                            tool, test, connector, rhea_output
                        )
                        if test_result:
                            logger.info("✅ Test passed")
                        else:
                            logger.info("🟨 Tool executed. Tests did not pass.")
                    else:
                        logger.info("❌ Tool execution failed")


if __name__ == "__main__":
    asyncio.run(main())
