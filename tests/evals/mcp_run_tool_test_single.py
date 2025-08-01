import asyncio
import logging
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)
from argparse import ArgumentParser
from tests.evals.helpers import run_tool_tests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = ArgumentParser(description="Run tool test via MCP server")
parser.add_argument("tool_id", help="Tool ID to test")
parser.add_argument(
    "-u",
    "--url",
    type=str,
    default="http://localhost:3001/mcp/",
    help="URL of streamable-http MCP server",
)
parser.add_argument(
    "--redis-host", type=str, default="localhost", help="Redis hostname"
)
parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
parser.add_argument(
    "--minio-endpoint", type=str, default="localhost:9000", help="MinIO endpoint"
)
parser.add_argument(
    "--minio-access", type=str, default="admin", help="MinIO access key"
)
parser.add_argument(
    "--minio-secret", type=str, default="password", help="MinIO secret key"
)
args = parser.parse_args()

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/rhea"

engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def main():
    async with AsyncSessionLocal() as db_session:
        await run_tool_tests(
            args.tool_id,
            db_session,
            mcp_url=args.url,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            minio_endpoint=args.minio_endpoint,
            minio_access=args.minio_access,
            minio_secret=args.minio_secret,
        )


if __name__ == "__main__":
    asyncio.run(main())
