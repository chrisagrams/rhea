import asyncio
import logging
import csv
import time
from datetime import datetime
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)
from argparse import ArgumentParser
from tests.evals.helpers import run_tool_tests
from rhea.server.schema import MCPOutput
import urllib3


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = ArgumentParser(description="Run tool tests via MCP server")
parser.add_argument("csv_file", help="CSV file containing a list of tools to test")
parser.add_argument(
    "-w",
    "--workers",
    type=int,
    default=4,
    help="Maximum number of concurrent tests",
)
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
parser.add_argument(
    "-r",
    "--results-file",
    type=str,
    default="test_results.csv",
    help="Output CSV file for test results",
)
args = parser.parse_args()

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/rhea"
engine: AsyncEngine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_size=args.workers,
    max_overflow=40,
    pool_timeout=30,
)
AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
)

http_client = urllib3.PoolManager(
    num_pools=args.workers,
    maxsize=50,
    timeout=urllib3.Timeout(connect=5.0, read=30.0),
    retries=urllib3.Retry(total=3),
)


async def test_tool(tool_id: str):
    async with AsyncSessionLocal() as db_session:
        return await run_tool_tests(
            tool_id,
            db_session,
            mcp_url=args.url,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            minio_endpoint=args.minio_endpoint,
            minio_access=args.minio_access,
            minio_secret=args.minio_secret,
            http_client=http_client,
        )


async def main():
    with open(args.csv_file, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        tool_ids = [row[0] for row in reader if row]

    results_f = open(args.results_file, "w", newline="")
    writer = csv.writer(results_f)
    writer.writerow(
        [
            "tool_id",
            "worker_id",
            "start_time",
            "end_time",
            "elapsed_s",
            "return_code",
            "error",
        ]
    )
    results_f.flush()

    sem = asyncio.Semaphore(args.workers)
    worker_queue = asyncio.Queue()
    for i in range(args.workers):
        worker_queue.put_nowait(i + 1)
    lock = asyncio.Lock()

    async def sem_task(tool_id: str):
        async with sem:
            worker_id = await worker_queue.get()
            start_time = datetime.now().isoformat()
            start = time.perf_counter()
            error_msg = ""
            result = None
            try:
                logger.info(f"Worker {worker_id} starting {tool_id}")
                result: MCPOutput | None = await test_tool(tool_id)
                logger.info(f"Worker {worker_id} finished {tool_id}")
            except Exception as e:
                error_msg = str(e)
                logger.error(
                    f"Worker {worker_id} error on {tool_id}: {e}", exc_info=True
                )
            end_time = datetime.now().isoformat()
            elapsed = time.perf_counter() - start

            return_code = None
            if result is not None:
                return_code = result.return_code

            async with lock:
                writer.writerow(
                    [
                        tool_id,
                        worker_id,
                        start_time,
                        end_time,
                        f"{elapsed:.6f}",
                        return_code,
                        error_msg,
                    ]
                )
                results_f.flush()

            worker_queue.put_nowait(worker_id)

    tasks = [asyncio.create_task(sem_task(tid)) for tid in tool_ids]
    await asyncio.gather(*tasks)
    results_f.close()


if __name__ == "__main__":
    asyncio.run(main())
