import os
import sys
import io
import csv
import pytest
import anyio
import signal

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.client.sse import sse_client

from rhea.utils.proxy import RheaFileHandle, RheaFileProxy

from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
import cloudpickle

from minio import Minio

from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
async def db_session():
    DATABASE_URL = os.environ.get(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/rhea"
    )
    engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False, future=True)
    AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    async with AsyncSessionLocal() as db_session:
        yield db_session


@pytest.fixture
def connector() -> RedisConnector:
    return RedisConnector("localhost", 6379)


@pytest.fixture
def minio_client():
    return Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password",
        secure=False,
    )


@pytest.fixture(scope="session")
def coverage_env():
    env = os.environ.copy()
    env["COVERAGE_PROCESS_START"] = os.path.abspath(".coveragerc")
    return env


@pytest.fixture
async def http_client_session(coverage_env):
    python = sys.executable
    cmd = [
        python,
        "-m",
        "coverage",
        "run",
        "--parallel-mode",
        "-m",
        "rhea.server.mcp_server",
        "--transport",
        "streamable-http",
    ]

    proc = await anyio.open_process(
        cmd,
        cwd=os.getcwd(),
        env=os.environ.copy(),
        stderr=sys.stderr,
    )

    await anyio.sleep(3)

    try:
        async with streamablehttp_client("http://localhost:3001/mcp/") as (
            read,
            write,
            get_session_id_callback,
        ):
            async with ClientSession(read, write) as session:
                yield session

    finally:
        proc.send_signal(signal.SIGINT)
        await proc.wait()


@pytest.fixture
async def sse_client_session(coverage_env):
    python = sys.executable
    cmd = [
        python,
        "-m",
        "coverage",
        "run",
        "--parallel-mode",
        "-m",
        "rhea.server.mcp_server",
        "--transport",
        "sse",
    ]

    proc = await anyio.open_process(
        cmd, cwd=os.getcwd(), env=os.environ.copy(), stderr=sys.stderr
    )

    await anyio.sleep(3)

    try:
        async with sse_client("http://localhost:3001/sse/") as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session

    finally:
        proc.send_signal(signal.SIGINT)
        await proc.wait()


@pytest.fixture()
def input_proxystore():
    connector = RedisConnector(
        os.environ.get("REDIS_HOST", "localhost"),
        int(os.environ.get("REDIS_PORT", "6379")),
    )
    with Store(
        "rhea-input",
        connector,
        register=True,
        serializer=cloudpickle.dumps,
        deserializer=cloudpickle.loads,
    ) as store:
        yield store


@pytest.fixture()
def example_csv(input_proxystore: Store[RedisConnector]) -> str:
    # Create example CSV as a bytes buffer
    data = [
        ["col1", "col2"],
        ["1", "2"],
        ["3", "4"],
    ]
    buf = io.BytesIO()
    text_buf = io.TextIOWrapper(buf, encoding="utf-8", newline="")
    writer = csv.writer(text_buf)
    writer.writerows(data)
    text_buf.flush()
    buf.seek(0)

    # Put CSV buffer in Redis
    file_handle: RheaFileHandle = RheaFileHandle(
        r=input_proxystore.connector._redis_client
    )
    file_handle.append(buf.getvalue())

    # Create RheaFileProxy object
    proxy = RheaFileProxy(
        name="test.csv",
        format=file_handle.filetype(),
        filename="test.csv",
        filesize=len(file_handle),
        file_key=file_handle.key,
    )

    # Store in ProxyStore
    key = proxy.to_proxy(input_proxystore)
    return key
