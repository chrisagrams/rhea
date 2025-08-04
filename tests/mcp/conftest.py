import os
import sys
import io
import csv
import pytest
import anyio
import signal

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.client.sse import sse_client

from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key

from dotenv import load_dotenv

load_dotenv()


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
        "server.mcp_server",
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
        "server.mcp_server",
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
    with Store("rhea-input", connector, register=True) as store:
        yield store


@pytest.fixture()
def example_csv(input_proxystore):
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
    proxy = input_proxystore.proxy(buf.getvalue())
    key = get_key(proxy)
    return str(key.redis_key)  # type: ignore
