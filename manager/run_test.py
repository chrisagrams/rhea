import asyncio
import uuid
from academy.exchange.redis import RedisExchangeFactory
from academy.logging import init_logging
from academy.handle import UnboundRemoteHandle, RemoteHandle
from utils.schema import Tool, Param, Tests, Test, Conditional
from utils.process import process_inputs, process_outputs
from manager.parsl_config import generate_parsl_config
from manager.launch_agent import launch_agent
from manager.utils import get_handle_from_redis
from proxystore.connectors.redis import RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
from typing import List
import pickle
import logging
import parsl
from minio import Minio
from redis import Redis

minio_bucket = "dev"

minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password",
    secure=False,
)

connector = RedisConnector("localhost", 6379)
factory = RedisExchangeFactory("localhost", 6379)

r = Redis("localhost", 6379)


async def run_tool_tests(tool: Tool) -> List[bool]:

    # Need to create a client to communicate w/ agent
    client = await factory.create_user_client(name="rhea-manager")

    test_results = []

    run_id = f"tool-tests-{str(uuid.uuid4())}"
    # Register and start agent
    launch_agent(
        tool,
        run_id,
        redis_host="host.docker.internal",
        redis_port=6379,
        minio_endpoint="host.docker.internal:9000",
        minio_access_key="admin",
        minio_secret_key="password",
        minio_secure=False,
    )

    unbound_handle: UnboundRemoteHandle | None = await get_handle_from_redis(
        tool.id, run_id, r, timeout=30
    )

    if unbound_handle is None:
        raise RuntimeError("Never received handle from Parsl worker.")

    handle: RemoteHandle = unbound_handle.bind_to_client(client)

    # Run tests
    for test in tool.tests.tests:
        for input_param in tool.inputs.params:
            if input_param.name is None and input_param.argument is not None:
                input_param.name = input_param.argument.replace("--", "")

        # Populate RheaParams
        tool_params = process_inputs(tool, test, connector, minio_client, minio_bucket)

        tool_result_future = await handle.run_tool(tool_params)

        tool_result = await tool_result_future

        # Get outputs
        test_result = process_outputs(tool, test, connector, tool_result)
        if tool_result:
            print(f"{tool.id} : PASSED")
        else:
            print(f"{tool.id} : FAILED")
        test_results.append(test_result)

    # Shut down tool agent
    await handle.shutdown()

    return test_results


if __name__ == "__main__":
    init_logging(logging.INFO)

    with open("tools_dict.pkl", "rb") as f:
        tools = pickle.load(f)
    # tool = tools["783bde422b425bd9"]
    tool = tools["dba308ddf7976bcd"]
    # tool = tools["a74ca2106a7a2073"] # Not working (macros)
    # tool = tools["593966108c52c584"]
    # tool = tools["f69b601af5ce77b7"]
    # tool = tools["c198b9ec43cfbe0e"]
    # tool = tools["8e36777d470b3c19"]
    # tool = tools["fa1c79f582a17d50"] # Not working (configfile)
    # tool = tools["c8658b82d8429f5d"]

    # tool = tools["e1d52e9221388701"]

    with parsl.load(
        generate_parsl_config(backend="docker", network="local", debug=False)
    ) as dfk:
        tool_results = asyncio.run(run_tool_tests(tool))
