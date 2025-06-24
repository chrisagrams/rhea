from lib.academy.academy.exchange.redis import RedisExchangeFactory
from agent.tool import RheaToolAgent, RheaParam, RheaOutput, RheaDataOutput
from utils.schema import Tool, Param, Tests, Test, Conditional
from utils.process import process_inputs, process_outputs
from manager.parsl_config import config
from manager.launch_agent import launch_agent
from proxystore.connectors.redis import RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
from typing import List
import pickle
import logging
import parsl
from minio import Minio

minio_bucket = "dev"

minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password",
    secure=False,
)

connector = RedisConnector("localhost", 6379)
factory = RedisExchangeFactory("localhost", 6379)
client = factory.bind_as_client(name="manager")


def run_tool_tests(tool: Tool) -> List[bool]:
    tool_id = tool.id
    test_results = []

    # Register and start agent
    agent_id = client.register_agent(RheaToolAgent, name=tool.name)
    fut = launch_agent(
        agent_id,
        tool,
        redis_host="host.docker.internal",
        redis_port=6379,
        minio_endpoint="host.docker.internal:9000",
        minio_access_key="admin",
        minio_secret_key="password",
        minio_secure=False,
    )
    handle = client.get_handle(agent_id)

    # Run tests
    for test in tool.tests.tests:
        for input_param in tool.inputs.params:
            if input_param.name is None and input_param.argument is not None:
                input_param.name = input_param.argument.replace("--", "")

        # Populate RheaParams
        tool_params = process_inputs(tool, test, connector, minio_client, minio_bucket)

        tool_result = handle.run_tool(tool_params).result()

        # Get outputs
        test_result = process_outputs(tool, test, connector, tool_result)
        if tool_result:
            print(f"{tool.id} : PASSED")
        else:
            print(f"{tool.id} : FAILED")
        test_results.append(test_result)

    # Shut down tool agent
    handle.shutdown()

    return test_results


if __name__ == "__main__":
    with open("tools_dict.pkl", "rb") as f:
        tools = pickle.load(f)
    # tool = tools["783bde422b425bd9"]
    # tool = tools["a74ca2106a7a2073"]
    # tool = tools["593966108c52c584"]
    # tool = tools["f69b601af5ce77b7"]
    tool = tools["c198b9ec43cfbe0e"]
    
    tool_results = run_tool_tests(tool)


    parsl.dfk().cleanup()
