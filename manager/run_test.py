from lib.academy.academy.exchange.redis import RedisExchangeFactory
from agent.tool import RheaToolAgent, RheaParam, RheaOutput, RheaDataOutput
from utils.schema import Tool, Param, Tests, Test, Conditional
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


def get_test_file_from_store(
    tool_id: str, input_param: Param, test_param: Param, bucket: str
) -> RheaParam:
    if input_param.name != test_param.name:
        raise Exception(
            f"Parameters do not match {input_param.name}!={test_param.name}"
        )
    if input_param.type != "data":
        raise Exception(f"Expected a 'data' param. Got {input_param.value}")

    for obj in minio_client.list_objects(bucket, prefix=f"{tool_id}/", recursive=True):
        if obj.object_name is not None:
            if obj.object_name.split("/")[-1] == test_param.value:
                with Store("rhea-input", connector, register=True) as input_store:
                    resp = minio_client.get_object(bucket, obj.object_name)
                    content = resp.read()
                    proxy = input_store.proxy(content)
                    key = get_key(proxy)
                    return RheaParam.from_param(input_param, key)
    raise ValueError(f"{test_param.name} not found in bucket.")


def process_conditional_inputs(conditional: Conditional):
    return


def process_inputs(tool: Tool, test: Test) -> List[RheaParam]:
    tool_params: List[RheaParam] = []
    if test.params is not None:
        for input_param in tool.inputs.params:
            for test_param in test.params:
                if input_param.name == test_param.name:
                    if input_param.type == "data":
                        tool_params.append(
                            get_test_file_from_store(
                                tool.id, input_param, test_param, minio_bucket
                            )
                        )
                    else:
                        tool_params.append(
                            RheaParam.from_param(input_param, test_param.value)
                        )
    return tool_params


def assert_tool_tests(tool: Tool, test: Test, output: RheaDataOutput, store: Store) -> bool:
    if test.output_collection is not None:
        if test.output_collection.elements is not None:
            for element in test.output_collection.elements:
                if element.assert_contents is None: # No need to assert contents
                    if element.name == output.name:
                        return True
    if test.outputs is not None:
        for out in test.outputs:
            if out.name == output.name:
                if out.assert_contents is None: # No need to assert contents
                    return True
                else:
                    buffer = store.get(output.key)
                    if buffer is not None:
                        try:
                            out.assert_contents.run_all(buffer)
                        except AssertionError as e:
                            print(e)
                            return False
                        return True

    return False


def process_outputs(tool: Tool, test: Test, outputs: RheaOutput) -> bool:
    with Store("rhea-output", connector, register=True) as output_store:
        if outputs.files is not None:
            for result in outputs.files:
                if not assert_tool_tests(tool, test, result, output_store):
                    print(f"{result.key},{result.filename},{result.name} : FAILED")
                    return False
                else:
                    print(f"{result.key},{result.filename},{result.name} : PASSED")
        return True


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
        tool_params = process_inputs(tool, test)

        tool_result = handle.run_tool(tool_params).result()

        # Get outputs
        test_result = process_outputs(tool, test, tool_result)
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
    tool = tools["f69b601af5ce77b7"]
    
    tool_results = run_tool_tests(tool)


    parsl.dfk().cleanup()
