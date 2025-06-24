from typing import List
from utils.schema import Tool, Test, Param, Conditional
from agent.tool import RheaParam, RheaOutput, RheaDataOutput
from proxystore.connectors.redis import RedisConnector
from proxystore.store import Store
from minio import Minio
from proxystore.store.utils import get_key

def get_test_file_from_store(
    tool_id: str, input_param: Param, test_param: Param, connector: RedisConnector, minio_client: Minio, bucket: str
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


def process_inputs(tool: Tool, test: Test, connector: RedisConnector,minio_client: Minio, minio_bucket: str) -> List[RheaParam]:
    tool_params: List[RheaParam] = []
    if test.params is not None:
        for input_param in tool.inputs.params:
            for test_param in test.params:
                if input_param.name == test_param.name:
                    if input_param.type == "data":
                        tool_params.append(
                            get_test_file_from_store(
                                tool.id, input_param, test_param, connector, minio_client, minio_bucket
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


def process_outputs(tool: Tool, test: Test, connector: RedisConnector, outputs: RheaOutput) -> bool:
    with Store("rhea-output", connector, register=True) as output_store:
        if outputs.files is not None:
            for result in outputs.files:
                if not assert_tool_tests(tool, test, result, output_store):
                    print(f"{result.key},{result.filename},{result.name} : FAILED")
                    return False
                else:
                    print(f"{result.key},{result.filename},{result.name} : PASSED")
        return True