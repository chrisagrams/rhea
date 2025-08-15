import os
from typing import List, Any
from utils.schema import Tool, Test, Param, Conditional, Section
from utils.proxy import RheaFileProxy, RheaFileHandle
from agent.schema import (
    RheaParam,
    RheaOutput,
    RheaDataOutput,
    RheaFileParam,
    RheaSelectParam,
)

from proxystore.connectors.redis import RedisConnector, RedisKey
from proxystore.store import Store
import cloudpickle

from minio import Minio
from proxystore.store.utils import get_key


class FileConnector:
    def __init__(
        self, tool_id: str, connector: RedisConnector, minio_client: Minio, bucket: str
    ):
        self.tool_id = tool_id
        self.connector = connector
        self.minio_client = minio_client
        self.bucket = bucket


def get_test_file_from_store(
    input_param: Param, test_param: Param, fc: FileConnector
) -> RheaParam:
    if input_param.name != test_param.name:
        raise Exception(
            f"Parameters do not match {input_param.name}!={test_param.name}"
        )
    if input_param.type != "data":
        raise Exception(f"Expected a 'data' param. Got {input_param.type}")

    prefix = f"{fc.tool_id}/"
    for obj in fc.minio_client.list_objects(fc.bucket, prefix=prefix, recursive=True):
        object_name = obj.object_name
        if object_name is None or test_param.value is None:
            continue
        if object_name.split("/")[1] == ".hg":
            continue
        relative_path = object_name[len(prefix) :]
        if test_param.value in relative_path:
            with Store(
                "rhea-input",
                fc.connector,
                register=True,
                serializer=cloudpickle.dumps,
                deserializer=cloudpickle.loads,
            ) as input_store:
                resp = fc.minio_client.get_object(fc.bucket, object_name)
                content = resp.read()
                proxy: RheaFileProxy = RheaFileProxy.from_buffer(
                    os.path.basename(object_name), content, r=fc.connector._redis_client
                )
                key = RedisKey(redis_key=proxy.to_proxy(input_store))
                p = RheaParam.from_param(input_param, key)
                if isinstance(p, RheaFileParam):
                    p.filename = object_name.split("/")[-1]
                return p
    raise ValueError(f"{test_param.value} not found in bucket.")


def process_conditional_inputs(conditional: Conditional):
    return


def populate_regular_and_conditional(
    test_param: Param,
    parent: str,
    value: Any,
    input_param: Param | None = None,
    fc: FileConnector | None = None,
) -> List[RheaParam]:

    result = []

    if input_param is not None and input_param.type == "data":
        if input_param is None or fc is None:
            raise ValueError(f"Trying to insert data object without connector")
        result.append(get_test_file_from_store(input_param, test_param, fc))
        cp = test_param.model_copy()
        cp.name = f"{parent}.{test_param.name}"
        ip_cp = input_param.model_copy()
        ip_cp.name = cp.name
        result.append(get_test_file_from_store(ip_cp, cp, fc))
    else:
        if input_param is not None:
            # Edge case when test_param doesn't specify type
            if test_param.type is None and input_param.type is not None:
                test_param.type = input_param.type
        result.append(RheaParam.from_param(test_param, value))
        cp = test_param.model_copy()
        cp.name = f"{parent}.{test_param.name}"
        result.append(RheaParam.from_param(cp, value))

    return result


def populate_defaults(
    param: Param, collection: Conditional | Section
) -> List[RheaParam]:
    result = []
    if param.type == "boolean":
        result.extend(
            populate_regular_and_conditional(param, collection.name, param.checked)
        )
    elif param.type == "select":
        try:
            result.extend(populate_regular_and_conditional(param, collection.name, ""))
        except ValueError:  # None value doesn't exist, do nothing
            pass
    elif param.type == "integer":
        result.extend(
            populate_regular_and_conditional(param, collection.name, param.value)
        )
    elif param.type == "text":
        result.extend(
            populate_regular_and_conditional(param, collection.name, param.value)
        )
    elif param.type == "float":
        result.extend(
            populate_regular_and_conditional(param, collection.name, param.value)
        )
    elif param.optional:
        return result
    else:
        raise NotImplementedError(f"Param of type {param.type} not implemented.")

    return result


def process_inputs(
    tool: Tool,
    test: Test,
    connector: RedisConnector,
    minio_client: Minio,
    minio_bucket: str,
) -> List[RheaParam]:
    tool_params: List[RheaParam] = []
    if test.params is None:
        return tool_params

    # Initialize FileConnector
    fc = FileConnector(tool.id, connector, minio_client, minio_bucket)

    # Params
    # The old way of doing repeats is with a "|" split, where LHS is the index and RHS is the actual param name
    test_map: dict[str, Param] = {
        p.name.split("|")[-1]: p for p in (test.params or []) if p.name is not None
    }
    for input_param in tool.inputs.params:
        if input_param.name is None and input_param.argument is not None:
            input_param.name = input_param.argument.replace("--", "")
        if input_param.name is not None:
            test_param = test_map.get(input_param.name)
            if test_param:
                if input_param.name == test_param.name:
                    if input_param.type == "data" and test_param.value is not None:
                        if input_param.multiple:
                            values = test_param.value.split(",")
                            for value in values:
                                test_param_copy = test_param.model_copy()
                                test_param_copy.value = value
                                tool_params.append(
                                    get_test_file_from_store(
                                        input_param, test_param_copy, fc
                                    )
                                )
                        else:
                            tool_params.append(
                                get_test_file_from_store(input_param, test_param, fc)
                            )
                    else:
                        tool_params.append(
                            RheaParam.from_param(input_param, test_param.value)
                        )
            else:  # Populate defaults
                if input_param.type == "boolean":
                    tool_params.append(
                        RheaParam.from_param(input_param, input_param.checked)
                    )
                elif input_param.type == "select":
                    try:
                        p = RheaParam.from_param(input_param, "")
                        tool_params.append(p)
                    except ValueError:  # None value doesn't exist, do nothing
                        pass
                elif (
                    input_param.type == "integer"
                    or input_param.type == "text"
                    or input_param.type == "float"
                ):
                    if input_param.value:
                        tool_params.append(
                            RheaParam.from_param(input_param, input_param.value)
                        )
                    else:
                        input_copy = input_param.model_copy()
                        input_copy.type = "text"
                        tool_params.append(RheaParam.from_param(input_copy, ""))
                elif input_param.type == "hidden":
                    input_copy = input_param.model_copy()
                    input_copy.type = "text"
                    if input_copy.value:
                        tool_params.append(
                            RheaParam.from_param(input_copy, input_copy.value)
                        )
                    else:
                        tool_params.append(RheaParam.from_param(input_copy, ""))
                else:
                    if not input_param.optional:
                        raise NotImplementedError(
                            f"Param of type {input_param.type} not implemented."
                        )

    # Conditionals
    if tool.inputs.conditionals is not None:
        for conditional in tool.inputs.conditionals:
            if (
                conditional.param.name is not None
                or conditional.param.argument is not None
            ):
                if (
                    conditional.param.name is None
                    and conditional.param.argument is not None
                ):
                    conditional.param.name = conditional.param.argument.replace(
                        "--", ""
                    )
            if conditional.param.name is not None:
                param = test_map.get(conditional.param.name)
                if param:
                    # Insert regular
                    tool_params.append(
                        RheaParam.from_param(conditional.param, param.value)
                    )

                    # Insert conditional
                    cp = conditional.param.model_copy()
                    cp.name = f"{conditional.name}.{conditional.param.name}"
                    tool_params.append(RheaParam.from_param(cp, param.value))
                    for when in conditional.whens:
                        if when.value == param.value:
                            for when_param in when.params:
                                if when_param.type == "hidden":
                                    when_param.type = "text"

                                if when_param.name is not None:
                                    second_param = test_map.get(when_param.name)
                                    if second_param:
                                        if when_param.type == "select":
                                            # If its a select, propagate the options list
                                            second_param.options = when_param.options
                                            second_param.multiple = when_param.multiple
                                        tool_params.extend(
                                            populate_regular_and_conditional(
                                                second_param,
                                                conditional.name,
                                                second_param.value,
                                                input_param=when_param,
                                                fc=fc,
                                            )
                                        )
                                    else:
                                        tool_params.extend(
                                            populate_regular_and_conditional(
                                                when_param,
                                                conditional.name,
                                                when_param.value,
                                            )
                                        )
                else:  # Populate defaults
                    tool_params.extend(
                        populate_defaults(conditional.param, conditional)
                    )

    # Section
    if tool.inputs.sections is not None:
        for section in tool.inputs.sections:
            for section_param in section.params:
                if section_param.name is not None or section_param.argument is not None:
                    if (
                        section_param.name is None
                        and section_param.argument is not None
                    ):
                        section_param.name = section_param.argument.replace("--", "")
                    if section_param.name is not None:
                        test_param = test_map.get(section_param.name)
                        if test_param:
                            tool_params.extend(
                                populate_regular_and_conditional(
                                    section_param,
                                    section.name,
                                    test_param.value,
                                    input_param=section_param,
                                    fc=fc,
                                )
                            )
                        else:  # Populate defaults
                            tool_params.extend(
                                populate_defaults(section_param, section)
                            )

    return tool_params


def assert_tool_tests(
    tool: Tool, test: Test, output: RheaDataOutput, store: Store[RedisConnector]
) -> bool:
    if test.output_collection is not None:
        if test.output_collection.elements is not None:
            for element in test.output_collection.elements:
                if element.assert_contents is None:  # No need to assert contents
                    if element.name == output.name:
                        return True
    if test.outputs is not None:
        for out in test.outputs:
            if out.name == output.name:
                if out.assert_contents is None:  # No need to assert contents
                    return True
                else:
                    proxy = RheaFileProxy.from_proxy(key=output.key, store=store)
                    file_object: RheaFileHandle = proxy.open(
                        r=store.connector._redis_client
                    )
                    buffer: bytes = file_object.read()

                    if buffer is not None:
                        try:
                            out.assert_contents.run_all(buffer)
                        except AssertionError as e:
                            print(e)
                            return False
                        return True

    return False


def process_outputs(
    tool: Tool, test: Test, connector: RedisConnector, outputs: RheaOutput
) -> bool:
    with Store(
        "rhea-output",
        connector,
        register=True,
        serializer=cloudpickle.dumps,
        deserializer=cloudpickle.loads,
    ) as output_store:
        if outputs.files is not None:
            for result in outputs.files:
                if not assert_tool_tests(tool, test, result, output_store):
                    print(f"{result.key},{result.filename},{result.name} : FAILED")
                    return False
                else:
                    print(f"{result.key},{result.filename},{result.name} : PASSED")
        return True
