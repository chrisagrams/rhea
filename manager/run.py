import os
from lib.academy.academy.exchange.redis import RedisExchangeFactory
from agent.tool import RheaToolAgent, RheaParam, RheaFileParam, RheaTextParam, RheaBooleanParam
from utils.schema import Tool
from manager.parsl_config import config
from manager.launch_agent import launch_agent
from proxystore.connectors.redis import RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
import pickle
import logging
import parsl
from minio import Minio
logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    with open('tools_dict.pkl', 'rb') as f:
        tools = pickle.load(f)
    tool = tools['204bd0ff6499fcca']
    connector = RedisConnector('localhost', 6379)

    with Store('rhea-input', connector, register=True) as input_store:
        with open('test_files/test.csv', 'rb') as f:
            buffer = f.read()
            proxy = input_store.proxy(buffer)
            key = get_key(proxy)

            factory = RedisExchangeFactory('localhost', 6379)
            client = factory.bind_as_client(name="manager")

            agent_id = client.register_agent(RheaToolAgent, name=tool.name)
            fut = launch_agent(
                agent_id,
                tool,
                redis_host="host.docker.internal",
                redis_port=6379,
                minio_endpoint="host.docker.internal:9000",
                minio_access_key="admin",
                minio_secret_key="password",
                minio_secure=False
            )
            handle = client.get_handle(agent_id)
            
            packages = handle.get_installed_packages().result()

            # file_param = RheaFileParam()
            # file_param.argument = "--input"
            # file_param.name = "input_file"
            # file_param.type = "data"
            # file_param.format = "thermo.raw"
            # file_param.value = key

            file_param = RheaFileParam()
            file_param.name = "input1"
            file_param.type = "data"
            file_param.format = "csv"
            file_param.value = key

            text_param = RheaTextParam()
            text_param.name = "sep"
            text_param.type = "text"
            text_param.value = ","

            bool_param = RheaBooleanParam()
            bool_param.name = "header"
            bool_param.type = "boolean"
            bool_param.truevalue = "TRUE"
            bool_param.falsevalue = "FALSE"
            bool_param.checked = True
            
            
            tool_result = handle.run_tool([file_param, text_param, bool_param]).result()

            print(packages)

            for result in tool_result.files:
                with Store('rhea-output', connector, register=True) as output_store:
                    result = output_store.get(result.key)
                    print(result)

            handle.shutdown()

            parsl.dfk().cleanup()


    