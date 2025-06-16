import os
from lib.academy.academy.exchange.redis import RedisExchangeFactory
from agent.tool import RheaToolAgent
from utils.schema import Tool
from manager.parsl_config import config
from manager.launch_agent import launch_agent
import pickle
import logging
import parsl

logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    with open('tools_dict.pkl', 'rb') as f:
        tools = pickle.load(f)
    tool = tools['thermo_raw_file_converter']
    factory = RedisExchangeFactory('localhost', 6379)
    client = factory.bind_as_client(name="manager")
    agent_id = client.register_agent(RheaToolAgent, name=tool.name)
    fut = launch_agent(agent_id, tool)
    handle = client.get_handle(agent_id)
    
    packages = handle.get_installed_packages().result()

    print(packages)

    handle.shutdown()

    parsl.dfk().cleanup()


    