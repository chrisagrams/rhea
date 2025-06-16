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
    with open('tools.pkl', 'rb') as f:
        tools = pickle.load(f)

    tools_subset = tools[:10]
    factory = RedisExchangeFactory('localhost', 6379)
    client = factory.bind_as_client(name="manager")

    launch_futs = []
    handles = []
    for tool in tools_subset:
        agent_id = client.register_agent(RheaToolAgent, name=tool.name)
        launch_futs.append( launch_agent(agent_id, tool) )
        handles.append( client.get_handle(agent_id) )

    for fut in launch_futs:
        fut.result()

    pkg_futs = [h.get_installed_packages() for h in handles]

    for h, pf in zip(handles, pkg_futs):
        packages = pf.result()
        print(f"{h.agent_id!r} packages:", packages)
        h.shutdown()

    parsl.dfk().cleanup()
