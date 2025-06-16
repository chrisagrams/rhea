from academy.exchange.thread import ThreadExchange
from academy.launcher.thread import ThreadLauncher
from academy.manager import Manager
from agent.tool import RheaToolAgent
import pickle

if __name__ == "__main__":
    with open('tools.pkl', 'rb') as f:
        tools = pickle.load(f)
    with Manager(
        exchange=ThreadExchange(),
        launcher=ThreadLauncher(),  
    ) as manager:
        behavior = RheaToolAgent(tools[0])
        agent_handle = manager.launch(behavior)


        agent_handle.shutdown()