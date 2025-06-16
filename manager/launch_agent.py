from parsl import python_app
from lib.academy.academy.identifier import AgentId
from utils.schema import Tool


@python_app(executors=["docker_workers"])
def launch_agent(agent_id: AgentId, tool: Tool):
    from uuid import UUID
    from lib.academy.academy.exchange.redis import RedisExchangeFactory
    from lib.academy.academy.manager import Manager
    from lib.academy.academy.launcher import ThreadLauncher, Launcher
    from utils.schema import Tool
    from agent.tool import RheaToolAgent

    factory = RedisExchangeFactory(hostname="localhost", port=6379)
    launchers: dict[str, Launcher] = {"default": ThreadLauncher()}

    with Manager(
        exchange=factory,
        launcher=launchers,
        default_launcher="default"
    ) as manager:
        manager.launch(RheaToolAgent(tool), agent_id=agent_id)
        manager.wait(agent_id)