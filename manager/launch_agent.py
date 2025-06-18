from parsl import python_app
from lib.academy.academy.identifier import AgentId
from utils.schema import Tool
from proxystore.connectors.redis import RedisConnector
from minio import Minio


@python_app(executors=["docker_workers"])
def launch_agent(
        agent_id: AgentId,
        tool: Tool,
        redis_host: str,
        redis_port: int,
        minio_endpoint: str,
        minio_access_key: str,
        minio_secret_key: str,
        minio_secure: bool
    ):
    from uuid import UUID
    from lib.academy.academy.exchange.redis import RedisExchangeFactory
    from lib.academy.academy.manager import Manager
    from lib.academy.academy.launcher import ThreadLauncher, Launcher
    from utils.schema import Tool
    from agent.tool import RheaToolAgent
    from proxystore.connectors.redis import RedisConnector
    from minio import Minio

    factory = RedisExchangeFactory(hostname=redis_host, port=redis_port)
    launchers: dict[str, Launcher] = {"default": ThreadLauncher()}

    with Manager(
        exchange=factory,
        launcher=launchers,
        default_launcher="default"
    ) as manager:
        manager.launch(
            RheaToolAgent(
                tool,
                redis_host=redis_host,
                redis_port=redis_port,
                minio_endpoint=minio_endpoint,
                minio_access_key=minio_access_key,
                minio_secret_key=minio_secret_key,
                minio_secure=minio_secure
            ), 
            agent_id=agent_id
        )
        manager.wait(agent_id)