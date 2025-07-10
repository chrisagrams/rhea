from parsl import python_app
from academy.identifier import AgentId
from utils.schema import Tool
from proxystore.connectors.redis import RedisConnector
from minio import Minio


@python_app(executors=["docker_workers"])
def launch_agent(
    tool: Tool,
    redis_host: str,
    redis_port: int,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    minio_secure: bool,
):
    import asyncio
    from uuid import UUID
    from concurrent.futures import ThreadPoolExecutor
    from academy.exchange.redis import RedisExchangeFactory
    from academy.manager import Manager
    from utils.schema import Tool
    from agent.tool import RheaToolAgent
    from proxystore.connectors.redis import RedisConnector
    from minio import Minio

    async def _do_launch():
        factory = RedisExchangeFactory(hostname=redis_host, port=redis_port)

        mgr_ctx = await Manager.from_exchange_factory(
            factory=factory,
            executors=ThreadPoolExecutor()
        )
        manager = await mgr_ctx.__aenter__()
        handle = await manager.launch(
            RheaToolAgent(
                tool,
                redis_host=redis_host,
                redis_port=redis_port,
                minio_endpoint=minio_endpoint,
                minio_access_key=minio_access_key,
                minio_secret_key=minio_secret_key,
                minio_secure=minio_secure,
            )
        )
        return handle

    return asyncio.run(_do_launch())
