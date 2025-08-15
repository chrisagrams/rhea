from parsl import python_app
from rhea.utils.schema import Tool


@python_app(executors=["rhea-workers"])
def launch_agent(
    tool: Tool,
    run_id: str,
    redis_host: str,
    redis_port: int,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    minio_secure: bool,
):
    import asyncio
    import pickle
    from concurrent.futures import ThreadPoolExecutor
    from academy.exchange.redis import RedisExchangeFactory
    from academy.manager import Manager
    from academy.exception import AgentTerminatedError
    from rhea.agent.tool import RheaToolAgent
    from redis import Redis

    HEARTBEAT_INTERVAL = 30
    HEARTBEAT_TIMEOUT = 10

    r = Redis(host=redis_host, port=redis_port)

    async def _do_launch():
        factory = RedisExchangeFactory(hostname=redis_host, port=redis_port)

        mgr_ctx = await Manager.from_exchange_factory(
            factory=factory, executors=ThreadPoolExecutor()
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

        # Put the handle in Redis
        key = f"agent_handle:{run_id}-{tool.id}"
        serialized = pickle.dumps(handle)
        r.set(key, serialized)

        try:
            while True:
                await asyncio.wait_for(handle.ping(), timeout=HEARTBEAT_TIMEOUT)
                await asyncio.sleep(HEARTBEAT_INTERVAL)
        except (AgentTerminatedError, asyncio.TimeoutError):
            pass
        finally:
            r.delete(key)  # Remove Redis handle

    return asyncio.run(_do_launch())
