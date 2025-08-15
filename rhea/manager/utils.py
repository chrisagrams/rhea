import pickle
import time
import asyncio
from redis import Redis
from academy.handle import UnboundRemoteHandle, RemoteHandle


async def get_handle_from_redis(
    tool_id: str, run_id: str, r: Redis, timeout: float = 30.0
) -> UnboundRemoteHandle | None:
    interval = 0.1
    deadline = time.time() + timeout
    while True:
        data = r.get(f"agent_handle:{run_id}-{tool_id}")
        if data is not None:
            result: UnboundRemoteHandle = pickle.loads(data)  # type: ignore
            return result
        if time.time() > deadline:
            return None
        await asyncio.sleep(interval)
