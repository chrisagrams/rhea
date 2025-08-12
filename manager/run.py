import uuid
import asyncio
from academy.logging import init_logging
from academy.exchange.redis import RedisExchangeFactory
from academy.handle import UnboundRemoteHandle, RemoteHandle
from agent.schema import RheaParam
from manager.parsl_config import generate_parsl_config
from manager.launch_agent import launch_agent
from manager.utils import get_handle_from_redis

from proxystore.connectors.redis import RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
import cloudpickle

import pickle
import logging
import parsl
from minio import Minio
from redis import Redis

logging.basicConfig(level=logging.INFO)


async def main():
    with parsl.load(
        generate_parsl_config(backend="docker", network="local", debug=False)
    ) as dfk:
        with open("tools_dict.pkl", "rb") as f:
            tools = pickle.load(f)
        tool = tools["204bd0ff6499fcca"]
        connector = RedisConnector("localhost", 6379)

        with Store(
            "rhea-input",
            connector,
            register=True,
            serializer=cloudpickle.dumps,
            deserializer=cloudpickle.loads,
        ) as input_store:
            with open("test_files/test.csv", "rb") as f:
                buffer = f.read()
                proxy = input_store.proxy(buffer)
                key = get_key(proxy)

                rhea_params = []
                for param in tool.inputs.params:
                    if param.name == "input1":
                        rhea_params.append(RheaParam.from_param(param, key))
                    elif param.name == "sep":
                        rhea_params.append(RheaParam.from_param(param, ","))
                    elif param.name == "header":
                        rhea_params.append(RheaParam.from_param(param, True))

                # Need to create a client to communicate w/ agent
                factory = RedisExchangeFactory("localhost", 6379)
                client = await factory.create_user_client(name="rhea-manager")

                r = Redis("localhost", 6379)

                run_id = f"tool-tests-{str(uuid.uuid4())}"

                launch_agent(
                    tool,
                    run_id,
                    redis_host="host.docker.internal",
                    redis_port=6379,
                    minio_endpoint="host.docker.internal:9000",
                    minio_access_key="admin",
                    minio_secret_key="password",
                    minio_secure=False,
                )

                unbound_handle: UnboundRemoteHandle | None = (
                    await get_handle_from_redis(tool.id, run_id, r, timeout=30)
                )

                if unbound_handle is None:
                    raise RuntimeError("Never received handle from Parsl worker.")
                handle: RemoteHandle = unbound_handle.bind_to_client(client)

                packages = await (await handle.get_installed_packages())

                tool_result = await (await handle.run_tool(rhea_params))

                print(packages)

                for result in tool_result.files:
                    with Store(
                        "rhea-output",
                        connector,
                        register=True,
                        serializer=cloudpickle.dumps,
                        deserializer=cloudpickle.loads,
                    ) as output_store:
                        result = output_store.get(result.key)
                        print(result)

                await handle.shutdown()


if __name__ == "__main__":
    init_logging(logging.INFO)

    asyncio.run(main())
