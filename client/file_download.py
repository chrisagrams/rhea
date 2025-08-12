from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
import cloudpickle

import os
from argparse import ArgumentParser

from utils.proxy import RheaFileProxy


parser = ArgumentParser(description="Download files from ProxyStore")
parser.add_argument("key", help="Redis key of file")
parser.add_argument("--output-path", help="Output path.", default="./")
parser.add_argument(
    "--store_name", default="rhea-output", help="Name of ProxyStore store"
)
parser.add_argument("--hostname", default="localhost", help="Hostname of Redis")
parser.add_argument("--port", default=6379, type=int, help="Port of Redis")

args = parser.parse_args()

connector = RedisConnector(args.hostname, args.port)


def download_file(key: RedisKey, path: str, store: Store[RedisConnector]) -> str:
    proxy: RheaFileProxy = RheaFileProxy.from_proxy(key, store)
    output_path = os.path.realpath(os.path.join(path, proxy.filename))

    with open(output_path, "wb") as f:
        f.write(proxy.contents)

    return output_path


if __name__ == "__main__":
    with Store(
        args.store_name,
        connector,
        register=True,
        serializer=cloudpickle.dumps,
        deserializer=cloudpickle.loads,
    ) as store:
        redis_key = RedisKey(redis_key=args.key)
        print(download_file(redis_key, args.output_path, store))
