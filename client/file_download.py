from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
from argparse import ArgumentParser

parser = ArgumentParser(description="Download files from ProxyStore")
parser.add_argument("key", help="Redis key of file")
parser.add_argument("output_file", help="Output filename")
parser.add_argument("--store_name", default="rhea-output", help="Name of ProxyStore store")
parser.add_argument("--hostname", default="localhost", help="Hostname of Redis")
parser.add_argument("--port", default=6379, type=int, help="Port of Redis")

args = parser.parse_args()

connector = RedisConnector(args.hostname, args.port)

    
def download_file(key: RedisKey, filepath: str, store: Store[RedisConnector]) -> None:
    buffer = store.get(key)
    if buffer is not None:
        with open(filepath, "wb") as f:
            f.write(buffer)


if __name__ == "__main__":
    with Store(args.store_name, connector, register=True) as store:
        redis_key = RedisKey(redis_key=args.key)
        download_file(redis_key, args.output_file, store)