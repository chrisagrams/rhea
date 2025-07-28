from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
from argparse import ArgumentParser

parser = ArgumentParser(description="Upload files to ProxyStore")
parser.add_argument("input_file", help="Input file")
parser.add_argument(
    "--store_name", default="rhea-input", help="Name of ProxyStore store"
)
parser.add_argument("--hostname", default="localhost", help="Hostname of Redis")
parser.add_argument("--port", default=6379, type=int, help="Port of Redis")

args = parser.parse_args()

connector = RedisConnector(args.hostname, args.port)


def upload_file(filepath: str, store: Store[RedisConnector]) -> str:
    with open(filepath, "rb") as f:
        buffer = f.read()
        proxy = store.proxy(buffer)
        key = get_key(proxy)
        return str(key.redis_key)  # type: ignore


if __name__ == "__main__":
    with Store(args.store_name, connector, register=True) as store:
        result = upload_file(args.input_file, store)
        print(f"{result}")
