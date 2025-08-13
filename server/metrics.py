from prometheus_client import Counter, Histogram
from prometheus_client.core import GaugeMetricFamily
from redis import Redis
from redis.exceptions import ResponseError
from prometheus_client.registry import Collector

find_tools_request_count = Counter(
    "find_tools_requests_total", "Total number of calls to `find_tools` MCP tool."
)

find_tool_request_latency = Histogram(
    "find_tools_request_latency_seconds",
    "Histogram of `find_tools` request latencies in seconds.",
)

tool_execution_request_count = Counter(
    "tool_execution_request_total",
    "Total number of tool executions (excluding `find_tools`).",
)

tool_execution_runtime = Histogram(
    "tool_execution_runtime_seconds",
    "Historgram of tool execution runtimes.",
    buckets=tuple(float(x) for x in range(1, 601, 15)),
)

successful_tool_executions = Counter(
    "successful_tool_executions", "Total number of sucessful tool executions."
)

failed_tool_executions = Counter(
    "failed_tool_executions", "Total number of failed tool executions."
)


class RedisHashCollector(Collector):
    def __init__(self, redis_client: Redis, hash_key: str):
        self.r = redis_client
        self.hash_key = hash_key
        super().__init__()

    def collect(self):
        try:
            count = self.r.hlen(self.hash_key)
        except ResponseError:
            count = 0
        metric = GaugeMetricFamily(
            f"{self.hash_key}_fields_total",
            "Number of members in Redis hash.",
        )
        metric.add_metric([], count)  # type: ignore
        yield metric
