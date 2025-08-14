from typing import Dict, Any

from prometheus_client import Counter, Histogram
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from redis import Redis
from redis.exceptions import ResponseError

from parsl import DataFlowKernel
from parsl.dataflow.states import States

EXECUTION_TIME_BUCKETS = tuple(float(x) for x in range(1, 601, 15))

FILE_SIZE_BUCKETS = (
    1 * 1024 * 1024,
    2 * 1024 * 1024,
    5 * 1024 * 1024,
    10 * 1024 * 1024,
    50 * 1024 * 1024,
    100 * 1024 * 1024,
    500 * 1024 * 1024,
    1 * 1024**3,
    2 * 1024**3,
    5 * 1024**3,
    10 * 1024**3,
    20 * 1024**3,
)

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
    "Histogram of tool execution runtimes.",
    buckets=EXECUTION_TIME_BUCKETS,
)

successful_tool_executions = Counter(
    "successful_tool_executions", "Total number of sucessful tool executions."
)

failed_tool_executions = Counter(
    "failed_tool_executions", "Total number of failed tool executions."
)

upload_requests = Counter("upload_requests", "Total number of file upload requests.")

upload_size = Histogram(
    "upload_size",
    "Histogram of uploaded filesizes.",
    buckets=FILE_SIZE_BUCKETS,
)

download_requests = Counter(
    "download_requests", "Total number of file download requests."
)

download_size = Histogram(
    "download_size",
    "Histogram of downloaded filesizes.",
    buckets=FILE_SIZE_BUCKETS,
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


class ParslCollector(Collector):
    def __init__(self, dfk: DataFlowKernel):
        self.dfk = dfk

    def collect(self):
        task_counts: Dict[str, int] = {}
        for t in self.dfk.tasks.values():
            st = t.get("status") if isinstance(t, dict) else getattr(t, "status", None)
            if isinstance(st, States):
                key = st.name.lower()
            else:
                key = str(st).lower() if st is not None else "unknown"
            task_counts[key] = task_counts.get(key, 0) + 1

        m_tasks = GaugeMetricFamily(
            "parsl_tasks_total",
            "Parsl tasks by state.",
            labels=["state"],
        )
        for state, count in sorted(task_counts.items()):
            m_tasks.add_metric([state], count)
        yield m_tasks

        m_blocks = GaugeMetricFamily(
            "parsl_executor_blocks",
            "Parsl provider blocks per executor (status=unknown|pending|running|cancelled|completed|failed|timeout|held|missing|scaled_in).",
            labels=["executor", "status"],
        )
        m_workers = GaugeMetricFamily(
            "parsl_executor_workers",
            "Parsl connected workers per executor.",
            labels=["executor"],
        )

        for label, ex in self.dfk.executors.items():
            provider = getattr(ex, "provider", None)
            if provider:
                try:
                    resources = getattr(provider, "resources", None)
                    if resources:
                        state_counts: Dict[str, int] = {}
                        for _, rinfo in resources.items():
                            job_status = rinfo.get("status")
                            if hasattr(job_status, "status"):
                                state_name = job_status.state.name.lower()
                            else:
                                state_name = "unknown"
                            state_counts[state_name] = (
                                state_counts.get(state_name, 0) + 1
                            )

                        for state, count in sorted(state_counts.items()):
                            m_blocks.add_metric([label, state], count)

                except Exception:
                    pass

            workers_obj = getattr(ex, "connected_workers", None)
            if workers_obj is not None:
                try:
                    n = len(workers_obj)
                except Exception:
                    n = 0
                m_workers.add_metric([label], float(n))

        yield m_blocks
        yield m_workers
