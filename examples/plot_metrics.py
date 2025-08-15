import asyncio
import math
from typing import Dict, Any, List
import matplotlib.pyplot as plt
from client import RheaClient


def _rows(metrics: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    out = []
    for metric, samples in metrics.items():
        for s in samples:
            r = {
                "metric": metric,
                "sample": s.get("sample"),
                "value": float(s.get("value", 0.0)),
            }
            lbls = s.get("labels", {}) or {}
            r.update(lbls)
            out.append(r)
    return out


def plot_parsl_workers(ax, metrics: Dict[str, Any]) -> None:
    rows = _rows(metrics)
    data = [r for r in rows if r["metric"] == "parsl_executor_workers"]
    execs = [r.get("executor", "unknown") for r in data]
    vals = [r["value"] for r in data]
    ax.bar(execs, vals)
    ax.set_title("Parsl workers by executor")
    ax.set_xlabel("Executor")
    ax.set_ylabel("Workers")


def plot_histogram_buckets(
    ax, metrics: Dict[str, Any], metric_base: str, unit_scale: str | None = None
) -> None:
    rows = _rows(metrics)
    bucket_name = f"{metric_base}_bucket"
    data = [r for r in rows if r["sample"] == bucket_name]
    if not data:
        ax.set_title(f"{metric_base} (no data)")
        return

    # Sort finite buckets and handle +Inf
    finite = [r for r in data if r.get("le") not in (None, "+Inf")]
    finite.sort(key=lambda r: float(r["le"]))
    labels = [r["le"] for r in finite] + ["+Inf"]
    counts = [r["value"] for r in finite]
    inf = next((r for r in data if r.get("le") == "+Inf"), None)
    if inf:
        counts.append(inf["value"])

    # Convert to per-bucket counts instead of cumulative
    bucket_vals = [counts[0]] + [
        counts[i] - counts[i - 1] for i in range(1, len(counts))
    ]

    # Label formatting
    if unit_scale == "seconds":
        labels_fmt = [f"{float(l):.0f}s" if l != "+Inf" else "+Inf" for l in labels]
    else:
        labels_fmt = _pretty_sizes(labels)

    # Plot as histogram-style bars
    ax.bar(labels_fmt, bucket_vals, width=0.8, align="center")
    ax.set_title(f"{metric_base} histogram")
    ax.set_xlabel("Range upper bound")
    ax.set_ylabel("Count")
    ax.tick_params(axis="x", rotation=45)


def _pretty_sizes(labels: List[str]) -> List[str]:
    out = []
    for s in labels:
        if s == "+Inf":
            out.append("+Inf")
            continue
        x = float(s)
        if x == 0:
            out.append("0")
            continue
        units = ["B", "KB", "MB", "GB", "TB"]
        i = min(int(math.log(x, 1024)), len(units) - 1)
        out.append(f"{x / (1024 ** i):.2f}{units[i]}")
    return out


async def main():
    async with RheaClient("localhost", 3001) as client:
        metrics = await client.metrics()

        fig, axes = plt.subplots(2, 2, figsize=(12, 8))
        axes = axes.flatten()

        plot_parsl_workers(axes[0], metrics)
        plot_histogram_buckets(axes[1], metrics, "upload_size")
        plot_histogram_buckets(
            axes[2], metrics, "tool_execution_runtime_seconds", unit_scale="seconds"
        )
        plot_histogram_buckets(
            axes[3], metrics, "find_tools_request_latency_seconds", unit_scale="seconds"
        )

        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    asyncio.run(main())
