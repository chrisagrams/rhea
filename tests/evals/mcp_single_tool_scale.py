import argparse
import asyncio
import csv
import multiprocessing
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

def log_time(label: str, worker_id: int, csv_path: Path):
    @contextmanager
    def _inner():
        start_wall = time.time()
        start_perf = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start_perf
            header_needed = not csv_path.exists()
            with csv_path.open("a", newline="") as f:
                w = csv.writer(f)
                if header_needed:
                    w.writerow(["worker", "label", "start_unix", "end_unix", "elapsed_s"])
                w.writerow([
                    worker_id,
                    label,
                    start_wall,
                    start_wall + elapsed,
                    f"{elapsed:.6f}"
                ])
    return _inner()

async def call_csv(session: ClientSession, i: int):
    res = await session.call_tool(
        "csv_to_tabular",
        arguments={
            "input1": "731b1268-7651-4ba9-923a-a001c1116a9b",
            "sep": ",",
            "header": True,
        },
    )
    assert res.structuredContent is not None
    rc = res.structuredContent.get("return_code")
    assert str(rc) == "0"
    return res

async def worker_run_serial(worker_id: int, url: str, num_calls: int, csv_path: Path):
    async with streamablehttp_client(url) as (read, write, get_session_id_callback):
        async with ClientSession(read, write) as session:
            # Initialize the connection
            await session.initialize()
            
            for i in range(1, num_calls + 1):
                with log_time(f"tool_call_{i}", worker_id, csv_path):
                    await call_csv(session, i)

def worker_main(worker_id: int, url: str, num_calls: int, csv_path: Path):
    asyncio.run(worker_run_serial(worker_id, url, num_calls, csv_path))

def main():
    p = argparse.ArgumentParser(
        description="Spawn multiple workers to call csv_to_tabular and log timings"
    )
    p.add_argument(
        "-u", "--url", type=str, default="http://localhost:3001/mcp",
        help="URL of streamable-http MCP server"
    )
    p.add_argument(
        "-w", "--workers", type=int, default=4,
        help="number of parallel worker processes"
    )
    p.add_argument(
        "-n", "--calls", type=int, default=10,
        help="number of calls per worker"
    )
    args = p.parse_args()

    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    csv_path = results_dir / f"timings_{timestamp}.csv"

    procs = []
    for wid in range(1, args.workers + 1):
        p = multiprocessing.Process(
            target=worker_main,
            args=(wid, args.url, args.calls, csv_path)
        )
        p.start()
        procs.append(p)
    for p in procs:
        p.join()

if __name__ == "__main__":
    main()
