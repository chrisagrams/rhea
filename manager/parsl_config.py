import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.launchers import WrappedLauncher

docker_cmd = (
    "docker run --rm "
    "--platform linux/amd64 " # Ensure amd64 platform is used
    "--network host "
    "-e REDIS_HOST=127.0.0.1 "
    "-e REDIS_PORT=6379 "
    "rhea-worker:latest "
)

launch_cmd = ("/opt/venv/bin/python -u -m parsl.executors.high_throughput.process_worker_pool {debug} {max_workers_per_node} "
                      "-a {addresses} "
                      "-p {prefetch_capacity} "
                      "-c {cores_per_worker} "
                      "-m {mem_per_worker} "
                      "--poll {poll_period} "
                      "--task_port={task_port} "
                      "--result_port={result_port} "
                      "--cert_dir {cert_dir} "
                      "--logdir={logdir} "
                      "--block_id={{block_id}} "
                      "--hb_period={heartbeat_period} "
                      "{address_probe_timeout_string} "
                      "--hb_threshold={heartbeat_threshold} "
                      "--drain_period={drain_period} "
                      "--cpu-affinity {cpu_affinity} "
                      "{enable_mpi_mode} "
                      "--mpi-launcher={mpi_launcher} "
                      "--available-accelerators {accelerators}")

config = Config(
    executors=[
        HighThroughputExecutor(
            label="docker_workers",
            provider=LocalProvider(
                launcher=WrappedLauncher(prepend=docker_cmd),
                init_blocks=0,
                min_blocks=0,
                max_blocks=5,
                nodes_per_block=1,
                parallelism=1,
            ),
            worker_debug=True,
            launch_cmd=launch_cmd,
        )
    ]
)

parsl.load(config)
