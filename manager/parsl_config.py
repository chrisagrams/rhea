from typing import Literal
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.launchers import WrappedLauncher

docker_cmd = (
    "docker run --rm "
    "{debug_port} "
    "--platform linux/amd64 "  # Ensure amd64 platform is used
    "{network_flag} "
    "chrisagrams/rhea-worker-agent:latest "
)

podman_cmd = (
    "podman run --rm "
    "{debug_port} "
    "--user root "
    "--platform linux/amd64 "  # Ensure amd64 platform is used
    "{network_flag} "
    "docker://chrisagrams/rhea-worker-agent "
)

def generate_parsl_config(
        backend: Literal["docker", "podman"] = "docker", 
        network: Literal["host", "local"] = "host",
        debug: bool = False
    ) -> Config:
    """
    Generate Parsl config for Docker executor
    """

    debug_port = "-p 5680:5680 " if debug and network == "local" else ""
    debugpy = "-m debugpy --listen 0.0.0.0:5680 --wait-for-client " if debug else ""
    debug_flag = "-d " if debug else ""

    local_flag = "--add-host=host.docker.internal:host-gateway "
    host_flag = "--network host "

    
    if backend == 'docker':
        prepend = docker_cmd.format(
            debug_port=debug_port,
            network_flag=host_flag if network == "host" else local_flag
        )
    elif backend == 'podman':
        prepend = podman_cmd.format(
            debug_port=debug_port,
            network_flag=host_flag if network == "host" else local_flag
        )
    else:
        raise ValueError(f"Backend '{backend}' not supported")
    
    launch_cmd_template = (
        "/home/rhea/venv/bin/python -u "
        f"{debugpy}"
        "-m parsl.executors.high_throughput.process_worker_pool "
        f"{debug_flag}{{max_workers_per_node}} "
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
        "--available-accelerators {accelerators}"
    )
    
    return Config(
        executors=[
            HighThroughputExecutor(
                label="docker_workers",
                provider=LocalProvider(
                    launcher=WrappedLauncher(prepend=prepend), # type: ignore
                    init_blocks=0,
                    min_blocks=0,
                    max_blocks=5,
                    nodes_per_block=1,
                    parallelism=1,
                ),
                worker_debug=debug,
                launch_cmd=launch_cmd_template,
                worker_logdir_root="./",
            )
        ]
    )