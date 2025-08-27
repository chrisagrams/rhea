from typing import Literal, Optional
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider, PBSProProvider, KubernetesProvider
from parsl.launchers import WrappedLauncher
from kubernetes import config as k8s_config
from pathlib import Path
import tomli as tomllib

from rhea.server.schema import PBSSettings, K8Settings


def _pkg_version() -> str | None:
    try:
        import rhea  # noqa: F401

        v = getattr(rhea, "__version__", None)
        return str(v) if v else None
    except Exception:
        return None


def _pkg_version_from_toml() -> str | None:
    try:
        pyproject_path = Path("pyproject.toml")

        with pyproject_path.open("rb") as f:
            pyproject_data = tomllib.load(f)

        return pyproject_data["project"]["version"]
    except Exception:
        return None


PROJECT_VERSION = _pkg_version() or _pkg_version_from_toml()

if PROJECT_VERSION is None:
    raise RuntimeError("Could not determine current Rhea version.")

DOCKER_IMAGE = (
    f"chrisagrams/rhea-worker-agent:{PROJECT_VERSION}"  # Use current library version
)


docker_cmd = (
    "docker run --rm "
    "{debug_port} "
    "--platform linux/amd64 "  # Ensure amd64 platform is used
    "-v /var/run/docker.sock:/var/run/docker.sock "  # Mount Docker socket
    "-v /tmp:/tmp "  # Mount temp to allow file sharing between containers
    "{network_flag} "
    "{docker_image} "
)

podman_cmd = (
    "sh -lc '"
    "uid=$(id -u); "
    'if [ "$uid" -eq 0 ]; then sock=/run/podman/podman.sock; '
    'else run_dir=$XDG_RUNTIME_DIR; [ -n "$run_dir" ] || run_dir=/run/user/$uid; '
    "sock=$run_dir/podman/podman.sock; fi; "
    'dir=$(dirname "$sock"); [ -d "$dir" ] || mkdir -p "$dir"; '
    'if [ ! -S "$sock" ]; then nohup sh -lc "podman system service --time=0 unix://$sock" >/dev/null 2>&1 & fi; '
    'i=0; while [ ! -S "$sock" ] && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done; '
    'if [ ! -S "$sock" ]; then echo "podman socket not available: $sock"; exit 1; fi; '
    "export CONTAINER_HOST=unix://$sock; "
    "podman run --rm "
    "-e HTTP_PROXY -e HTTPS_PROXY -e http_proxy -e https_proxy "
    "{debug_port}"
    "--user root "
    "--platform linux/amd64 "  # Ensure amd64 platform is used
    "{network_flag}"
    "-v $XDG_RUNTIME_DIR/podman/podman.sock:/run/podman/podman.sock "  # Mount Podman socket
    "-v /tmp:/tmp "  # Mount temp to allow file sharing between containers
    "-e CONTAINER_HOST=unix:///run/podman/podman.sock "
    "docker://{docker_image} "
    "'"
)


def generate_parsl_config(
    backend: Literal["docker", "podman"] = "docker",
    network: Literal["host", "local"] = "host",
    provider: Literal["local", "pbs", "k8"] = "local",
    max_workers_per_node: int = 1,
    init_blocks: int = 0,
    min_blocks: int = 0,
    max_blocks: int = 5,
    nodes_per_block: int = 1,
    parallelism: int = 1,
    debug: bool = False,
    pbs_settings: Optional[PBSSettings] = None,
    k8_settings: Optional[K8Settings] = None,
) -> Config:
    """
    Generate Parsl config for Docker executor
    """

    debug_port = "-p 5680:5680 " if debug and network == "local" else ""
    debugpy = "-m debugpy --listen 0.0.0.0:5680 --wait-for-client " if debug else ""
    debug_flag = "-d " if debug else ""

    local_flag = "--add-host=host.docker.internal:host-gateway "
    host_flag = "--network host "

    if backend == "docker":
        prepend = docker_cmd.format(
            debug_port=debug_port,
            network_flag=host_flag if network == "host" else local_flag,
            docker_image=DOCKER_IMAGE,
        )
    elif backend == "podman":
        prepend = podman_cmd.format(
            debug_port=debug_port,
            network_flag=host_flag if network == "host" else local_flag,
            docker_image=DOCKER_IMAGE,
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

    if provider == "local":
        parsl_provider = LocalProvider(
            launcher=WrappedLauncher(prepend=prepend),  # type: ignore
            init_blocks=init_blocks,
            min_blocks=min_blocks,
            max_blocks=max_blocks,
            nodes_per_block=nodes_per_block,
            parallelism=parallelism,
        )
    elif provider == "pbs":
        if pbs_settings is None:
            raise ValueError("PBSSettings cannot be None when provider = 'pbs'")
        parsl_provider = PBSProProvider(
            launcher=WrappedLauncher(prepend=prepend),  # type: ignore
            account=pbs_settings.account,
            queue=pbs_settings.queue,
            walltime=pbs_settings.walltime,
            scheduler_options=pbs_settings.scheduler_options,
            select_options=pbs_settings.select_options,
            init_blocks=init_blocks,
            min_blocks=min_blocks,
            max_blocks=max_blocks,
            nodes_per_block=nodes_per_block,
            cpus_per_node=pbs_settings.cpus_per_node,
            parallelism=parallelism,
            worker_init=pbs_settings.worker_init,
        )
    elif provider == "k8":
        if k8_settings is None:
            raise ValueError("K8Settings cannot be None when provider = 'k8'")
        try:
            k8s_config.load_kube_config()
        except Exception:
            k8s_config.load_incluster_config()

        parsl_provider = KubernetesProvider(
            image=DOCKER_IMAGE,
            namespace=k8_settings.namespace,
            init_blocks=init_blocks,
            max_blocks=max_blocks,
            nodes_per_block=nodes_per_block,
            parallelism=parallelism,
            max_cpu=k8_settings.max_cpu,
            max_mem=k8_settings.max_mem,
            init_cpu=k8_settings.request_cpu,
            init_mem=k8_settings.request_mem,
        )

    return Config(
        executors=[
            HighThroughputExecutor(
                label="rhea-workers",
                max_workers_per_node=max_workers_per_node,
                provider=parsl_provider,
                worker_debug=debug,
                launch_cmd=launch_cmd_template,
                worker_logdir_root="./",
            )
        ]
    )
