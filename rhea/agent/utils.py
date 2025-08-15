import os
import asyncio
from asyncio.subprocess import PIPE
import aiofiles
import logging
import subprocess
import conda_pack
import zstandard
import tarfile
import shutil
from rhea.utils.schema import Requirement
from typing import List
from tempfile import mkdtemp, mktemp
from io import BytesIO
from minio import Minio
from redis import StrictRedis


logger = logging.getLogger(__name__)


def requirements_to_package_list(
    requirements: List[Requirement], strict: bool = True
) -> List[str]:
    """
    Convert a Galaxy-style requirements list into Conda package specifications.

    Args:
        requirements: Galaxy Requirement objects to translate.
        strict: If True, enforce exact version matches; if False, relax version
            constraints when an exact version isn't available in Conda.

    Returns:
        A list of Conda package strings to install.
    """
    packages: List[str] = []
    for requirement in requirements:
        if requirement.type == "package":
            if strict:
                packages.append(f"{requirement.value}={requirement.version}")
            else:
                packages.append(f"{requirement.value}>={requirement.version}")
        else:
            raise NotImplementedError(
                f'Requirement of type "{requirement.type}" not yet implemented.'
            )
    return packages


async def configure_tool_directory(tool_id: str, minio: Minio) -> str:
    """
    Configure the scripts required for the tool.
    Pulls all objects from the repo from object store and places them into a temporary directory
    Returns: A path to the temporary directory containing scripts
    NOTE: Must cleanup after yourself!
    """

    async def _fetch_and_write(
        minio: Minio, bucket: str, obj, dest_dir: str, prefix: str
    ):
        name = obj.object_name
        if not name:
            return

        resp = await asyncio.to_thread(minio.get_object, bucket, name)
        data = await asyncio.to_thread(resp.read)
        await asyncio.to_thread(resp.close)
        await asyncio.to_thread(resp.release_conn)

        local_path = os.path.join(dest_dir, os.path.relpath(name, prefix))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        async with aiofiles.open(local_path, "wb") as f:
            await f.write(data)

    dest_dir = mkdtemp()
    prefix = f"{tool_id}/"

    objs = await asyncio.to_thread(
        lambda: list(minio.list_objects("dev", prefix=prefix, recursive=True))
    )
    logger.info(f"Pulling {len(objs)} objects.")

    tasks = [
        asyncio.create_task(_fetch_and_write(minio, "dev", obj, dest_dir, prefix))
        for obj in objs
    ]

    await asyncio.gather(*tasks)
    logger.info(f"Objects pulled into {dest_dir}")
    return dest_dir


async def cleanup_tool_directory(dir_path: str) -> None:
    """
    Remove the temporary directory created for a tool.
    """
    try:
        await asyncio.to_thread(shutil.rmtree, dir_path)
        logger.info(f"Cleaned up tool directory: {dir_path}")
    except Exception as e:
        logger.warning(f"Failed to clean up {dir_path}: {e}")


async def install_conda_env(
    env_name: str,
    requirements: List[Requirement],
    r: StrictRedis,
    target_path: str,
    n_threads: int = -1,
) -> List[str]:
    loop = asyncio.get_running_loop()

    # If Conda environment is cached, unpack and return immediately
    exists = await loop.run_in_executor(None, r.hexists, "conda_envs", env_name)
    if exists:
        await loop.run_in_executor(None, unpack_conda_env, env_name, r, target_path)
        return []

    # Create a new environment
    packages: List[str] = []
    for strict in (True, False):
        packages = requirements_to_package_list(requirements, strict=strict)
        proc = await asyncio.create_subprocess_exec(
            "conda", "create", "-n", env_name, "-y", *packages, stdout=PIPE, stderr=PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode == 0:
            break
        if not strict:
            raise RuntimeError(stdout.decode().strip() + "\n" + stderr.decode().strip())

    # Pack the environment in another thread
    future = loop.run_in_executor(None, pack_conda_env, env_name, r, n_threads)
    asyncio.ensure_future(future)

    return packages


def pack_conda_env(env_name: str, r: StrictRedis, n_threads: int = -1) -> None:
    """
    Packages the generated Conda enviroment, compresses w/ zstd, and pushes to Redis.
    """
    out_path = mktemp(suffix=".tar.zst")
    logger.info(f"Packing environment '{env_name}' into {out_path}")
    # Pack Conda environment to buffer
    conda_pack.pack(name=env_name, output=out_path, n_threads=n_threads)
    with open(out_path, mode="rb") as f:
        buff = f.read()
        logger.debug(f"Resulting size of packed environment '{env_name}': {len(buff)}")

        if len(buff) <= 0:
            raise RuntimeError("Length of packaged environment <=0!")

        # Create Redis transaction and add buffer
        pipe = r.pipeline(transaction=True)
        pipe.hset("conda_envs", mapping={env_name: buff})
        pipe.execute()
        logger.info(f"Environment '{env_name}' stored in Redis.")
    os.remove(out_path)


def unpack_conda_env(env_name: str, r: StrictRedis, target_path: str) -> None:
    """
    Get packaged Conda environment from Redis and upack it.
    Raises KeyError if Conda enviorment is not in Redis.
    """
    buff = r.hget("conda_envs", env_name)
    if buff is None:
        raise KeyError(f"No entry for '{env_name}' in Redis hash 'conda_envs'")

    logger.info(f"Getting environment {env_name} from Redis")
    dctx = zstandard.ZstdDecompressor()
    reader = dctx.stream_reader(BytesIO(buff))  # type: ignore

    with tarfile.open(fileobj=reader, mode="r|*") as tar:
        tar.extractall(path=target_path)

    conda_unpack = os.path.join(target_path, "bin", "conda-unpack")
    subprocess.run([conda_unpack], cwd=target_path, check=True)
    logger.info(f"Unpacked environment {env_name}")
