import os
import asyncio
from asyncio.subprocess import PIPE
import aiofiles
import logging
import subprocess
import conda_pack
from utils.schema import Requirement
from typing import List
from tempfile import SpooledTemporaryFile, mkdtemp
from io import BytesIO
from minio import Minio

logger = logging.getLogger(__name__)

def requirements_to_package_list(
        requirements: List[Requirement],
        strict: bool = True
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
    async def _fetch_and_write(minio: Minio, bucket: str, obj, dest_dir: str, prefix: str):
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
        lambda: list(minio.list_objects('dev', prefix=prefix, recursive=True))
    )
    logger.info(f"Pulling {len(objs)} objects.")

    tasks = [
        asyncio.create_task(_fetch_and_write(minio, 'dev', obj, dest_dir, prefix))
        for obj in objs
    ]

    await asyncio.gather(*tasks)
    logger.info(f"Objects pulled into {dest_dir}")
    return dest_dir



async def install_conda_env(env_name: str, requirements: List[Requirement]) -> List[str]:
    packages: List[str] = []
    for strict in (True, False):
        packages = requirements_to_package_list(requirements, strict=strict)
        logger.info(f"Installing Conda packages (strict={strict}): {packages}")

        proc = await asyncio.create_subprocess_exec(
            "conda", "create", "-n", env_name, "-y", *packages,
            stdout=PIPE, stderr=PIPE
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode == 0:
            break
        if not strict:
            msg = stdout.decode().strip() + "\n" + stderr.decode().strip()
            raise RuntimeError(f"Error installing Conda packages:\n{msg}")

    logger.info(f"Successfully installed packages into Conda environment {env_name}: {packages}")
    return packages
    

def pack_conda_env(env_name: str, n_threads: int = -1) -> BytesIO: 
    with SpooledTemporaryFile(max_size=10**9, suffix=".tar.zst") as tmp:
        conda_pack.pack(prefix=env_name, output=tmp.name)
        tmp.seek(0)
        return BytesIO(tmp.read())