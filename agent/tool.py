from lib.academy.academy.behavior import Behavior, action, loop
from typing import List, Optional, Any
from utils.schema import Tool, Param
import os
import subprocess
import json
from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
from tempfile import TemporaryDirectory, NamedTemporaryFile, mkdtemp
from minio import Minio
from dataclasses import dataclass


class RheaParam():
    def __init__(
        self, 
        name: str,
        type: str,
        argument: str | None = None
    ) -> None:
        self.name = name
        self.type = type
        self.argument = argument

    @classmethod
    def from_param(cls, param: Param, value: Any) -> "RheaParam":
        if param.type == "data": # RheaFileParam
            if type(value) is not RedisKey:
                raise ValueError("Value must be a 'RedisKey' for data param.")
            return RheaFileParam.from_param(param, value)
        elif param.type == "text": # RheaTextParam
            if type(value) is not str:
                raise ValueError("Value must be a 'str' for text param.")
            return RheaTextParam.from_param(param, value)
        elif param.type == "boolean":
            if type(value) is not bool:
                raise ValueError("Value must be a 'bool' for boolean param.")
            return RheaBooleanParam.from_param(param, value)
        raise NotImplementedError(f"Param {param.type} not implemented.")



class RheaFileParam(RheaParam):
    def __init__(
        self,
        name: str,
        type: str,
        format: str,
        value: RedisKey,
        argument: str | None = None
    ) -> None:
        super().__init__(name, type, argument)
        self.format = format
        self.value = value
    
    @classmethod
    def from_param(cls, param: Param, value: RedisKey) -> "RheaFileParam":
        if param.name is None or param.type is None or param.format is None:
            raise ValueError("Required fields are 'None'")
        return cls(
            name=param.name,
            type=param.type,
            format=param.format,
            value=value
        )


class RheaBooleanParam(RheaParam):
    def __init__(
        self,
        name: str,
        type: str,
        truevalue: str,
        falsevalue: str,
        value: bool | None = None,
        checked: bool | None = None,
        argument: str | None = None
    ) -> None:
        super().__init__(name, type, argument)
        self.truevalue = truevalue
        self.falsevalue = falsevalue
        self.value = value
        self.checked = checked


    @classmethod
    def from_param(cls, param: Param, value: bool) -> "RheaBooleanParam":
        if param.name is None or param.type is None or param.truevalue is None or param.falsevalue is None:
            raise ValueError("Required fields are 'None'")
        if param.value is None and param.checked is None:
            raise ValueError("Either 'value' or 'checked' must not be 'None'")
        return cls(
            name=param.name,
            type=param.type,
            truevalue=param.truevalue,
            falsevalue=param.falsevalue,
            checked=value,
            value=value
        )


class RheaTextParam(RheaParam):
    def __init__(
        self,
        name: str,
        type: str,
        value: str,
        argument: str | None = None
    ) -> None:
        super().__init__(name, type, argument)
        self.value = value

    @classmethod
    def from_param(cls, param: Param, value: str) -> "RheaTextParam":
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        return cls(
            name=param.name,
            type=param.type,
            value=value
        )


@dataclass
class RheaDataOutput():
    key: RedisKey
    size: int

    @classmethod
    def from_file(cls, filepath: str, store: Store[RedisConnector]) -> "RheaDataOutput":
        with open(filepath, 'rb') as f:
            buffer = f.read()
            proxy = store.proxy(buffer)
            key = get_key(proxy)

        size = os.path.getsize(filepath)
        return cls(key=key, size=size)


class RheaOutput():
    def __init__(self, return_code: int, stdout: str, stderr: str) -> None:
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr
    
    return_code: int
    stdout: str
    stderr: str
    files: Optional[List[RheaDataOutput]] = None


class RheaToolAgent(Behavior):
    def __init__(self,
            tool: Tool,
            redis_host: str,
            redis_port: int,
            minio_endpoint: str,
            minio_access_key: str,
            minio_secret_key: str,
            minio_secure: bool
        ) -> None:
        super().__init__()
        self.tool: Tool = tool
        self.python_verion: str = "3.8"
        self.installed_packages: List[str]
        self.connector = RedisConnector(redis_host, redis_port)

        self.minio = Minio(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=minio_secure
        )
    

    def on_setup(self) -> None:
        # Create Conda environment
        cmd = ["conda", "create", "-n", self.tool.id, f"python={self.python_verion}", "-y"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error creating Conda environment: {result.stdout}")

        # Install Conda packages
        requirements = self.tool.requirements.requirements
        packages = []
        for requirement in requirements:
            if requirement.type == "package":
                packages.append(f"{requirement.value}={requirement.version}")
            else:
                raise NotImplementedError(f'Requirement type of "{requirement.type}" not yet implemented.')
        try:
            cmd = ["conda", "install", "-n", self.tool.id, "-y"] + packages
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"Error installing Conda packages: {result.stdout}")
        except Exception as e:
            print(e)
            # Best-effort try installing the latest available package if our first attempt failed.
            print("Best-effort installing packages...")
            packages = [p.replace("=", ">=") for p in packages]
            cmd = ["conda", "install", "-n", self.tool.id, "-y"] + packages
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"Error installing Conda packages: {result.stdout}")
        
        # List installed packages and parse into installed_packages
        cmd = ["conda", "list", "-n", self.tool.id, "--json"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error listing Conda packages: {result.stdout}")
        pkg_info = json.loads(result.stdout)
        self.installed_packages = [f"{p['name']}={p['version']}" for p in pkg_info]


    def on_shutdown(self) -> None:
        # Delete Conda environment
        cmd = ["conda", "env", "remove", "-n", self.tool.id]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error deleting Conda environment: {result.stdout}")


    @action
    def get_installed_packages(self) -> List[str]:
        cmd = ["conda", "list", "-n", self.tool.id, "--json"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error listing Conda packages: {result.stdout}")
        pkg_info = json.loads(result.stdout)
        packages = [f"{p['name']}={p['version']}" for p in pkg_info]
        return packages
    

    @action
    def run_version_command(self) -> str | None:
        if len(self.tool.version_command) > 0:
            with NamedTemporaryFile("w", suffix=".sh", delete=False) as tf:
                script_path = tf.name
                tf.write("#!/usr/bin/env bash\n")
                tf.write(self.tool.version_command)
                os.chmod(script_path, 0o755)
            cmd = [
                "conda", "run",
                "-n", self.tool.id,
                "--no-capture-output",
                "bash", "-c", script_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"Error in running tool version command: {result.stderr}")
            return result.stdout


    def configure_tool_directory(self) -> str:
        """
        Configure the scripts required for the tool.
        Pulls all objects from the repo from object store and places them into a temporary directory
        Returns: A path to the temporary directory containing scripts
        NOTE: Must cleanup after yourself!
        """
        dir = mkdtemp()

        prefix = f"{self.tool.id}/"
        for obj in self.minio.list_objects('dev', prefix=prefix, recursive=True):
            name = obj.object_name
            if name is not None:
                resp = self.minio.get_object('dev', name)
                content = resp.read()
                local_path = os.path.join(dir, os.path.relpath(name, prefix))
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(local_path, 'wb') as f:
                    f.write(content)
                resp.close()
                resp.release_conn()

        return dir

    
    @action
    def run_tool(self, params: List[RheaParam]) -> RheaOutput:
        env = os.environ.copy()
        env['__tool_directory__'] = self.configure_tool_directory() 
        with Store('rhea-input', self.connector, register=True) as input_store, Store('rhea-output', self.connector, register=True) as output_store:
            with TemporaryDirectory() as input, TemporaryDirectory() as output:
                cwd = output
                # Configure parameters
                for param in params:
                    if isinstance(param, RheaFileParam):
                        tmp_file_path = os.path.join(input, str(param.value.redis_key))
                        with open(tmp_file_path, "wb") as f:
                            buffer = input_store.get(param.value)
                            if buffer is not None:
                                f.write(buffer)
                            else:
                                raise KeyError(f"No file associated with key {param.value}")
                        env[param.name] = tmp_file_path
                    elif isinstance(param, RheaBooleanParam):
                        if param.checked or param.value:
                            value = param.truevalue
                        else:
                            value = param.falsevalue
                        env[param.name] = value
                    elif isinstance(param, RheaTextParam):
                        env[param.name] = param.value
                # Configure command script
                with NamedTemporaryFile("w", suffix=".sh", delete=False) as tf:
                    script_path = tf.name
                    tf.write("#!/usr/bin/env bash\n")
                    tf.write(self.tool.command)
                    os.chmod(script_path, 0o755)

                # Configure outputs
                if self.tool.outputs.data is not None:
                    for out in self.tool.outputs.data:
                        if out.from_work_dir is not None:
                            env[out.name] = os.path.join(output, out.from_work_dir)

                # Run tool
                cmd = [
                    "conda", "run",
                    "-n", self.tool.id,
                    "--no-capture-output",
                    "bash", "-c",
                    f"envsubst < '{script_path}' | bash"
                ]
                result = subprocess.run(cmd, env=env, cwd=cwd, capture_output=True, text=True)
                if result.returncode != 0:
                    raise Exception(f"Error in running tool command: {result.stderr}")
                
                # Get outputs
                outputs = RheaOutput(
                    return_code=result.returncode,
                    stdout=result.stdout,
                    stderr=result.stderr
                )

                if self.tool.outputs.data is not None:
                    outputs.files = []
                    for out in self.tool.outputs.data:
                        if out.from_work_dir is not None:
                            outputs.files.append(
                                RheaDataOutput.from_file(
                                    env[out.name],
                                    output_store
                                )
                            )

                return outputs
                
                            

