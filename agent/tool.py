from lib.academy.academy.behavior import Behavior, action, loop
from typing import List, Optional, Any
from utils.schema import Tool, Param, CollectionOutput
import os
import glob
import subprocess
import json
import re
from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
from tempfile import TemporaryDirectory, NamedTemporaryFile, mkdtemp
from minio import Minio
from dataclasses import dataclass


class RheaParam:
    def __init__(self, name: str, type: str, argument: str | None = None) -> None:
        self.name = name
        self.type = type
        self.argument = argument

    @classmethod
    def from_param(cls, param: Param, value: Any) -> "RheaParam":
        if param.name is None and param.argument is not None:
            # An edge case where name is not specified in the param,
            # but its assumed its the same as argument.
            param.name = param.argument.replace("--", "")
        if param.type == "data":  # RheaFileParam
            if type(value) is not RedisKey:
                raise ValueError("Value must be a 'RedisKey' for data param.")
            return RheaFileParam.from_param(param, value)
        elif param.type == "text":  # RheaTextParam
            if type(value) is not str:
                raise ValueError("Value must be a 'str' for text param.")
            return RheaTextParam.from_param(param, value)
        elif param.type == "integer": # RheaIntegerParam
            if isinstance(value, str):
                try:
                    value = int(value)
                except ValueError:
                    raise ValueError("Value must be an 'int' or string castable to 'int' for integer param.")
            if not isinstance(value, int):
                raise ValueError("Value must be an 'int' for integer param.")
            return RheaIntegerParam.from_param(param, value)
        elif param.type == "float": # RheaFloatParam
            if isinstance(value, str):
                try:
                    value = float(value)
                except ValueError:
                    raise ValueError("Value must be a 'float' or string castable to 'float' for float param.")
            if not isinstance(value, float):
                raise ValueError("Value must be a 'float' for float param.")
            return RheaFloatParam.from_param(param, value)
        elif param.type == "boolean":
            if type(value) is not bool:
                if value.lower() == "true":
                    value = True
                elif value.lower() == "false":
                    value = False
                else:
                    raise ValueError("Value must be a 'bool' for boolean param.")
            return RheaBooleanParam.from_param(param, value)
        elif param.type == "select" and param.multiple:
            if type(value) is not str:
                raise ValueError("Value must be a 'str' for select param.")
            values = value.split(",")
            if len(value) < 1:
                raise ValueError("Unpacked params is empty.")
            return RheaMultiSelectParam.from_param(param, values)
        elif param.type == "select":
            if type(value) is not str:
                if param.options is not None:
                    for option in param.options:
                        if option.selected:
                            return RheaSelectParam.from_param(param, option.value)
                raise ValueError("Value must be a 'str' for select param.")
            return RheaSelectParam.from_param(param, value)
        raise NotImplementedError(f"Param {param.type} not implemented.")


class RheaFileParam(RheaParam):
    def __init__(
        self,
        name: str,
        type: str,
        format: str,
        value: RedisKey,
        argument: str | None = None,
    ) -> None:
        super().__init__(name, type, argument)
        self.format = format
        self.value = value

    @classmethod
    def from_param(cls, param: Param, value: RedisKey) -> "RheaFileParam":
        if param.name is None or param.type is None or param.format is None:
            raise ValueError("Required fields are 'None'")
        return cls(name=param.name, type=param.type, format=param.format, value=value)


class RheaBooleanParam(RheaParam):
    def __init__(
        self,
        name: str,
        type: str,
        truevalue: str,
        falsevalue: str,
        value: bool | None = None,
        checked: bool | None = None,
        argument: str | None = None,
    ) -> None:
        super().__init__(name, type, argument)
        self.truevalue = truevalue
        self.falsevalue = falsevalue
        self.value = value
        self.checked = checked

    @classmethod
    def from_param(cls, param: Param, value: bool) -> "RheaBooleanParam":
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        if param.value is None and param.checked is None:
            raise ValueError("Either 'value' or 'checked' must not be 'None'")
        if param.truevalue is None:
            param.truevalue = "true"
        if param.falsevalue is None:
            param.falsevalue = "false"
        return cls(
            name=param.name,
            type=param.type,
            truevalue=param.truevalue,
            falsevalue=param.falsevalue,
            checked=value,
            value=value,
        )


class RheaTextParam(RheaParam):
    def __init__(
        self, name: str, type: str, value: str, argument: str | None = None
    ) -> None:
        super().__init__(name, type, argument)
        self.value = value

    @classmethod
    def from_param(cls, param: Param, value: str) -> "RheaTextParam":
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        return cls(name=param.name, type=param.type, value=value)


class RheaIntegerParam(RheaParam):
    def __init__(
            self, name: str, type: str, value: int, min: int | None = None, max: int | None = None, argument: str | None = None,
    ) -> None: 
        super().__init__(name, type, argument)
        self.value = value
        self.min = min
        self.max = max
    
    @classmethod
    def from_param(cls, param: Param, value: int, min: int | None = None, max: int | None = None) -> "RheaIntegerParam":
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        return cls(name=param.name, type=param.type, value=value, min=min, max=max)
    

class RheaFloatParam(RheaParam):
    def __init__(
            self, name: str, type: str, value: float, min: float | None = None, max: float | None = None, argument: str | None = None,
    ) -> None:
        super().__init__(name, type, argument)
        self.value = value
        self.min = min
        self.max = max

    @classmethod 
    def from_param(cls, param: Param, value: float, min: float | None = None, max: float | None = None) -> "RheaFloatParam":
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        return cls(name=param.name, type=param.type, value=value, min=min, max=max)
        

class RheaSelectParam(RheaParam):
    def __init__(
        self, name: str, type: str, value: str, argument: str | None = None
    ) -> None:
        super().__init__(name, type, argument)
        self.value = value

    @classmethod
    def from_param(cls, param: Param, value: str) -> "RheaSelectParam":
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        if param.options is None:
            raise ValueError("Param has no options.")
        for option in param.options:
            if option.value == value:
                return cls(name=param.name, type=param.type, value=option.value)
        if param.optional:
            return cls(name=param.name, type=param.type, value='')
        raise ValueError(f"Value {value} not in select options.")


class RheaMultiSelectParam(RheaParam):
    def __init__(
            self, name: str, type: str, values: List[RheaSelectParam], argument: str | None = None
    ) -> None:
        super().__init__(name, type, argument)
        self.values = values
    
    @classmethod
    def from_param(cls, param: Param, value: List[str]) -> "RheaMultiSelectParam":
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        res = []
        for val in value:
            res.append(RheaSelectParam.from_param(param, val))
        return cls(name=param.name, type=param.type, values=res)
    

@dataclass
class RheaDataOutput:
    key: RedisKey
    size: int
    filename: str
    name: Optional[str] = None

    @classmethod
    def from_file(
        cls, filepath: str, store: Store[RedisConnector], name: Optional[str] = None
    ) -> "RheaDataOutput":
        with open(filepath, "rb") as f:
            buffer = f.read()
            proxy = store.proxy(buffer)
            key = get_key(proxy)

        size = os.path.getsize(filepath)
        filename = os.path.basename(filepath)
        return cls(key=key, size=size, filename=filename, name=name)


class RheaOutput:
    def __init__(self, return_code: int, stdout: str, stderr: str) -> None:
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr

    return_code: int
    stdout: str
    stderr: str
    files: Optional[List[RheaDataOutput]] = None


class RheaCollectionOuput(RheaOutput):
    def __init__(
        self,
        return_code: int,
        stdout: str,
        stderr: str,
        collections: List[CollectionOutput],
    ) -> None:
        super().__init__(return_code, stdout, stderr)
        self.collections = collections

    def resolve(self, output_dir: str, store: Store[RedisConnector]) -> None:
        for collection in self.collections:
            if collection.type == "list":
                if collection.discover_datasets is None:
                    raise ValueError("Discover datasets is None")
                if collection.discover_datasets.pattern is not None:  # Regex method
                    rgx = re.compile(
                        collection.discover_datasets.pattern.replace("\\\\", "\\")
                    )
                    search_path = output_dir
                    if collection.discover_datasets.directory is not None:
                        search_path = os.path.join(
                            output_dir, collection.discover_datasets.directory
                        )
                    listing = glob.glob(
                        f"{search_path}/*",
                        recursive=(
                            collection.discover_datasets.recurse
                            if collection.discover_datasets.recurse is not None
                            else False
                        ),
                    )
                    for file in listing:
                        if rgx.match(file):
                            if self.files is None:
                                self.files = []
                            name_match = rgx.match(os.path.basename(file))
                            if name_match is not None:
                                name = name_match.group(1)
                            else:
                                name = None
                            self.files.append(
                                RheaDataOutput.from_file(file, store, name=name)
                            )
                else:
                    raise NotImplementedError(
                        f"Discover dataset method not implemented."
                    )
            else:
                raise NotImplementedError(
                    f"CollectionOutput type of {collection.type} not implemented."
                )


class RheaToolAgent(Behavior):
    def __init__(
        self,
        tool: Tool,
        redis_host: str,
        redis_port: int,
        minio_endpoint: str,
        minio_access_key: str,
        minio_secret_key: str,
        minio_secure: bool,
    ) -> None:
        super().__init__()
        self.tool: Tool = tool
        self.installed_packages: List[str]
        self.connector = RedisConnector(redis_host, redis_port)
        self.replace_galaxy_var(
            "GALAXY_SLOTS", None
        )  # TODO, allow user to pass how many threads to use
        self.replace_galaxy_var(
            "GALAXY_MEMORY_MB", None
        )  # TODO, allow user to pass how much memory to use
        self.replace_galaxy_var(
            "GALAXY_MEMORY_MB_PER_SLOT", None
        )  # TODO, allow user to pass how much memory to use per slot

        self.minio = Minio(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=minio_secure,
        )

    def on_setup(self) -> None:
        # Create Conda environment and install Conda packages 
        requirements = self.tool.requirements.requirements
        packages = []
        for requirement in requirements:
            if requirement.type == "package":
                packages.append(f"{requirement.value}={requirement.version}")
            else:
                raise NotImplementedError(
                    f'Requirement type of "{requirement.type}" not yet implemented.'
                )
        try:
            cmd = ["conda", "create", "-n", self.tool.id, "-y"] + packages
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
                "conda",
                "run",
                "-n",
                self.tool.id,
                "--no-capture-output",
                "bash",
                "-c",
                script_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(
                    f"Error in running tool version command: {result.stderr}"
                )
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
        for obj in self.minio.list_objects("dev", prefix=prefix, recursive=True):
            name = obj.object_name
            if name is not None:
                resp = self.minio.get_object("dev", name)
                content = resp.read()
                local_path = os.path.join(dir, os.path.relpath(name, prefix))
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(local_path, "wb") as f:
                    f.write(content)
                resp.close()
                resp.release_conn()

        return dir

    def replace_galaxy_var(self, var: str, value: Optional[int] = None) -> None:
        """
        Replace occurrences of "\\${VAR:-Z}" (with or without surrounding quotes) in `script`.
        If `value` is given, use it; otherwise keep the default Z.
        """
        pattern = re.compile(rf'"?\\\$\{{{re.escape(var)}:-(\d+)\}}"?')

        def _repl(m: re.Match) -> str:
            default = m.group(1)
            return str(value) if value is not None else default

        self.tool.command = pattern.sub(_repl, self.tool.command)

    def expand_galaxy_if(self, cmd: str, params: List[RheaParam]) -> str:
        lines = cmd.splitlines()
        processed: List[tuple[str, bool]] = []
        stack = [True]
        pmap = {p.name: p for p in params}

        for line in lines:
            # inline "#if"
            m = re.search(r"#if\s+(.*?):", line, flags=re.IGNORECASE)
            if m:
                prefix = line[: m.start()].rstrip()
                if prefix and stack[-1]:
                    processed.append((prefix, False))
                cond = m.group(1)
                for name, p in pmap.items():
                    if isinstance(p, RheaBooleanParam):
                        lit = p.truevalue if p.value else p.falsevalue
                    elif isinstance(p, (RheaSelectParam, RheaTextParam)):
                        lit = p.value
                    elif isinstance(p, RheaMultiSelectParam):
                        vals = []
                        for _p in p.values:
                            vals.append(_p.value)
                        lit = ",".join(vals) # TODO: Check if this is correct
                    elif isinstance(p, RheaFileParam):
                        lit = str(p.value)
                    elif isinstance(p, RheaFloatParam):
                        lit = str(p.value)
                    else:
                        raise NotImplementedError(type(p))
                    cond = cond.replace(f"${name}", repr(lit))
                try:
                    keep = bool(eval(cond, {"str": str}))
                except Exception:
                    keep = False
                stack.append(stack[-1] and keep)
                continue

            # end of block
            if re.search(r"#end(?:\s*if)?\b", line, flags=re.IGNORECASE):
                if len(stack) > 1:
                    stack.pop()
                continue

            # normal line
            if stack[-1]:
                in_if = len(stack) > 1
                processed.append((line, in_if))

        # final substitution only inside if-blocks
        var_pattern = re.compile(r"\$(\w+)")
        result_lines: List[str] = []
        for text, in_if in processed:
            if in_if:

                def repl(m: re.Match) -> str:
                    name = m.group(1)
                    p = pmap.get(name)
                    if not p:
                        return m.group(0)
                    if isinstance(p, RheaBooleanParam):
                        return p.truevalue if p.value else p.falsevalue
                    if isinstance(p, (RheaSelectParam, RheaTextParam)):
                        return str(p.value or "")
                    if isinstance(p, RheaFileParam):
                        return m.group(0)  # leave $var untouched
                    return m.group(0)

                text = var_pattern.sub(repl, text)
            result_lines.append(text)

        return " ".join(result_lines).strip()

    def unescape_bash_vars(self, cmd: str) -> str:
        """
        Turn every '\\$foo' into '$foo' so that bash will expand it at runtime.
        """
        # Replace any backslash immediately before a $ with nothing
        return re.sub(r"\\\$", r"$", cmd)

    def fix_var_quotes(self, cmd: str) -> str:
        """
        Replace any single-quoted $VAR or ${…} with double-quotes so
        bash will expand them at runtime.
        E.g.  '$__tool_directory__' → "$__tool_directory__"
        """
        # This will match a single quote, then a $ plus anything up to the next single-quote,
        # then that closing quote. We capture the $… inside.
        pattern = re.compile(r"'(\$[^']+)'")
        return pattern.sub(r'"\1"', cmd)

    def quote_shell_params(self, cmd: str) -> str:
        # split out double- or single-quoted spans
        parts = re.split(r'(".*?"|\'.*?\')', cmd)
        def wrap(seg: str) -> str:
            # leave quoted spans untouched
            if seg and seg[0] in ('"', "'"):
                return seg
            # match unescaped $VAR or ${VAR}, wrap in quotes
            return re.sub(
                r'(?<!\\)(\$(?:\{[^}]+\}|[A-Za-z_]\w*))',
                r'"\1"',
                seg
            )
        return ''.join(wrap(p) for p in parts)
    
    def replace_dotted_vars(self, cmd: str) -> str:
        """
        Replace bash vars like $name.value or ${name.value} with $name_value or ${name_value}.
        """
        pattern = re.compile(r'(?<!\\)\$(\{)?([A-Za-z_]\w*)\.([A-Za-z_]\w*)(\})?')
        def repl(m: re.Match) -> str:
            has_brace, var, field, closing = m.group(1), m.group(2), m.group(3), m.group(4)
            if has_brace:
                return f'${{{var}_{field}}}'
            return f'${var}_{field}'
        return pattern.sub(repl, cmd)
        

    def build_env_parameters(
            self,
            env: dict[str, str],
            params: List[RheaParam],
            input_dir: str,
            input_store: Store
        ) -> None:
        # Configure parameters
        env["__tool_directory__"] = self.configure_tool_directory()

        for param in params:
            if isinstance(param, RheaFileParam):
                tmp_file_path = os.path.join(input_dir, str(param.value.redis_key))
                with open(tmp_file_path, "wb") as f:
                    buffer = input_store.get(param.value)
                    if buffer is not None:
                        f.write(buffer)
                    else:
                        raise KeyError(
                            f"No file associated with key {param.value}"
                        )
                env[param.name] = tmp_file_path
            elif isinstance(param, RheaBooleanParam):
                if param.checked or param.value:
                    value = param.truevalue
                else:
                    value = param.falsevalue
                env[param.name] = value
            elif isinstance(param, RheaTextParam):
                env[param.name] = param.value
            elif isinstance(param, RheaIntegerParam):
                env[param.name] = str(param.value)
            elif isinstance(param, RheaFloatParam):
                env[param.name] = str(param.value)
            elif isinstance(param, RheaSelectParam):
                env[param.name] = param.value
            elif isinstance(param, RheaMultiSelectParam):
                values = []
                for p in param.values:
                    values.append(p.value)
                env[param.name] = ",".join(values) # TODO: Check if this is correct
        
    def build_output_env_parameters(
            self,
            env: dict[str, str],
            output_dir: str,
    ) -> None:
        if self.tool.outputs.data is not None:
            for out in self.tool.outputs.data:
                if out.from_work_dir is None or out.from_work_dir == "":
                    env[out.name] = os.path.join(output_dir, out.name)
                else:
                    env[out.name] = os.path.join(output_dir, out.from_work_dir)

    @action
    def run_tool(self, params: List[RheaParam]) -> RheaOutput:
        env = os.environ.copy()
        with (
            Store("rhea-input", self.connector, register=True) as input_store,
            Store("rhea-output", self.connector, register=True) as output_store,
        ):
            with TemporaryDirectory() as input, TemporaryDirectory() as output:
                cwd = output

                # Populate input environment variables
                self.build_env_parameters(env, params, input, input_store)

                # Configure command script
                cmd = self.expand_galaxy_if(self.tool.command, params)
                cmd = " ".join(cmd.split())  # Collapse to one line
                cmd = self.unescape_bash_vars(cmd)
                cmd = self.fix_var_quotes(cmd)
                cmd = self.quote_shell_params(cmd)
                cmd = self.replace_dotted_vars(cmd)

                with NamedTemporaryFile("w", suffix=".sh", delete=False) as tf:
                    script_path = tf.name
                    tf.write("#!/usr/bin/env bash\n")
                    tf.write(cmd + "\n")
                    os.chmod(script_path, 0o755)

                # Configure configfiles (if any)
                # TODO: Expand the parameters from within the configfiles (WHY DID THEY DO THIS)
                if self.tool.configfiles is not None and self.tool.configfiles.configfiles is not None:
                    for configfile in self.tool.configfiles.configfiles:
                        with NamedTemporaryFile('w', delete=False) as tf:
                            script_path = tf.name
                            tf.write(configfile.text)
                            os.chmod(script_path, 0o755)
                            env[configfile.name] = script_path

                # Configure outputs
                self.build_output_env_parameters(env, output)

                # Run tool
                cmd = [
                    "conda",
                    "run",
                    "-n",
                    self.tool.id,
                    "--no-capture-output",
                    "bash",
                    script_path,
                ]
                result = subprocess.run(
                    cmd, env=env, cwd=cwd, capture_output=True, text=True
                )
                if result.returncode != 0:
                    raise Exception(f"Error in running tool command: {result.stderr}")

                # Get outputs
                outputs = RheaOutput(
                    return_code=result.returncode,
                    stdout=result.stdout,
                    stderr=result.stderr,
                )

                if self.tool.outputs.data is not None:
                    outputs.files = []
                    for out in self.tool.outputs.data:
                        if out.from_work_dir is not None:
                            if (
                                out.filters is not None
                            ):  # TODO: Actually apply the filters, for now just best-effort try to copy the file
                                try:
                                    outputs.files.append(
                                        RheaDataOutput.from_file(
                                            env[out.name], output_store, name=out.name
                                        )
                                    )
                                except Exception:
                                    pass
                            else:
                                outputs.files.append(
                                    RheaDataOutput.from_file(
                                        env[out.name], output_store, name=out.name
                                    )
                                )

                elif self.tool.outputs.collection is not None:
                    outputs = RheaCollectionOuput(
                        return_code=result.returncode,
                        stdout=result.stdout,
                        stderr=result.stderr,
                        collections=self.tool.outputs.collection,
                    )
                    if outputs.files is None:
                        outputs.files = []
                        outputs.resolve(output, output_store)

                return outputs
