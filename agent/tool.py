import os
import subprocess
import json
import re
from academy.agent import Agent, action
from typing import List, Optional
from utils.schema import Tool, Param, ConfigFile
from agent.schema import *
from proxystore.connectors.redis import RedisConnector
from proxystore.store import Store
from tempfile import TemporaryDirectory, NamedTemporaryFile, mkdtemp
from minio import Minio
from Cheetah.Template import Template


class RheaToolAgent(Agent):
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

    async def agent_on_startup(self) -> None:
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
            cmd = ["conda", "create", "-n", self.tool.id, "-y"] + packages
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

    async def agent_on_shutdown(self) -> None:
        # Delete Conda environment
        cmd = ["conda", "env", "remove", "-n", self.tool.id]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error deleting Conda environment: {result.stdout}")

    @action
    async def get_installed_packages(self) -> List[str]:
        cmd = ["conda", "list", "-n", self.tool.id, "--json"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error listing Conda packages: {result.stdout}")
        pkg_info = json.loads(result.stdout)
        packages = [f"{p['name']}={p['version']}" for p in pkg_info]
        return packages

    @action
    async def run_version_command(self) -> str | None:
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

        self.tool.command.command = pattern.sub(_repl, self.tool.command.command)

    
    def apply_interpreter_command(self) -> str:
        """
        Command might have an "interpreter" section, append it before the command. (e.g. python)
        """
        if self.tool.command.interpreter is not None and self.tool.command.interpreter != "":
            return f"{self.tool.command.interpreter} {self.tool.command.command}"
        return self.tool.command.command


    def expand_galaxy_if(self, cmd: str, env: dict[str, Any]) -> str:
        var_pattern = re.compile(r'\$\{?([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\}?')
        vars_ = sorted(set(var_pattern.findall(cmd)),
                    key=lambda v: v.count('.'),
                    reverse=True)
        nested_roots = {v.split('.')[0] for v in vars_ if '.' in v}

        context: dict[str, Any] = {}
        for k, v in env.items():
            if '.' not in k:
                if isinstance(v, (list, GalaxyFileVar)):
                    context[k] = v
                else:
                    context[k] = GalaxyVar(v)

        for k, v in env.items():
            if '.' in k:
                root, rest = k.split('.', 1)
                if root not in context:
                    context[root] = GalaxyVar('')
                
                if isinstance(context[root], GalaxyVar):
                    context[root].set_nested(rest, v)

        for var in vars_:
            parts = var.split('.')
            if len(parts) == 1:
                root = parts[0]
                if root not in nested_roots and root not in context:
                    context[root] = GalaxyVar('')
            else:
                # Handle multi-level nesting
                root = parts[0]
                if root not in context:
                    context[root] = GalaxyVar('')
                
                # Build the nested structure level by level
                current = context[root]
                for i in range(1, len(parts)):
                    nested_key = parts[i]
                    if isinstance(current, GalaxyVar):
                        if nested_key not in current._nested:
                            # If this is the last part, set empty string, otherwise create new GalaxyVar
                            if i == len(parts) - 1:
                                current.set_nested(nested_key, '')
                            else:
                                current.set_nested(nested_key, GalaxyVar(''))
                        current = current._nested[nested_key]

        tmpl = Template(source=cmd, searchList=[context])
        return tmpl.respond()
    
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
        env: dict[str, Any],
        params: List[RheaParam],
        tool_params: List[Param],
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
                
                file_var = GalaxyFileVar(tmp_file_path, param.filename)
                
                if param.name in env:
                    if isinstance(env[param.name], list):
                        env[param.name].append(file_var)
                    else:
                        env[param.name] = [env[param.name], file_var]
                else:
                    env[param.name] = file_var
                    
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
                env[param.name] = values
        
        # For params that were not provided (optional ones), put their default value
        for param in tool_params:
            if param.optional:
                if param.name not in env and param.name is not None:
                    if param.value is not None:
                        env[param.name] = param.value
            

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
                    env[out.name] = os.path.join(env["__tool_directory__"], out.from_work_dir) # Get the file out of the workdir

    def build_configfile(self, env: dict[str, str], configfile: ConfigFile) -> str:
        with NamedTemporaryFile('w', delete=False) as tf:
            script_path = tf.name
            text = self.expand_galaxy_if(configfile.text, env)
            tf.write(text)
            os.chmod(script_path, 0o755)
            env[configfile.name] = script_path
            return script_path

    @action
    async def run_tool(self, params: List[RheaParam]) -> RheaOutput:
        env = os.environ.copy()
        with (
            Store("rhea-input", self.connector, register=True) as input_store,
            Store("rhea-output", self.connector, register=True) as output_store,
        ):
            with TemporaryDirectory() as input, TemporaryDirectory() as output:
                cwd = output

                # Populate input environment variables
                self.build_env_parameters(env, params, self.tool.inputs.params, input, input_store)

                # Configure outputs
                self.build_output_env_parameters(env, output)

                # Configure configfiles (if any)
                if self.tool.configfiles is not None and self.tool.configfiles.configfiles is not None:
                    for configfile in self.tool.configfiles.configfiles:
                        self.build_configfile(env, configfile)
                
                # Configure command script
                cmd = self.apply_interpreter_command()
                cmd = self.expand_galaxy_if(cmd, env)
                cmd = cmd.replace('\n', ' ')
                cmd = self.unescape_bash_vars(cmd)
                cmd = self.fix_var_quotes(cmd)
                cmd = self.quote_shell_params(cmd)
                cmd = self.replace_dotted_vars(cmd)

                with NamedTemporaryFile("w", suffix=".sh", delete=False) as tf:
                    script_path = tf.name
                    tf.write("#!/usr/bin/env bash\n")
                    tf.write(cmd + "\n")
                    os.chmod(script_path, 0o755)

                # Remove any objects from environment
                for k, v in env.items():
                    if isinstance(v, list):
                        env[k] = str(v)
                    elif isinstance(v, GalaxyVar):
                        env[k] = str(v)
                    elif isinstance(v, GalaxyFileVar):
                        env[k] = str(v)
                
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
                    cmd, env=env, cwd=env["__tool_directory__"], capture_output=True, text=True
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
                                            env[out.name], output_store, name=out.name, format=out.format
                                        )
                                    )
                                except Exception:
                                    pass
                            else:
                                outputs.files.append(
                                    RheaDataOutput.from_file(
                                        env[out.name], output_store, name=out.name, format=out.format
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
