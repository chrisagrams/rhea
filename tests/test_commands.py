import pytest
import pickle
import subprocess
import json
import os
import shlex
from types import SimpleNamespace
from agent.tool import RheaToolAgent
from utils.schema import Tool, Test
from utils.process import process_inputs
from proxystore.connectors.redis import RedisConnector
from proxystore.store import Store
from typing import List, Dict, Any
from minio import Minio


@pytest.fixture
def agent():
    agent = RheaToolAgent.__new__(RheaToolAgent)
    return agent


@pytest.fixture
def tools():
    with open("tools_dict.pkl", "rb") as f:
        return pickle.load(f)


@pytest.fixture
def connector():
    return RedisConnector("localhost", 6379)


@pytest.fixture
def minio_client():
    return Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password",
        secure=False,
    )


@pytest.fixture
def sample_tool(tools):
    # tool_id = "c198b9ec43cfbe0e"
    # tool_id = "783bde422b425bd9"
    # tool_id = "a74ca2106a7a2073"
    # tool_id = "adf651eab94cc80c"
    # tool_id = "8423143bf85371a0"
    # tool_id = "8e36777d470b3c19"
    # tool_id = "fa1c79f582a17d50"
    # tool_id = "790743498728befc"
    # tool_id = "9c80f36219a53991"

    # tool_id = "82ab1904790830ef"
    # tool_id = "46188e84a762dfdb"
    # tool_id = "db44833d587592d4"
    # tool_id = "693ed3fbdee03329"
    # tool_id = "c8658b82d8429f5d"
    # tool_id = "de07f280bfbbbd77"
    # tool_id = "d32aec850d519e76"
    # tool_id = "146d94f3f5a366b3"
    # tool_id = "038c16bdafb1198d"
    # tool_id = "e80befe4deb80218"
    # tool_id = "3d6a9a001720230c"
    # tool_id = "e419dee9cfaa2f3f"
    tool_id = "ec6d2afcc6959e78"
 
    return tools.get(tool_id) or next(iter(tools.values()))


ignore_codes = [
    2164, #Use 'cd ... || exit' or 'cd ... || return' in case cd fails.
]

def shellcheck_lint(script: str, env: dict[str, str]) -> List[Dict[str, Any]]:
    header_lines = []
    for k, v in env.items():
        if not k or v is None:
            continue
        if not isinstance(v, str):
            v = str(v)
        header_lines.append(f"export {k}={shlex.quote(v)}")

    header = "\n".join(header_lines)
    full_script = header + "\n" + script

    cmd = ["shellcheck", "-s", "bash", "-f", "json"]
    codes = ",".join(f"SC{c}" for c in ignore_codes)
    cmd += ["-e", codes]
    cmd += ["-"]

    result = subprocess.run(
        cmd,
        input=full_script,
        text=True,
        capture_output=True,
        check=False,
    )
    if not result.stdout:
        return []
    data = json.loads(result.stdout)
    return data


def test_simple_replace_galaxy_var_with_value(agent):
    agent.tool = SimpleNamespace(command='echo "\\${VAR:-5}"')
    agent.replace_galaxy_var("VAR", 10)
    assert agent.tool.command == "echo 10"


def test_simple_replace_galaxy_var_with_default(agent):
    agent.tool = SimpleNamespace(command='echo "\\${VAR:-5}"')
    agent.replace_galaxy_var("VAR")
    assert agent.tool.command == "echo 5"


def test_configfiles(agent, sample_tool: Tool, connector, minio_client):
    agent.tool = sample_tool
    agent.minio = minio_client

    if len(sample_tool.tests.tests) == 0:
        assert True
        return

    params = process_inputs(
        agent.tool, sample_tool.tests.tests[0], connector, minio_client, "dev"
    )
    with (
        Store("rhea-test-input", connector, register=True) as input_store,
        Store("rhea-test-output", connector, register=True) as output_store,
    ):
        env = os.environ.copy()
        agent.build_env_parameters(env, params, sample_tool.inputs.params, "/tmp", input_store)
        agent.build_output_env_parameters(env, "/tmp")
        if sample_tool.configfiles is not None:
            if sample_tool.configfiles.configfiles is not None:
                for configfile in sample_tool.configfiles.configfiles:
                    configfile_path = agent.build_configfile(env, configfile)
                    assert isinstance(configfile_path, str)
        else:
            assert True


def test_expand_galaxy_if(agent, sample_tool: Tool, connector, minio_client):
    agent.tool = sample_tool
    agent.minio = minio_client

    if len(sample_tool.tests.tests) == 0:
        assert True
        return
    
    try:
        params = process_inputs(
            agent.tool, sample_tool.tests.tests[0], connector, minio_client, "dev"
        )
        with (
            Store("rhea-test-input", connector, register=True) as input_store,
            Store("rhea-test-output", connector, register=True) as output_store,
        ):
            env = os.environ.copy()
            agent.build_env_parameters(env, params, sample_tool.inputs.params, "/tmp", input_store)
            agent.build_output_env_parameters(env, "/tmp")
            if sample_tool.configfiles is not None:
                if sample_tool.configfiles.configfiles is not None:
                    for configfile in sample_tool.configfiles.configfiles:
                        agent.build_configfile(env, configfile)
            cmd = agent.expand_galaxy_if(sample_tool.command, env)
            cmd = cmd.replace('\n', ' ')
            cmd = agent.unescape_bash_vars(cmd)
            cmd = agent.fix_var_quotes(cmd)
            cmd = agent.quote_shell_params(cmd)
            cmd = agent.replace_dotted_vars(cmd)
            issues = shellcheck_lint(cmd, env)
            assert len(issues) == 0
    except ValueError as e:
        msg = str(e)
        if "not found in bucket" in msg:
            assert True
            return


def test_all_expand_galaxy_if(agent, tools, connector, minio_client):
    passed = []
    failed = []
    limit = len(tools.items())
    # limit = 100
    count = 0
    for tool_id, tool in tools.items():
        agent.tool = tool
        try:
            test_expand_galaxy_if(agent, tool, connector, minio_client)
            passed.append(tool_id)
        except Exception:
            failed.append(tool_id)
        count += 1
        if count == limit: 
            break
    total = len(passed) + len(failed)
    num_passed = len(passed)
    assert not failed, (
        f"{num_passed}/{total} passed; failures: {failed[0:10]}..."
    )