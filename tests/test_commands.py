import pytest
import pickle
from types import SimpleNamespace
from agent.tool import RheaToolAgent, RheaParam
from utils.schema import Tool, Test
from utils.process import process_inputs
from proxystore.connectors.redis import RedisConnector
from typing import List
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
    tool_id = "c198b9ec43cfbe0e"
    return tools.get(tool_id) or next(iter(tools.values()))

def test_simple_replace_galaxy_var_with_value(agent):
    agent.tool = SimpleNamespace(command='echo "\\${VAR:-5}"')
    agent.replace_galaxy_var("VAR", 10)
    assert agent.tool.command == 'echo 10'

def test_simple_replace_galaxy_var_with_default(agent):
    agent.tool = SimpleNamespace(command='echo "\\${VAR:-5}"')
    agent.replace_galaxy_var("VAR")
    assert agent.tool.command == 'echo 5'

def test_expand_galaxy_if(agent, sample_tool: Tool, connector, minio_client):
    agent.tool = sample_tool

    params = process_inputs(agent.tool, sample_tool.tests.tests[0], connector, minio_client, "dev")
    cmd = agent.expand_galaxy_if(sample_tool.command, params)

    assert True