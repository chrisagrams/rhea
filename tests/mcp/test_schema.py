import pytest
from sqlalchemy import select
from typing import List, Dict, Any
from inspect import Signature, Parameter

from agent.schema import RheaParam

from utils.schema import Tool
from utils.models import GalaxyTool, get_galaxytool_by_name
from utils.process import process_inputs

from tests.evals.helpers import unwrap_user_inputs

from server.utils import construct_params, process_user_inputs


@pytest.mark.parametrize("anyio_backend", ["asyncio"])
async def test_single_tool_schema(anyio_backend, db_session, connector, minio_client):

    tool: Tool | None = await get_galaxytool_by_name(db_session, "2d_auto_threshold")
    assert tool

    for test in tool.tests.tests:
        manager_params: List[RheaParam] = process_inputs(
            tool, test, connector, minio_client, "dev"
        )
        mcp_params: List[Parameter] = construct_params(tool.inputs)

        assert len(manager_params) == len(mcp_params)
