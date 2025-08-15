import pytest
from sqlalchemy import select
from typing import List, Dict, Any
from inspect import Signature, Parameter

from rhea.agent.schema import RheaParam, RheaFileParam

from rhea.utils.schema import Tool
from rhea.utils.models import GalaxyTool, get_galaxytool_by_name
from rhea.utils.process import process_inputs

from tests.evals.helpers import unwrap_user_inputs

from rhea.server.utils import construct_params, process_user_inputs

TOOL_NAMES = [
    "fasta_to_fastq",
    "10x_bamtofastq",
    "2d_auto_threshold",
    "fastqc",
    "query_tabular",
    "bowtie2",
    "fastp",
    "tabular_to_fasta",
    "fasta_compute_length",
    "fasta_formatter",
    "thermo_raw_file_converter",
    "openms_msfraggeradapter",
]


@pytest.mark.parametrize("tool_name", TOOL_NAMES)
@pytest.mark.parametrize("anyio_backend", ["asyncio"])
async def test_single_tool_schema(
    anyio_backend, tool_name, db_session, connector, minio_client
):

    tool: Tool | None = await get_galaxytool_by_name(db_session, tool_name)
    assert tool

    for test in tool.tests.tests:
        manager_params: List[RheaParam] = process_inputs(
            tool, test, connector, minio_client, "dev"
        )
        mcp_params: List[Parameter] = construct_params(tool.inputs)
        test_params: Dict[str, Any] = unwrap_user_inputs(manager_params)

        # assert len(mcp_params) == len(test_params.keys())

        resulting_params: List[RheaParam] = process_user_inputs(tool, test_params)

        for param in manager_params:
            if isinstance(param, RheaFileParam):
                param.filename = None
                param.value = None  # type: ignore
        for param in resulting_params:
            if isinstance(param, RheaFileParam):
                param.filename = None
                param.value = None  # type: ignore
        assert len(manager_params) == len(resulting_params)
        assert [str(p) for p in manager_params] == [str(p) for p in resulting_params]
