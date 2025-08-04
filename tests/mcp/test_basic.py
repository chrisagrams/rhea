import pytest


@pytest.mark.anyio
async def test_get_initial_tools(client_session):
    result = await client_session.list_tools()
    assert result.tools[0].name == "find_tools"


@pytest.mark.anyio
async def test_find_tools(client_session):
    tools = await client_session.list_tools()
    assert tools.tools[0].name == "find_tools"
    result = await client_session.call_tool("find_tools", {"query": "CSV to tabular"})
    assert result.structuredContent
    assert len(result.structuredContent["result"]) == 10


@pytest.mark.anyio
async def test_tool_call_no_rag(client_session, example_csv):
    """
    Call tool without performing rag
    """
    result = await client_session.call_tool(
        "csv_to_tabular", {"input1": example_csv, "header": True, "sep": ","}
    )
    assert result.structuredContent
