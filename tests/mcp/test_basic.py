import pytest


@pytest.mark.anyio
async def test_get_initial_tools(http_client_session):
    await http_client_session.initialize()
    result = await http_client_session.list_tools()
    assert result.tools[0].name == "find_tools"


@pytest.mark.anyio
async def test_find_tools(http_client_session):
    await http_client_session.initialize()
    tools = await http_client_session.list_tools()
    assert tools.tools[0].name == "find_tools"
    result = await http_client_session.call_tool(
        "find_tools", {"query": "CSV to tabular"}
    )
    assert result.structuredContent
    assert len(result.structuredContent["result"]) == 10


@pytest.mark.anyio
async def test_tool_call_no_rag(http_client_session, example_csv):
    """
    Call tool without performing RAG
    """
    await http_client_session.initialize()
    result = await http_client_session.call_tool(
        "csv_to_tabular", {"input1": example_csv, "header": True, "sep": ","}
    )
    assert result.structuredContent


@pytest.mark.anyio
async def test_tool_call_w_rag(http_client_session, example_csv):
    """
    Call tool after performing RAG
    """
    await http_client_session.initialize()
    await http_client_session.call_tool("find_tools", {"query": "CSV to tabular"})
    tools = await http_client_session.list_tools()
    assert len(tools.tools) == 11
    assert tools.tools[1].name == "csv_to_tabular"
    result = await http_client_session.call_tool(
        "csv_to_tabular", {"input1": example_csv, "header": True, "sep": ","}
    )
    assert result.structuredContent
