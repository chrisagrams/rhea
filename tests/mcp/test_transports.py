import pytest
from mcp.types import InitializeResult, ServerCapabilities


@pytest.mark.anyio
async def test_http_client(http_client_session):
    await http_client_session.initialize()
    result = await http_client_session.list_tools()
    assert result.tools[0].name == "find_tools"


@pytest.mark.anyio
async def test_sse_client(sse_client_session):
    await sse_client_session.initialize()
    result = await sse_client_session.list_tools()
    assert result.tools[0].name == "find_tools"


@pytest.mark.anyio
async def test_http_capabilities(http_client_session):
    handshake: InitializeResult = await http_client_session.initialize()
    capabilities: ServerCapabilities = handshake.capabilities
    assert capabilities.tools
    assert capabilities.tools.listChanged == True
    assert capabilities.resources
    assert capabilities.resources.listChanged == True


@pytest.mark.anyio
async def test_sse_capabilities(sse_client_session):
    handshake: InitializeResult = await sse_client_session.initialize()
    capabilities: ServerCapabilities = handshake.capabilities
    assert capabilities.tools
    assert capabilities.tools.listChanged == True
    assert capabilities.resources
    assert capabilities.resources.listChanged == True
