import re
import unicodedata
from mcp.server.fastmcp import Context
from utils.schema import Tool, Inputs
from mcp.server.fastmcp.tools import Tool as FastMCPTool
from server.schema import MCPOutput, Settings
from agent.schema import RheaParam, RheaOutput
from proxystore.connectors.redis import RedisKey
from typing import List
from inspect import Signature, Parameter
from manager.launch_agent import launch_agent
from academy.handle import UnboundRemoteHandle, RemoteHandle


def construct_params(inputs: Inputs) -> List[Parameter]:
    res = []
    for param in inputs.params:
        res.append(param.to_python_parameter())
    return res


def process_user_inputs(inputs: Inputs, args: dict) -> List[RheaParam]:
    res = []

    for param in inputs.params:
        a = args.get(param.name, None)

        if a is not None:
            if param.type == "data":
                res.append(RheaParam.from_param(param, RedisKey(a)))
            else:
                res.append(RheaParam.from_param(param, a))

    return res


def sanitize_tool_name(text: str, repl: str = "_") -> str:
    if len(repl) != 1 or not re.match(r"[A-Za-z0-9_-]", repl):
        raise ValueError("`repl` must be a single allowed character.")

    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9_-]+", repl, text)
    text = re.sub(re.escape(repl) + r"+", repl, text)

    return text.strip(repl + "-")


def create_tool(tool: Tool, ctx: Context) -> FastMCPTool:
    params: List[Parameter] = construct_params(tool.inputs)

    # Add Context to tool params
    params.append(
        Parameter("ctx", kind=Parameter.POSITIONAL_OR_KEYWORD, annotation=Context)
    )

    sig = Signature(parameters=params, return_annotation=MCPOutput)

    def make_wrapper(tool_id, param_names):
        async def wrapper(*args, **kwargs):
            # Get context
            ctx: Context = kwargs.pop("ctx")
            await ctx.info(f"Launching tool {tool_id}")

            tool: Tool = ctx.request_context.lifespan_context.galaxy_tools[tool_id]

            await ctx.report_progress(0, 1)

            for name, value in zip(param_names, args):
                kwargs.setdefault(name, value)

            # Construct RheaParams
            rhea_params = process_user_inputs(tool.inputs, kwargs)

            await ctx.report_progress(0.05, 1)

            if tool_id not in ctx.request_context.lifespan_context.agents:
                # Get settings from app context
                settings: Settings = ctx.request_context.lifespan_context.settings

                # Launch agent
                future_handle = launch_agent(
                    tool,
                    redis_host=settings.agent_redis_host,
                    redis_port=settings.agent_redis_port,
                    minio_endpoint=settings.minio_endpoint,
                    minio_access_key=settings.minio_access_key,
                    minio_secret_key=settings.minio_secret_key,
                    minio_secure=False,
                )

                unbound_handle: UnboundRemoteHandle = future_handle.result()

                handle: RemoteHandle = unbound_handle.bind_to_client(
                    ctx.request_context.lifespan_context.academy_client
                )

                await ctx.info(f"Lanched agent {handle.agent_id}")

                ctx.request_context.lifespan_context.agents[tool_id] = handle

            # Get handle from dictionary
            handle: RemoteHandle = ctx.request_context.lifespan_context.agents[tool_id]

            await ctx.info(f"Executing tool {tool_id} in {handle.agent_id}")
            await ctx.report_progress(0.1, 1)

            # Execute tool
            tool_result: RheaOutput = await (await handle.run_tool(rhea_params))

            await ctx.info(f"Tool {tool_id} finished in {handle.agent_id}")
            await ctx.report_progress(1, 1)

            result = MCPOutput.from_rhea(tool_result)

            return result

        return wrapper

    # Create tool.call()
    if tool.name is None:
        tool.name = tool.user_provided_name
    safe_name = sanitize_tool_name(tool.name.lower())  # Normalize tool name
    fn = make_wrapper(tool.id, [name for name in params])
    fn.__name__ = safe_name
    fn.__doc__ = tool.description
    fn.__signature__ = sig  # type: ignore[attr-defined]

    fn.__annotations__ = {p.name: p.annotation for p in params}
    fn.__annotations__["return"] = MCPOutput

    return FastMCPTool.from_function(
        fn=fn, name=safe_name, title=tool.name, description=tool.description
    )
