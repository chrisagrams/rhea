import re
import unicodedata
import asyncio
import time
import copy
from mcp.server.fastmcp import Context
from utils.schema import Tool, Inputs
from mcp.server.fastmcp.tools import Tool as FastMCPTool
from mcp.server.fastmcp.resources import FunctionResource
from server.schema import MCPOutput, MCPDataOutput, Settings, AgentState
from agent.schema import RheaParam, RheaOutput
from utils.models import get_galaxytool_by_id
from manager.utils import get_handle_from_redis
from proxystore.connectors.redis import RedisKey
from proxystore.store import Store
from typing import List
from inspect import Signature, Parameter
from manager.launch_agent import launch_agent
from academy.handle import UnboundRemoteHandle, RemoteHandle
from pydantic import AnyUrl
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from redis import Redis

import server.metrics as metrics


def construct_params(inputs: Inputs) -> List[Parameter]:
    params = [param.to_python_parameter() for param in inputs.params]
    if inputs.conditionals is not None:
        for cond in inputs.conditionals:
            params.extend(cond.to_python_parameter())

    # Deduplicate by name, preserving first occurrence
    seen = {}
    for p in params:
        if p.name not in seen:
            seen[p.name] = p
    params = list(seen.values())

    # Split into those without a default, and those with one to prevent 'non-default argument follows default argument'
    no_default = [p for p in params if p.default is Parameter.empty]
    with_default = [p for p in params if p.default is not Parameter.empty]

    return no_default + with_default


def process_user_inputs(tool: Tool, args: dict) -> List[RheaParam]:
    res = []

    for param in tool.inputs.params:
        a = args.get(param.name, None)

        if a is not None:
            if param.type == "data":
                res.append(RheaParam.from_param(param, RedisKey(a)))
            else:
                res.append(RheaParam.from_param(param, a))

    if tool.inputs.conditionals is not None:
        for conditional in tool.inputs.conditionals:
            value = args.get(conditional.param.name)
            if value is not None:
                conditional_p = RheaParam.from_param(conditional.param, value)
                res.append(conditional_p)
                conditional_cp = copy.copy(conditional_p)
                conditional_cp.name = f"{conditional.name}.{conditional_p.name}"
                res.append(conditional_cp)
                for when in conditional.whens:
                    if when.value == value:
                        for p in when.params:
                            if arg_val := args.get(p.name):
                                if p.type == "data":
                                    arg_val = RedisKey(redis_key=arg_val)
                                rp = RheaParam.from_param(p, arg_val)
                            else:
                                rp = RheaParam.from_param(p, p.value)
                            res.append(rp)
                            cp = copy.copy(rp)
                            cp.name = f"{conditional.name}.{rp.name}"
                            res.append(cp)

    return res


def sanitize_tool_name(text: str, repl: str = "_") -> str:
    if len(repl) != 1 or not re.match(r"[A-Za-z0-9_-]", repl):
        raise ValueError("`repl` must be a single allowed character.")

    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9_-]+", repl, text)
    text = re.sub(re.escape(repl) + r"+", repl, text)

    return text.strip(repl + "-")


def create_proxystore_function_resource(
    file: MCPDataOutput, ctx: Context
) -> FunctionResource:
    async def _fetch() -> bytes:
        redis_key = RedisKey(redis_key=file.key)
        output_store: Store = ctx.request_context.lifespan_context.output_store

        res = output_store.get(redis_key)
        if res is None:
            raise ValueError(f"Key {file.key} not in ProxyStore")

        return res

    return FunctionResource(
        uri=AnyUrl(f"proxystore://{file.key}"),
        name=file.filename,
        title=file.name,
        description=f"Output file {file.filename} from tool call",
        mime_type="text/plain",
        fn=_fetch,
    )


def create_tool(tool: Tool, ctx: Context) -> FastMCPTool:
    params: List[Parameter] = []

    # Add Context to tool params
    params.append(
        Parameter("ctx", kind=Parameter.POSITIONAL_OR_KEYWORD, annotation=Context)
    )

    params += construct_params(tool.inputs)

    sig = Signature(parameters=params, return_annotation=MCPOutput)

    def make_wrapper(tool_id, param_names):
        async def wrapper(*args, **kwargs):
            try:
                # Log tool call
                metrics.tool_execution_request_count.inc()

                start_time = time.time()

                # Get context
                ctx: Context = kwargs.pop("ctx")
                await ctx.info(f"Launching tool {tool_id}")

                # Get settings from app context
                settings: Settings = ctx.request_context.lifespan_context.settings

                # Configure Redis client to get handle
                r = Redis(settings.redis_host, settings.redis_port)

                run_id = ctx.request_context.lifespan_context.run_id

                db_sessionmaker: async_sessionmaker[AsyncSession] = (
                    ctx.request_context.lifespan_context.db_sessionmaker
                )

                async with db_sessionmaker() as session:
                    tool: Tool | None = await get_galaxytool_by_id(session, tool_id)

                if tool is None:
                    raise RuntimeError(f"No tool found with ID: {tool_id}")

                await ctx.report_progress(0, 1)

                for name, value in zip(param_names, args):
                    kwargs.setdefault(name, value)

                # Construct RheaParams
                rhea_params = process_user_inputs(tool, kwargs)

                await ctx.report_progress(0.05, 1)

                if tool_id not in ctx.request_context.lifespan_context.agents:
                    # First, quickly check if the agent exists in other contexts
                    unbound_handle: UnboundRemoteHandle | None = (
                        await get_handle_from_redis(tool.id, run_id, r, timeout=1)
                    )

                    # Another context already initialized this tool, bind it to this Academy client:
                    if unbound_handle is not None:
                        handle: RemoteHandle = unbound_handle.bind_to_client(
                            ctx.request_context.lifespan_context.academy_client
                        )
                        ctx.request_context.lifespan_context.agents[tool_id] = (
                            AgentState(tool_id=tool_id, handle=handle)
                        )

                    else:
                        # Launch agent
                        launch_agent(
                            tool,
                            run_id=run_id,
                            redis_host=settings.agent_redis_host,
                            redis_port=settings.agent_redis_port,
                            minio_endpoint=settings.minio_endpoint,
                            minio_access_key=settings.minio_access_key,
                            minio_secret_key=settings.minio_secret_key,
                            minio_secure=False,
                        )

                        unbound_handle: UnboundRemoteHandle | None = (
                            await get_handle_from_redis(
                                tool.id,
                                run_id,
                                r,
                                timeout=settings.agent_handle_timeout,
                            )
                        )

                        if unbound_handle is None:
                            raise RuntimeError(
                                "Never received handle from Parsl worker."
                            )

                        handle: RemoteHandle = unbound_handle.bind_to_client(
                            ctx.request_context.lifespan_context.academy_client
                        )

                        await ctx.info(f"Lanched agent {handle.agent_id}")

                        ctx.request_context.lifespan_context.agents[tool_id] = (
                            AgentState(tool_id=tool_id, handle=handle)
                        )

                # Get handle from dictionary
                handle: RemoteHandle = ctx.request_context.lifespan_context.agents[
                    tool_id
                ].handle

                await ctx.info(f"Executing tool {tool_id} in {handle.agent_id}")
                await ctx.report_progress(0.1, 1)

                # Execute tool
                tool_result: RheaOutput = await (await handle.run_tool(rhea_params))

                await ctx.info(f"Tool {tool_id} finished in {handle.agent_id}")
                await ctx.report_progress(1, 1)

                result = MCPOutput.from_rhea(tool_result)

                # Add ProxyStore resource
                if result.files is not None:
                    for file in result.files:
                        ctx.request_context.lifespan_context.resource_manager.add_resource_to_context(
                            resource=create_proxystore_function_resource(file, ctx)
                        )

                # Notify the client that we have new output resources
                await ctx.request_context.session.send_resource_list_changed()

                # Log execution time
                metrics.tool_execution_runtime.observe(time.time() - start_time)

                # Log successful tool execution
                metrics.successful_tool_executions.inc()

                return result

            except Exception as e:
                # Log failed tool execution
                metrics.failed_tool_executions.inc()
                raise

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
