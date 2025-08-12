from dataclasses import dataclass
from logging import Logger
from openai import OpenAI
from academy.identifier import AgentId
from academy.exchange import UserExchangeClient
from academy.exchange.redis import RedisExchangeFactory
from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from utils.schema import Tool
from agent.tool import RheaToolAgent
from agent.schema import RheaDataOutput, RheaOutput
from pydantic import BaseModel, PrivateAttr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from typing import List, Optional, Literal, TYPE_CHECKING
from server.client_manager import ClientManager
from academy.handle import RemoteHandle
from datetime import datetime

if TYPE_CHECKING:
    from server.rhea_fastmcp import RheaResourceManager


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    host: str = "localhost"
    port: int = 3001
    debug_port: int | None = None

    # SQLAlchemy database url
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/rhea"

    # Client state configuration
    client_ttl: int = 3600

    # Parsl configuration
    parsl_container_backend: Literal["docker", "podman"] = "docker"
    parsl_container_network: Literal["host", "local"] = "host"
    parsl_container_debug: bool = False
    parsl_max_workers_per_node: int = 1
    parsl_provider: Literal["local", "pbs", "k8"] = "local"
    parsl_init_blocks: int = 0
    parsl_min_blocks: int = 0
    parsl_max_blocks: int = 5
    parsl_nodes_per_block: int = 1
    parsl_parallelism: int = 1

    # Time to wait to retrieve handle from agent
    agent_handle_timeout: int = 30

    redis_host: str = "localhost"
    redis_port: int = 6379

    embedding_url: str = "http://localhost:8000/v1"
    embedding_key: str = ""
    model: str = "Qwen/Qwen3-Embedding-0.6B"

    # Agent configuration
    # Agent may be executing on different host than MCP server.
    # Thus, it has its own variables for Redis and MinIO
    agent_redis_host: str = "localhost"
    agent_redis_port: int = 6379

    minio_endpoint: str = "localhost"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"


class PBSSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env_pbs", env_file_encoding="utf-8")

    account: str
    queue: str
    walltime: str
    scheduler_options: str
    select_options: str
    worker_init: str = ""  # Commands to run before workers launched
    cpus_per_node: int = 1  # Hardware threads per node


class K8Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env_k8", env_file_encoding="utf-8")

    namespace: str = "rhea"
    max_cpu: float = 2.0
    max_mem: str = "2048Mi"
    request_cpu: float = 1.0
    request_mem: str = "1024Mi"


class AgentState(BaseModel):
    tool_id: str
    last_accessed: float = datetime.now().timestamp()
    _handle: RemoteHandle = PrivateAttr()

    def __init__(self, handle: RemoteHandle, tool_id: str, **kwargs):
        super().__init__(tool_id=tool_id, **kwargs)
        self._handle = handle

    @property
    def handle(self):
        self.last_accessed = datetime.now().timestamp()
        return self._handle

    @handle.setter
    def handle(self, v: RemoteHandle):
        self._handle = v
        self.last_accessed = datetime.now().timestamp()


@dataclass
class AppContext:
    settings: Settings
    logger: Logger
    embedding_client: OpenAI
    db_sessionmaker: async_sessionmaker[AsyncSession]
    factory: RedisExchangeFactory
    connector: RedisConnector
    output_store: Store
    academy_client: UserExchangeClient
    agents: dict[str, AgentState]
    client_manager: ClientManager
    resource_manager: "RheaResourceManager"
    run_id: str


class MCPDataOutput(BaseModel):
    key: str
    size: int
    filename: str
    name: Optional[str] = None
    format: Optional[str] = None

    @classmethod
    def from_rhea(cls, p: RheaDataOutput):
        return cls(
            key=p.key.redis_key,
            size=p.size,
            filename=p.filename,
            name=p.name,
            format=p.format,
        )

    def to_rhea(self) -> RheaDataOutput:
        return RheaDataOutput(
            key=RedisKey(redis_key=self.key),
            size=self.size,
            filename=self.filename,
            name=self.name,
            format=self.format,
        )


class MCPOutput(BaseModel):
    return_code: int
    stdout: str
    stderr: str
    files: Optional[List[MCPDataOutput]] = None

    @classmethod
    def from_rhea(cls, p: RheaOutput):
        files = None
        if p.files is not None:
            files = []
            for f in p.files:
                files.append(MCPDataOutput.from_rhea(f))
        return cls(
            return_code=p.return_code, stdout=p.stdout, stderr=p.stderr, files=files
        )

    def to_rhea(self) -> RheaOutput:
        result = RheaOutput(
            return_code=self.return_code, stdout=self.stdout, stderr=self.stderr
        )

        if self.files:
            result.files = []
            for f in self.files:
                result.files.append(f.to_rhea())

        return result


class MCPTool(BaseModel):
    name: str
    description: str
    long_description: str

    @classmethod
    def from_rhea(cls, t: Tool):
        return cls(
            name=t.name or "",
            description=t.description,
            long_description=t.long_description
            or "Long description not available for this tool.",
        )
