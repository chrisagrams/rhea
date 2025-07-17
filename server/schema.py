from dataclasses import dataclass
from logging import Logger
from chromadb.api import ClientAPI
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
from chromadb.api.models.Collection import Collection
from academy.identifier import AgentId
from academy.exchange import UserExchangeClient
from academy.exchange.redis import RedisExchangeFactory
from proxystore.connectors.redis import RedisConnector
from utils.schema import Tool
from agent.tool import RheaToolAgent
from agent.schema import RheaDataOutput, RheaOutput
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional, Literal

@dataclass
class AppContext:
    logger: Logger
    chroma_client: ClientAPI
    openai_ef: OpenAIEmbeddingFunction
    collection: Collection
    factory: RedisExchangeFactory
    connector: RedisConnector
    academy_client: UserExchangeClient
    galaxy_tools: dict[str, Tool]
    agents: dict[str, AgentId[RheaToolAgent]]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    host: str = "localhost"
    port: int = 3001
    debug_port: int | None = None
    pickle_file: str = "tools_dict.pkl"

    # Parsl configuration
    parsl_container_backend: Literal["docker", "podman"] = "docker"
    parsl_container_network: Literal["host", "local"] = "host"
    parsl_container_debug: bool = False
    parsl_max_workers_per_node: int = 1
    parsl_provider: Literal["local", "pbs"] = "local"
    parsl_init_blocks: int = 0
    parsl_min_blocks: int = 0
    parsl_max_blocks: int = 5
    parsl_nodes_per_block: int = 1
    parsl_parallelism: int = 1
    
    redis_host: str = "localhost"
    redis_port: int = 6379

    vllm_url: str = "http://localhost:8000/v1"
    vllm_key: str = ""
    model: str = "Qwen/Qwen3-Embedding-0.6B"

    chroma_host: str = "localhost"
    chroma_port: int = 8001
    chroma_collection: str | None = None

    # Agent configuration
    # Agent may be executing on different host than MCP server.
    # Thus, it has its own variables for Redis and MinIO
    agent_redis_host: str = "localhost"
    agent_redis_port: int = 6379

    minio_endpoint: str = "localhost"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"


class PBSSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env_pbs', env_file_encoding='utf-8')

    account: str 
    queue: str
    walltime: str
    scheduler_options: str
    select_options: str
    worker_init: str = '' # Commands to run before workers launched
    cpus_per_node: int = 1 # Hardware threads per node


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
            format=p.format
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
            return_code=p.return_code,
            stdout=p.stdout,
            stderr=p.stderr,
            files=files
        )


class MCPTool(BaseModel):
    name: str
    description: str
    long_description: str

    @classmethod
    def from_rhea(cls, t: Tool):
        return cls(
            name=t.name,
            description=t.description,
            long_description=t.long_description or "Long description not available for this tool."
        )