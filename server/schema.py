from dataclasses import dataclass
from logging import Logger
from chromadb.api import ClientAPI
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
from chromadb.api.models.Collection import Collection
from academy.identifier import AgentId
from academy.exchange import UserExchangeClient
from academy.exchange.redis import RedisExchangeFactory
from utils.schema import Tool
from agent.tool import RheaToolAgent
from agent.schema import RheaDataOutput, RheaOutput
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional

@dataclass
class AppContext:
    logger: Logger
    chroma_client: ClientAPI
    openai_ef: OpenAIEmbeddingFunction
    collection: Collection
    factory: RedisExchangeFactory
    academy_client: UserExchangeClient
    galaxy_tools: dict[str, Tool]
    agents: dict[str, AgentId[RheaToolAgent]]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    debug_port: int | None = None
    pickle_file: str = "tools_dict.pkl"
    
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



class MCPDataOutput(BaseModel):
    key: str
    size: int
    filename: str
    name: Optional[str] = None

    @classmethod
    def from_rhea(cls, p: RheaDataOutput):
        return cls(
            key=p.key.redis_key,
            size=p.size,
            filename=p.filename,
            name=p.name
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