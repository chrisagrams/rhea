from dataclasses import dataclass
from logging import Logger
from chromadb.api import ClientAPI
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
from chromadb.api.models.Collection import Collection
from academy.identifier import AgentId
from academy.exchange import UserExchangeClient
from academy.exchange.redis import RedisExchangeFactory
from utils.schema import Tool
from agent.tool import RheaToolAgent, RheaDataOutput, RheaOutput
from pydantic import BaseModel
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