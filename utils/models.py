from sqlalchemy import (
    Column,
    String,
    Index,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from pgvector.sqlalchemy import Vector
from utils.schema import Tool

Base = declarative_base()


class GalaxyTool(Base):
    __tablename__ = "galaxytools"
    __table_args__ = (
        Index(
            "ix_galaxytools_embedding",
            "embedding",
            postgresql_using="ivfflat",
            postgresql_ops={"embedding": "vector_l2_ops"},
        ),
    )
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    user_provided_name = Column(String)
    description = Column(Text)
    long_description = Column(Text)
    documentation = Column(Text)
    _definition = Column("definition", JSONB, nullable=False)
    embedding = Column(Vector(1024), nullable=False)

    @property
    def definition(self) -> Tool:
        return Tool.model_validate(self._definition)

    @definition.setter
    def definition(self, t: Tool | dict):
        if isinstance(t, Tool):
            self._definition = t.model_dump()
        else:
            self._definition = Tool.model_validate(t).model_dump()
