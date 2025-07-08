from sqlalchemy import (
    create_engine,
    Column,
    String,
    Boolean,
    Integer,
    Table,
    DateTime,
    ForeignKey,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime

Base = declarative_base()

tool_category = Table(
    "tool_category",
    Base.metadata,
    Column("tool_id", String, ForeignKey("tools.id"), primary_key=True),
    Column("category_id", String, ForeignKey("categories.id"), primary_key=True),
)


class Category(Base):
    __tablename__ = "categories"
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    deleted = Column(Boolean, default=False)
    url = Column(String)
    repositories = Column(Integer)
    tools = relationship("Tool", secondary=tool_category, back_populates="categories")


class Tool(Base):
    __tablename__ = "tools"
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    type = Column(String)
    remote_repository_url = Column(String)
    homepage_url = Column(String)
    description = Column(Text)
    long_description = Column(Text)
    user_id = Column(String)
    private = Column(Boolean, default=False)
    deleted = Column(Boolean, default=False)
    times_downloaded = Column(Integer, default=0)
    deprecated = Column(Boolean, default=False)
    create_time = Column(DateTime, default=datetime.utcnow)
    owner = Column(String)
    categories = relationship(
        "Category", secondary=tool_category, back_populates="tools"
    )
