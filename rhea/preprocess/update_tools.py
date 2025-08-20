import os
import asyncio
import logging
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)
from typing import List, Dict

from rhea.preprocess.utils.fetch import get_galaxy_repositories
from rhea.utils.models import get_all_tool_ids

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_update_list(db_session: AsyncSession) -> set[str]:
    upstream_tools: List[Dict] = get_galaxy_repositories()
    deduped_tools = {}
    for tool in upstream_tools:
        if tool.get("type") == "repository_suite_definition" or tool.get("deprecated"):
            continue
        key = (tool.get("name"), tool.get("description"))
        times_downloaded = tool.get("times_downloaded", 0)
        if key not in deduped_tools or times_downloaded > deduped_tools[key].get(
            "times_downloaded", 0
        ):
            deduped_tools[key] = tool
    upstream_tool_set: set[str] = set(tool["id"] for tool in deduped_tools.values())
    local_tool_list: List[str] | None = await get_all_tool_ids(db_session)
    if not local_tool_list:
        raise RuntimeError("Failed to get tools from local DB.")
    local_tools_set: set[str] = set(local_tool_list)

    new_tools_set = upstream_tool_set - local_tools_set

    logger.info(f"{len(new_tools_set)} new tools found.")

    return new_tools_set


async def main():
    DATABASE_URL = os.environ.get(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/rhea"
    )
    engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False, future=True)
    AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    async with AsyncSessionLocal() as db_session:
        new_tool_set: set[str] = await get_update_list(db_session)


if __name__ == "__main__":
    asyncio.run(main())
