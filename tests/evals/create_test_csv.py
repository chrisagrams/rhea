import argparse
import asyncio
import csv
import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)

from utils.models import GalaxyTool


async def export_tools(db_url: str, limit: int, output: str):
    engine: AsyncEngine = create_async_engine(db_url, echo=False, future=True)
    AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(GalaxyTool).limit(limit))
        tools = result.scalars().all()

    with open(output, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "id",
                "name",
                "user_provided_name",
                # "description",
                # "long_description",
                # "documentation",
                # "definition_json",
            ]
        )
        for t in tools:
            writer.writerow(
                [
                    t.id,
                    t.name,
                    t.user_provided_name or "",
                    # (t.description or "").replace("\n", " "),
                    # (t.long_description or "").replace("\n", " "),
                    # (t.documentation or "").replace("\n", " "),
                    # json.dumps(t.definition.model_dump()),
                ]
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export up to N GalaxyTool records to CSV"
    )
    parser.add_argument(
        "n",
        type=int,
        help="Maximum number of tools to export",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/rhea",
        help="Database URL",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="galaxytools_export.csv",
        help="Output CSV file path",
    )
    args = parser.parse_args()
    asyncio.run(export_tools(args.db_url, args.n, args.output))
