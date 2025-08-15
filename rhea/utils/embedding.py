from openai import OpenAI
from typing import List

from rhea.utils.schema import Tool
from rhea.utils.models import GalaxyTool

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

template = """# {name}

**Description**  
{description}

---

## Long Description

{long_description}

## README

{readme}
"""


def get_embedding(input_text: str, client: OpenAI, model: str) -> List[float]:
    response = client.embeddings.create(
        model=model, input=input_text, encoding_format="float"
    )
    embedding: List[float] = response.data[0].embedding
    return embedding


def generate_tool_documentation_embedding(
    t: Tool, client: OpenAI, model: str
) -> List[float]:
    return get_embedding(
        input_text=template.format(
            name=t.name or t.user_provided_name,
            description=t.description,
            long_description=t.long_description,
            readme=t.documentation,
        ),
        client=client,
        model=model,
    )


async def get_l2_distance(
    query_vec: List[float], session: AsyncSession, limit: int = 10
) -> List[Tool]:
    dist_col = GalaxyTool.embedding.l2_distance(query_vec).label("distance")

    result = await session.execute(
        (select(GalaxyTool, dist_col).order_by(dist_col).limit(limit))
    )

    rows = result.all()
    return [orm_obj.definition for orm_obj, _dist in rows]
