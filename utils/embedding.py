from openai import OpenAI
from typing import List
from utils.schema import Tool

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
