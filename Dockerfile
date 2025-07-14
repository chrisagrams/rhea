FROM docker:24-dind AS docker-cli

FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim

WORKDIR /app

COPY . /app/

# Install "docker" command into the container 
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli /usr/local/libexec/docker/cli-plugins /usr/local/libexec/docker/cli-plugins

ENV PYTHONUNBUFFERED=1

RUN uv sync --locked

CMD ["uv", "run", "-m", "server.mcp_server", "--transport", "sse"]