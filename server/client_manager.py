from pydantic import BaseModel
from mcp.server.fastmcp.resources.base import Resource
from redis import Redis
from cachetools import TTLCache
from mcp.server.fastmcp.tools import Tool


class ClientState(BaseModel):
    _tools: dict[str, Tool] = {}
    _resources: dict[str, Resource] = {}


class ClientManager:
    def __init__(self, client_ttl: int = 3600):
        self.client_ttl = client_ttl
        self.client_states = {}

    def _get_key(self, client_id: str) -> str:
        return f"client_state:{client_id}"

    def get_client_state(self, client_id: str) -> ClientState:
        key = self._get_key(client_id)
        try:
            client_state: ClientState = self.client_states[key]
            return client_state
        except KeyError:
            return ClientState()

    def set_client_state(
        self,
        client_id: str,
        tools: dict[str, Tool] | None,
        resources: dict[str, Resource] | None,
    ) -> None:
        state = self.get_client_state(client_id)
        key = self._get_key(client_id)

        if tools is not None:
            state._tools = tools
        if resources is not None:
            state._resources = resources

        self.client_states[key] = state

    def clear_client_tools(self, client_id: str) -> None:
        state = self.get_client_state(client_id)
        self.set_client_state(client_id, {}, state._resources)


class LocalClientManager(ClientManager):
    def __init__(
        self,
        client_ttl: int = 3600,
        max_clients: int = 100,
    ):
        super().__init__(client_ttl=client_ttl)
        self.ttl_cache = TTLCache(maxsize=max_clients, ttl=client_ttl)

    def get_client_state(self, client_id: str) -> ClientState:
        key = self._get_key(client_id)
        try:
            client_state: ClientState = self.ttl_cache[key]
            self.ttl_cache[key] = client_state  # Refresh TTL on access
            return client_state
        except KeyError:  # Expired or non-existent key
            return ClientState()

    def set_client_state(
        self,
        client_id: str,
        tools: dict[str, Tool] | None,
        resources: dict[str, Resource] | None,
    ) -> None:
        state = self.get_client_state(client_id)
        key = self._get_key(client_id)

        if tools is not None:
            state._tools = tools
        if resources is not None:
            state._resources = resources

        self.ttl_cache[key] = state


class RedisClientManager(ClientManager):
    def __init__(self, redis_client: Redis, client_ttl: int = 3600):
        super().__init__(client_ttl=client_ttl)
        self.redis_client = redis_client

    def get_client_state(self, client_id: str) -> ClientState:
        key = self._get_key(client_id)
        raw = self.redis_client.get(key)
        if raw is None:
            return ClientState()

        # Refresh TTL on access
        self.redis_client.expire(key, self.client_ttl)

        return ClientState.model_validate_json(raw)  # type: ignore

    def set_client_state(
        self,
        client_id: str,
        tools: dict[str, Tool] | None,
        resources: dict[str, Resource] | None,
    ) -> None:
        state = self.get_client_state(client_id)
        key = self._get_key(client_id)

        if tools is not None:
            state._tools = tools
        if resources is not None:
            state._resources = resources

        payload = state.model_dump_json()

        self.redis_client.set(key, payload, ex=self.client_ttl)
