from __future__ import annotations
from pydantic import BaseModel, PrivateAttr

from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
from redis import Redis
import cloudpickle

import os
import logging
import filetype
import uuid
import io

logger = logging.getLogger(__name__)


def get_file_format(buffer: bytes) -> str:
    try:
        import magic

        m = magic.Magic(mime=True)
        format = m.from_buffer(buffer)
    except Exception:
        logging.warning(
            "'magic' failed to determine file format. Install libmagic if not available. Falling back to 'filetype'"
        )
        kind = filetype.guess(buffer)
        format = kind.mime if kind else "application/octet-stream"
    return format


class RheaFileHandle:
    def __init__(self, r: Redis, key: str | None = None):
        if key is None:
            self.key: str = f"file:{uuid.uuid4()}"
        else:
            self.key = key
        self._r: Redis = r
        self._pos: int = 0

    def append(self, chunk: bytes) -> None:
        self._r.append(self.key, chunk)

    def __len__(self) -> int:
        return self._r.strlen(self.key)  # type: ignore

    def filetype(self, probe_bytes: int = 4096) -> str:
        buf = self._r.getrange(self.key, 0, max(0, probe_bytes - 1)) or b""
        return get_file_format(buf)  # type: ignore

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            pos = offset
        elif whence == io.SEEK_CUR:
            pos = self._pos + offset
        elif whence == io.SEEK_END:
            pos = len(self) + offset
        else:
            raise ValueError("Invalid whence")
        if pos < 0:
            raise ValueError("Negative seek position")
        self._pos = pos
        return self._pos

    def read(self, size: int | None = -1) -> bytes:
        if size is None or size < 0:
            size = max(0, len(self) - self._pos)
        if size == 0:
            return b""
        start = self._pos
        end = start + size - 1
        data: bytes = self._r.getrange(self.key, start, end) or b""  # type: ignore
        self._pos += len(data)
        return data

    def iter_chunks(self, chunk_size: int = 1 << 20):
        while self._pos < len(self):
            yield self.read(chunk_size)


class RheaFileProxy(BaseModel):
    """
    A Pydantic model to represent a file stored in Redis.

    Attributes:
        name (str): Logical (or user provided) name of file.
        format (str): MIME type of file (magic/filetype).
        filename (str): Original filename.
        filesize (int): Size of the file in bytes.
        contents (bytes): Raw file contents.

    """

    name: str
    format: str
    filename: str
    filesize: int
    file_key: str
    _key: RedisKey | None = PrivateAttr()

    @classmethod
    def from_proxy(cls, key: RedisKey, store: Store) -> RheaFileProxy:
        data = store.get(key, deserializer=cloudpickle.loads)
        if data is None:
            raise ValueError(f"Key '{key}' not in store")
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str, r: Redis) -> RheaFileProxy:
        """
        Constructs a RheaFileProxy object from local file.
        *Does not put in proxy!* Must add to proxy using .to_proxy()
        """
        file_handle = RheaFileHandle(r=r)

        with open(path, "rb") as f:
            while chunk := f.read(1 << 20):
                file_handle.append(chunk)

        return cls(
            name=os.path.basename(path),
            format=file_handle.filetype(),
            filename=os.path.basename(path),
            filesize=len(file_handle),
            file_key=file_handle.key,
        )

    @classmethod
    def from_buffer(cls, name: str, contents: bytes, r: Redis) -> RheaFileProxy:
        file_handle = RheaFileHandle(r=r)
        file_handle.append(contents)
        return cls(
            name=name,
            format=file_handle.filetype(),
            filename=name,
            filesize=len(file_handle),
            file_key=file_handle.key,
        )

    def to_proxy(self, store: Store) -> str:
        proxy = store.proxy(self.model_dump(), serializer=cloudpickle.dumps)
        key = get_key(proxy)
        return key.redis_key  # type: ignore

    def open(self, r: Redis) -> RheaFileHandle:
        return RheaFileHandle(key=self.file_key, r=r)
