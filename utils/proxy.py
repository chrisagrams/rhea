from __future__ import annotations
from pydantic import BaseModel, PrivateAttr

from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key

import os
import logging
import filetype

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
    contents: bytes
    _key: RedisKey | None = PrivateAttr()

    @classmethod
    def from_proxy(cls, key: RedisKey, store: Store) -> RheaFileProxy:
        obj: RheaFileProxy | None = store.get(key)
        if obj is None:
            raise ValueError(f"Key '{key}' not in store")
        return obj

    @classmethod
    def from_file(cls, path: str) -> RheaFileProxy:
        """
        Constructs a RheaFileProxy object from local file.
        *Does not put in proxy!* Must add to proxy using .to_proxy()
        """
        with open(path, "rb") as f:
            contents: bytes = f.read()

        return cls(
            name=os.path.basename(path),
            format=get_file_format(contents),
            filename=os.path.basename(path),
            filesize=len(contents),
            contents=contents,
        )

    @classmethod
    def from_buffer(cls, name: str, contents: bytes) -> RheaFileProxy:
        return cls(
            name=name,
            format=get_file_format(contents),
            filename=name,
            filesize=len(contents),
            contents=contents,
        )

    def to_proxy(self, store: Store) -> str:
        proxy = store.proxy(self)
        key = get_key(proxy)
        return key.redis_key  # type: ignore
