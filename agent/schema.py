from __future__ import annotations
import os
import re
import glob
from typing import Any, List, Optional, cast
from dataclasses import dataclass
from utils.schema import Param, CollectionOutput
from utils.proxy import RheaFileProxy
from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key
from collections.abc import Mapping, MutableMapping


class GalaxyFileVar:
    def __init__(self, path: str, filename: Optional[str] = None):
        self.path = path
        self.filename = filename
        self._value = path

    def __str__(self):
        return self.path

    @property
    def ext(self):
        """Get file extension"""
        if self.filename:
            return self.filename.split(".")[-1]
        return os.path.splitext(self.path)[1].lstrip(".")

    def is_of_type(self, file_type: str) -> bool:
        """Check if file is of given type"""
        ext = self.ext.lower()

        type_mappings = {
            "fasta": ["fa", "fasta", "fna", "ffn", "faa", "frn"],
            "fastq": ["fq", "fastq"],
            "sam": ["sam"],
            "bam": ["bam"],
            "vcf": ["vcf"],
            "gff": ["gff", "gff3"],
            "bed": ["bed"],
        }

        if file_type.lower() in type_mappings:
            return ext in type_mappings[file_type.lower()]

        return ext == file_type.lower()

    @property
    def element_identifier(self):
        return self.filename


class GalaxyVar:
    def __init__(self, value=None):
        self._value = {} if value is None else value
        self._nested = {}

    def __str__(self):
        v = self._value
        if v is None:
            return ""
        if isinstance(v, (list, tuple)):
            return " ".join(str(x) for x in v)
        return str(v)

    def _wrap(self, v):
        if isinstance(v, (GalaxyVar, GalaxyFileVar)):
            return v
        if isinstance(v, (list, tuple, Mapping)):
            return GalaxyVar(v)
        return v

    def set_nested(self, key: str, value: Any):
        parts = key.split(".")
        cur: GalaxyVar = self
        for i, p in enumerate(parts):
            last = i == len(parts) - 1
            if last:
                v = (
                    value
                    if isinstance(value, (GalaxyVar, GalaxyFileVar))
                    else (
                        GalaxyVar(value)
                        if isinstance(value, (list, tuple, Mapping))
                        else value
                    )
                )
                cur._nested[p] = v
                if isinstance(cur._value, MutableMapping):
                    cast(MutableMapping[str, Any], cur._value)[p] = v
            else:
                nxt = cur._nested.get(p)
                if not isinstance(nxt, GalaxyVar):
                    nxt = GalaxyVar({})
                    cur._nested[p] = nxt
                    if isinstance(cur._value, MutableMapping):
                        cast(MutableMapping[str, Any], cur._value)[p] = nxt
                cur = nxt

    def __getattr__(self, name):
        if name in self._nested:
            return self._nested[name]
        v = self._value
        if isinstance(v, Mapping) and name in v:
            return v[name]
        if hasattr(v, name):
            return getattr(v, name)
        return ""

    def __getitem__(self, key):
        if key in self._nested:
            return self._nested[key]
        v = self._value
        if isinstance(v, Mapping) and key in v:
            return v[key]
        if isinstance(v, (list, tuple)) and (
            isinstance(key, int) or isinstance(key, slice)
        ):
            return v[key]
        try:
            return v[key]
        except Exception:
            return ""

    def __setitem__(self, key, value):
        self.set_nested(str(key), value)

    def __iter__(self):
        try:
            return iter(self._value)
        except TypeError:
            return iter(())

    def __len__(self):
        try:
            return len(self._value)
        except TypeError:
            return 0

    def __contains__(self, item):
        if item in self._nested:
            return True
        v = self._value
        if isinstance(v, Mapping):
            return item in v
        try:
            return item in v
        except TypeError:
            return False

    def __bool__(self):
        return bool(self._value) or bool(self._nested)

    def __eq__(self, other):
        if isinstance(other, GalaxyVar):
            return self._value == other._value and self._nested == other._nested
        return self._value == other


class RheaParam:
    def __init__(self, name: str, type: str, argument: str | None = None) -> None:
        self.name = name
        self.type = type
        self.argument = argument

    def __str__(self) -> str:
        arg = f", argument={self.argument!r}" if self.argument is not None else ""
        return f"RheaParam(name={self.name!r}, type={self.type!r}{arg})"

    __repr__ = __str__

    @classmethod
    def from_param(cls, param: Param, value: Any) -> "RheaParam":
        if param.name is None and param.argument is not None:
            # An edge case where name is not specified in the param,
            # but its assumed its the same as argument.
            param.name = param.argument.replace("--", "")
        if param.type == "data":  # RheaFileParam
            if type(value) is not RedisKey:
                raise ValueError("Value must be a 'RedisKey' for data param.")
            return RheaFileParam.from_param(param, value)
        elif param.type == "text":  # RheaTextParam
            if param.optional and value is None:
                return RheaTextParam.from_param(param, "")
            if type(value) is not str:
                raise ValueError("Value must be a 'str' for text param.")
            return RheaTextParam.from_param(param, value)
        elif param.type == "integer":  # RheaIntegerParam
            if isinstance(value, str):
                try:
                    if param.optional and value is None or value == "":
                        param.type = "text"
                        return RheaTextParam.from_param(param, value)
                    value = int(value)
                except ValueError:
                    raise ValueError(
                        "Value must be an 'int' or string castable to 'int' for integer param."
                    )
            if not isinstance(value, int):
                raise ValueError("Value must be an 'int' for integer param.")
            return RheaIntegerParam.from_param(param, value)
        elif param.type == "float":  # RheaFloatParam
            if isinstance(value, str):
                try:
                    value = float(value)
                except ValueError:
                    raise ValueError(
                        "Value must be a 'float' or string castable to 'float' for float param."
                    )
            if not isinstance(value, float):
                raise ValueError("Value must be a 'float' for float param.")
            return RheaFloatParam.from_param(param, value)
        elif param.type == "boolean":
            if value is None and param.checked is not None:
                value = param.checked
            if value is None:
                raise ValueError("Value is None.")
            if type(value) is not bool:
                if value.lower() == "true" or value == param.truevalue:
                    value = True
                elif value.lower() == "false" or value == param.falsevalue:
                    value = False
                else:
                    raise ValueError("Value must be a 'bool' for boolean param.")
            return RheaBooleanParam.from_param(param, value)
        elif param.type == "select" and param.multiple:
            if type(value) is not str:
                raise ValueError("Value must be a 'str' for select param.")
            values = value.split(",")
            if len(value) < 1:
                raise ValueError("Unpacked params is empty.")
            return RheaMultiSelectParam.from_param(param, values)
        elif param.type == "select":
            if type(value) is not str:
                if param.options is not None:
                    for option in param.options:
                        if option.selected:
                            return RheaSelectParam.from_param(param, option.value)
                raise ValueError("Value must be a 'str' for select param.")
            return RheaSelectParam.from_param(param, value)
        elif param.type == "data_column":
            return RheaTextParam.from_param(param, value)
        raise NotImplementedError(f"Param {param.type} not implemented.")


class RheaFileParam(RheaParam):
    def __init__(
        self,
        name: str,
        type: str,
        format: str,
        value: RedisKey,
        argument: str | None = None,
        filename: str | None = None,
        path: str | None = None,
    ) -> None:
        super().__init__(name, type, argument)
        self.format = format
        self.value = value
        self.filename = filename
        self.path = path

    def __str__(self) -> str:
        arg = f", argument={self.argument!r}" if self.argument is not None else ""
        fname = f", filename={self.filename!r}" if self.filename is not None else ""
        return (
            f"RheaFileParam(name={self.name!r}, type={self.type!r}{arg}, "
            f"format={self.format!r}, value={self.value!r}{fname})"
        )

    __repr__ = __str__

    @classmethod
    def from_param(cls, param: Param, value: RedisKey) -> "RheaFileParam":
        if param.name is None or param.type is None or param.format is None:
            raise ValueError("Required fields are 'None'")
        return cls(name=param.name, type=param.type, format=param.format, value=value)

    def to_galaxy(self) -> GalaxyFileVar:
        if self.path is None:
            raise ValueError("Path was not initialized in 'RheaFileParam")

        return GalaxyFileVar(path=self.path, filename=self.filename)


class RheaBooleanParam(RheaParam):
    def __init__(
        self,
        name: str,
        type: str,
        truevalue: str,
        falsevalue: str,
        value: bool | None = None,
        checked: bool | None = None,
        argument: str | None = None,
    ) -> None:
        super().__init__(name, type, argument)
        self.truevalue = truevalue
        self.falsevalue = falsevalue
        self.value = value
        self.checked = checked

    def __str__(self) -> str:
        arg = f", argument={self.argument!r}" if self.argument is not None else ""
        val = f", value={self.value!r}" if self.value is not None else ""
        chk = f", checked={self.checked!r}" if self.checked is not None else ""
        return (
            f"RheaBooleanParam(name={self.name!r}, type={self.type!r}"
            f"{arg}, truevalue={self.truevalue!r}, falsevalue={self.falsevalue!r}"
            f"{val}{chk})"
        )

    __repr__ = __str__

    @classmethod
    def from_param(cls, param: Param, value: bool) -> RheaBooleanParam:
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        if param.value is None and param.checked is None:
            raise ValueError("Either 'value' or 'checked' must not be 'None'")
        if param.truevalue is None:
            param.truevalue = "true"
        if param.falsevalue is None:
            param.falsevalue = "false"
        return cls(
            name=param.name,
            type=param.type,
            truevalue=param.truevalue,
            falsevalue=param.falsevalue,
            checked=value,
            value=value,
        )


class RheaTextParam(RheaParam):
    def __init__(
        self, name: str, type: str, value: str, argument: str | None = None
    ) -> None:
        super().__init__(name, type, argument)
        self.value = value

    def __str__(self) -> str:
        arg = f", argument={self.argument!r}" if self.argument is not None else ""
        return f"RheaTextParam(name={self.name!r}, type={self.type!r}{arg}, value={self.value!r})"

    __repr__ = __str__

    @classmethod
    def from_param(cls, param: Param, value: str) -> RheaTextParam:
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        if param.value is None and param.optional:
            return cls(name=param.name, type=param.type, value="")
        return cls(name=param.name, type=param.type, value=value)


class RheaIntegerParam(RheaParam):
    def __init__(
        self,
        name: str,
        type: str,
        value: int,
        min: int | None = None,
        max: int | None = None,
        argument: str | None = None,
    ) -> None:
        super().__init__(name, type, argument)
        self.value = value
        self.min = min
        self.max = max

    def __str__(self) -> str:
        arg = f", argument={self.argument!r}" if self.argument is not None else ""
        min_str = f", min={self.min!r}" if self.min is not None else ""
        max_str = f", max={self.max!r}" if self.max is not None else ""
        return (
            f"RheaIntegerParam(name={self.name!r}, type={self.type!r}"
            f"{arg}, value={self.value!r}{min_str}{max_str})"
        )

    __repr__ = __str__

    @classmethod
    def from_param(
        cls, param: Param, value: int, min: int | None = None, max: int | None = None
    ) -> RheaIntegerParam:
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        return cls(name=param.name, type=param.type, value=value, min=min, max=max)


class RheaFloatParam(RheaParam):
    def __init__(
        self,
        name: str,
        type: str,
        value: float,
        min: float | None = None,
        max: float | None = None,
        argument: str | None = None,
    ) -> None:
        super().__init__(name, type, argument)
        self.value = value
        self.min = min
        self.max = max

    def __str__(self) -> str:
        arg = f", argument={self.argument!r}" if self.argument is not None else ""
        min_str = f", min={self.min!r}" if self.min is not None else ""
        max_str = f", max={self.max!r}" if self.max is not None else ""
        return (
            f"RheaFloatParam(name={self.name!r}, type={self.type!r}"
            f"{arg}, value={self.value!r}{min_str}{max_str})"
        )

    __repr__ = __str__

    @classmethod
    def from_param(
        cls,
        param: Param,
        value: float,
        min: float | None = None,
        max: float | None = None,
    ) -> RheaFloatParam:
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        return cls(name=param.name, type=param.type, value=value, min=min, max=max)


class RheaSelectParam(RheaParam):
    def __init__(
        self, name: str, type: str, value: str, argument: str | None = None
    ) -> None:
        super().__init__(name, type, argument)
        self.value = value

    def __str__(self) -> str:
        arg = f", argument={self.argument!r}" if self.argument is not None else ""
        return f"RheaSelectParam(name={self.name!r}, type={self.type!r}{arg}, value={self.value!r})"

    __repr__ = __str__

    @classmethod
    def from_param(cls, param: Param, value: str) -> RheaSelectParam:
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        if param.options is None:
            raise ValueError("Param has no options.")
        for option in param.options:
            if option.value == value:
                return cls(name=param.name, type=param.type, value=option.value)
        for option in param.options:
            if option.selected:
                return cls(name=param.name, type=param.type, value=option.value)
        if param.optional:
            return cls(name=param.name, type=param.type, value="")
        raise ValueError(f"Value {value} not in select options.")


class RheaMultiSelectParam(RheaParam):
    def __init__(
        self,
        name: str,
        type: str,
        values: List[RheaSelectParam],
        argument: str | None = None,
    ) -> None:
        super().__init__(name, type, argument)
        self.values = values

    def __str__(self) -> str:
        arg = f", argument={self.argument!r}" if self.argument is not None else ""
        vals = "[" + ", ".join(repr(v) for v in self.values) + "]"
        return f"RheaMultiSelectParam(name={self.name!r}, type={self.type!r}{arg}, values={vals})"

    __repr__ = __str__

    @classmethod
    def from_param(cls, param: Param, value: List[str]) -> RheaMultiSelectParam:
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        res = []
        for val in value:
            res.append(RheaSelectParam.from_param(param, val))
        return cls(name=param.name, type=param.type, values=res)


@dataclass
class RheaDataOutput:
    key: RedisKey
    size: int
    filename: str
    name: Optional[str] = None
    format: Optional[str] = None

    def __str__(self) -> str:
        name_part = f", name={self.name!r}" if self.name is not None else ""
        fmt_part = f", format={self.format!r}" if self.format is not None else ""
        return (
            f"RheaDataOutput(key={self.key!r}, size={self.size}, "
            f"filename={self.filename!r}{name_part}{fmt_part})"
        )

    __repr__ = __str__

    @classmethod
    def from_file(
        cls,
        filepath: str,
        store: Store[RedisConnector],
        name: Optional[str] = None,
        format: Optional[str] = None,
    ) -> RheaDataOutput:
        proxy: RheaFileProxy = RheaFileProxy.from_file(
            filepath, r=store.connector._redis_client
        )

        if name is not None:
            proxy.name = name
        if format is not None:
            proxy.format = format

        key = proxy.to_proxy(store)

        return cls(
            key=RedisKey(redis_key=key),
            size=proxy.filesize,
            filename=proxy.filename,
            name=proxy.name,
            format=proxy.format,
        )


class RheaOutput:
    def __init__(self, return_code: int, stdout: str, stderr: str) -> None:
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr

    return_code: int
    stdout: str
    stderr: str
    files: Optional[List[RheaDataOutput]] = None

    def __str__(self) -> str:
        files_part = f", files={self.files!r}" if self.files is not None else ""
        return (
            f"RheaOutput(return_code={self.return_code}, "
            f"stdout={self.stdout!r}, stderr={self.stderr!r}{files_part})"
        )

    __repr__ = __str__


class RheaCollectionOuput(RheaOutput):
    def __init__(
        self,
        return_code: int,
        stdout: str,
        stderr: str,
        collections: List[CollectionOutput],
    ) -> None:
        super().__init__(return_code, stdout, stderr)
        self.collections = collections

    def __str__(self) -> str:
        files_part = (
            f", files={self.files!r}"
            if getattr(self, "files", None) is not None
            else ""
        )
        collections_repr = "[" + ", ".join(repr(c) for c in self.collections) + "]"
        return (
            f"RheaCollectionOuput(return_code={self.return_code}, "
            f"stdout={self.stdout!r}, stderr={self.stderr!r}"
            f"{files_part}, collections={collections_repr})"
        )

    __repr__ = __str__

    def resolve(self, output_dir: str, store: Store[RedisConnector]) -> None:
        for collection in self.collections:
            if collection.type == "list":
                if collection.discover_datasets is None:
                    raise ValueError("Discover datasets is None")
                if collection.discover_datasets.pattern is not None:  # Regex method
                    rgx = re.compile(
                        collection.discover_datasets.pattern.replace("\\\\", "\\")
                    )
                    search_path = output_dir
                    if collection.discover_datasets.directory is not None:
                        search_path = os.path.join(
                            output_dir, collection.discover_datasets.directory
                        )
                    listing = glob.glob(
                        f"{search_path}/*",
                        recursive=(
                            collection.discover_datasets.recurse
                            if collection.discover_datasets.recurse is not None
                            else False
                        ),
                    )
                    for file in listing:
                        if rgx.match(file):
                            if self.files is None:
                                self.files = []
                            name_match = rgx.match(os.path.basename(file))
                            if name_match is not None:
                                name = name_match.group(1)
                            else:
                                name = None
                            self.files.append(
                                RheaDataOutput.from_file(file, store, name=name)
                            )
                else:
                    raise NotImplementedError(
                        f"Discover dataset method not implemented."
                    )
            else:
                raise NotImplementedError(
                    f"CollectionOutput type of {collection.type} not implemented."
                )
