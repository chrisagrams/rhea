import os
import re
import glob
from typing import Any, List, Optional
from dataclasses import dataclass
from utils.schema import Param, CollectionOutput
from proxystore.connectors.redis import RedisKey, RedisConnector
from proxystore.store import Store
from proxystore.store.utils import get_key


class GalaxyVar(str):
    def __new__(cls, value):
        obj = super().__new__(cls, value)
        obj._nested = {} #type: ignore
        return obj

    def __getattr__(self, name):
        try:
            return self._nested[name]
        except KeyError:
            raise AttributeError(f"{name!r}")

    def __getitem__(self, key):
        return getattr(self, key)

    def set_nested(self, name, value):
        self._nested[name] = value


class RheaParam:
    def __init__(self, name: str, type: str, argument: str | None = None) -> None:
        self.name = name
        self.type = type
        self.argument = argument

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
                return RheaTextParam.from_param(param, '')
            if type(value) is not str:
                raise ValueError("Value must be a 'str' for text param.")
            return RheaTextParam.from_param(param, value)
        elif param.type == "integer": # RheaIntegerParam
            if isinstance(value, str):
                try:
                    value = int(value)
                except ValueError:
                    raise ValueError("Value must be an 'int' or string castable to 'int' for integer param.")
            if not isinstance(value, int):
                raise ValueError("Value must be an 'int' for integer param.")
            return RheaIntegerParam.from_param(param, value)
        elif param.type == "float": # RheaFloatParam
            if isinstance(value, str):
                try:
                    value = float(value)
                except ValueError:
                    raise ValueError("Value must be a 'float' or string castable to 'float' for float param.")
            if not isinstance(value, float):
                raise ValueError("Value must be a 'float' for float param.")
            return RheaFloatParam.from_param(param, value)
        elif param.type == "boolean":
            if type(value) is not bool:
                if value.lower() == "true":
                    value = True
                elif value.lower() == "false":
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
    ) -> None:
        super().__init__(name, type, argument)
        self.format = format
        self.value = value
        self.filename = filename

    @classmethod
    def from_param(cls, param: Param, value: RedisKey) -> "RheaFileParam":
        if param.name is None or param.type is None or param.format is None:
            raise ValueError("Required fields are 'None'")
        return cls(name=param.name, type=param.type, format=param.format, value=value)


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

    @classmethod
    def from_param(cls, param: Param, value: bool) -> "RheaBooleanParam":
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

    @classmethod
    def from_param(cls, param: Param, value: str) -> "RheaTextParam":
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        if param.value is None and param.optional:
            return cls(name=param.name, type=param.type, value='') 
        return cls(name=param.name, type=param.type, value=value)


class RheaIntegerParam(RheaParam):
    def __init__(
            self, name: str, type: str, value: int, min: int | None = None, max: int | None = None, argument: str | None = None,
    ) -> None: 
        super().__init__(name, type, argument)
        self.value = value
        self.min = min
        self.max = max
    
    @classmethod
    def from_param(cls, param: Param, value: int, min: int | None = None, max: int | None = None) -> "RheaIntegerParam":
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        return cls(name=param.name, type=param.type, value=value, min=min, max=max)
    

class RheaFloatParam(RheaParam):
    def __init__(
            self, name: str, type: str, value: float, min: float | None = None, max: float | None = None, argument: str | None = None,
    ) -> None:
        super().__init__(name, type, argument)
        self.value = value
        self.min = min
        self.max = max

    @classmethod 
    def from_param(cls, param: Param, value: float, min: float | None = None, max: float | None = None) -> "RheaFloatParam":
        if param.name is None or param.type is None:
            raise ValueError("Required fields are 'None'")
        return cls(name=param.name, type=param.type, value=value, min=min, max=max)
        

class RheaSelectParam(RheaParam):
    def __init__(
        self, name: str, type: str, value: str, argument: str | None = None
    ) -> None:
        super().__init__(name, type, argument)
        self.value = value

    @classmethod
    def from_param(cls, param: Param, value: str) -> "RheaSelectParam":
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
            return cls(name=param.name, type=param.type, value='')            
        raise ValueError(f"Value {value} not in select options.")


class RheaMultiSelectParam(RheaParam):
    def __init__(
            self, name: str, type: str, values: List[RheaSelectParam], argument: str | None = None
    ) -> None:
        super().__init__(name, type, argument)
        self.values = values
    
    @classmethod
    def from_param(cls, param: Param, value: List[str]) -> "RheaMultiSelectParam":
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

    @classmethod
    def from_file(
        cls, filepath: str, store: Store[RedisConnector], name: Optional[str] = None, format: Optional[str] = None
    ) -> "RheaDataOutput":
        with open(filepath, "rb") as f:
            buffer = f.read()
            proxy = store.proxy(buffer)
            key = get_key(proxy)

        size = os.path.getsize(filepath)
        filename = os.path.basename(filepath)
        return cls(key=key, size=size, filename=filename, name=name, format=format) #type: ignore


class RheaOutput:
    def __init__(self, return_code: int, stdout: str, stderr: str) -> None:
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr

    return_code: int
    stdout: str
    stderr: str
    files: Optional[List[RheaDataOutput]] = None


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

