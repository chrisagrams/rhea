from __future__ import annotations
from typing import ClassVar, List, Optional, Sequence, Union, Dict, Annotated, Any
from functools import wraps
from pydantic import BaseModel, Field
from inspect import Parameter
import xml.etree.ElementTree as ET
from pathlib import Path
import re


class XmlNode(BaseModel):
    tag: str
    attrib: Dict[str, str]
    text: Optional[str] = None
    children: List[XmlNode] = []

    @classmethod
    def from_element(cls, el) -> XmlNode:
        return cls(
            tag=el.tag,
            attrib=el.attrib,
            text=el.text.strip() if el.text and el.text.strip() else None,
            children=[cls.from_element(child) for child in el],
        )

    def to_element(self) -> ET.Element:
        el = ET.Element(self.tag, self.attrib or {})
        if self.text:
            el.text = self.text
        for child in self.children:
            el.append(child.to_element())
        return el


class Token(BaseModel):
    name: str
    value: str


class MacroExpand(BaseModel):
    name: str
    expand: XmlNode


class Macros(BaseModel):
    tokens: Optional[List[Token]] = None
    expands: Optional[List[MacroExpand]] = None

    @classmethod
    def from_xml(cls, xml_input: Union[str, Path, ET.Element]) -> Macros:
        if isinstance(xml_input, (str, Path)):
            tree = ET.parse(xml_input)
            root = tree.getroot()
        elif isinstance(xml_input, ET.Element):
            root = xml_input
        else:
            raise TypeError(
                "from_xml expects a file path or an xml.etree.ElementTree.Element"
            )

        token_els = root.findall("token")
        tokens = []
        for token_el in token_els:
            tokens.append(
                Token(name=token_el.get("name") or "", value=token_el.text or "")
            )

        xml_els = root.findall("xml")
        expands = []
        for xml in xml_els:
            expands.append(
                MacroExpand(
                    name=xml.get("name") or "", expand=XmlNode.from_element(xml)
                )
            )

        return cls(tokens=tokens, expands=expands)

    def apply_to_tool(self, tool_xml: ET.Element) -> ET.Element:
        """
        Apply the macros and expands to an XML.
        As the exapands might change the layout of the XML, do this before making the XML a Tool object.
        """
        for placeholder in tool_xml.findall(".//expand"):
            name = placeholder.get("macro")
            match = next((e for e in self.expands or [] if e.name == name), None)
            if not match:
                continue

            wrapper = match.expand.to_element()
            children = list(wrapper)

            for parent in tool_xml.iter():
                kids = list(parent)
                if placeholder in kids:
                    idx = kids.index(placeholder)
                    for offset, child in enumerate(children):
                        parent.insert(idx + offset, child)
                    parent.remove(placeholder)
                    break

        def _rep(s: Optional[str]) -> Optional[str]:
            if not s:
                return s
            for tok in self.tokens or []:
                s = s.replace(tok.name, tok.value)
            return s

        for el in tool_xml.iter():
            el.text = _rep(el.text)
            el.tail = _rep(el.tail)
            for attr, val in el.attrib.items():
                rep_val = _rep(val)
                if rep_val is not None:
                    el.attrib[attr] = rep_val

        return tool_xml


class ConfigFile(BaseModel):
    name: str
    text: str


class ConfigFiles(BaseModel):
    configfiles: Optional[List[ConfigFile]] = None


class Xref(BaseModel):
    type: str
    value: str


class Xrefs(BaseModel):
    xrefs: List[Xref]


class Requirement(BaseModel):
    type: str
    version: str
    value: str


class Requirements(BaseModel):
    requirements: List[Requirement]


class Regex(BaseModel):
    match: str
    source: str
    level: str
    description: str


class Stdio(BaseModel):
    regex: List[Regex]


class ChangeFormatWhen(BaseModel):
    input: str
    value: str
    format: str


class ChangeFormat(BaseModel):
    whens: List[ChangeFormatWhen]


class OutputFilter(BaseModel):
    """Represents a <filter> under data or collection"""

    code: str


class DiscoverDatasets(BaseModel):
    assign_primary_output: Optional[bool] = None
    from_provided_metadata: Optional[bool] = None
    pattern: Optional[str] = None
    directory: Optional[str] = None
    recurse: Optional[bool] = None
    match_relative_path: Optional[bool] = None
    format: Optional[str] = None
    ext: Optional[str] = None
    sort_by: Optional[str] = None
    visible: Optional[bool] = None


class ActionOption(BaseModel):
    value: str
    text: Optional[str] = None


class OutputAction(BaseModel):
    """Represents an <action> inside an <actions> block."""

    name: Optional[str] = None
    options: Optional[List[ActionOption]] = None


class OutputConditionalAction(BaseModel):
    """Represents a <conditional> inside an <actions> block."""

    value: str
    actions: List[OutputAction]


class DataActions(BaseModel):
    conditionals: List[OutputConditionalAction]


class DataOutput(BaseModel):
    name: str
    format: str
    label: str
    from_work_dir: Optional[str] = None
    change_format: Optional[ChangeFormat] = None
    filters: Optional[List[OutputFilter]] = None
    discover_datasets: Optional[DiscoverDatasets] = None
    actions: Optional[DataActions] = None


class CollectionData(BaseModel):
    name: str
    format: str
    label: str


class CollectionOutput(BaseModel):
    name: str
    type: str
    label: str
    data: List[CollectionData]
    discover_datasets: Optional[DiscoverDatasets] = None


class Outputs(BaseModel):
    data: Optional[List[DataOutput]] = None
    collection: Optional[List[CollectionOutput]] = None


class Option(BaseModel):
    value: str
    selected: Optional[bool] = None
    text: Optional[str] = None


class Param(BaseModel):
    argument: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    format: Optional[str] = None
    label: Optional[str] = None
    help: Optional[str] = None
    multiple: Optional[bool] = None
    optional: Optional[bool] = None
    value: Optional[str] = None
    truevalue: Optional[str] = None
    falsevalue: Optional[str] = None
    checked: Optional[bool] = None
    options: Optional[List[Option]] = None
    description: Optional[str] = None  # An LLM-generated description of the Parameter

    def to_python_parameter(self) -> Parameter:
        if (self.name is None or self.name == "") and self.argument is not None:
            self.name = self.argument.replace("--", "")
        if self.name is None:
            raise ValueError("Both parameter name and argument is None.")
        if self.name.startswith("-"):  # Python parameters can't start with a hyphen
            self.name = self.name.lstrip(
                "-"
            )  # TODO: Make sure this doesn't break downstream

        if self.type == "boolean":
            annotation = Optional[bool] if self.optional else bool
        # elif self.type == "select" and self.multiple:
        #     annotation = Optional[List[str]] if self.optional else List[str]
        elif self.type == "integer":
            annotation = Optional[int] if self.optional else int
        elif self.type == "float":
            annotation = Optional[float] if self.optional else float
        else:
            annotation = Optional[str] if self.optional else str

        default = None if self.optional else Parameter.empty

        if self.value is not None:
            if annotation in (bool, Optional[bool]):
                default = self.value.lower() == "true"
            elif annotation in (int, Optional[int]):
                if (self.value is None or self.value == "") and self.optional:
                    default = None
                elif (self.value is None or self.value == "") and not self.optional:
                    raise ValueError(f"Value is None and non-optional.")
                else:
                    default = int(self.value)
            elif annotation in (float, Optional[float]):
                if (self.value is None or self.value == "") and self.optional:
                    default = None
                elif (self.value is None or self.value == "") and not self.optional:
                    raise ValueError(f"Value is None and non-optional.")
                else:
                    default = float(self.value)
            else:
                default = self.value
        else:
            if self.type == "select" and self.options is not None:
                for option in self.options:
                    if option.selected:
                        default = option.value

        return Parameter(
            name=self.name,
            kind=Parameter.POSITIONAL_OR_KEYWORD,
            default=default,
            annotation=Annotated[annotation, Field(description=self.description)],
        )


class When(BaseModel):
    value: str
    params: List[Param]


class Conditional(BaseModel):
    name: str
    param: Param
    whens: List[When]

    def to_python_parameter(self) -> List[Parameter]:
        result: List[Parameter] = []
        self.param.optional = True  # Conditional params are optional in some contexts yet not specified as optional (ex. 10x_bamtofastq)
        result.append(self.param.to_python_parameter())

        for when in self.whens:
            for param in when.params:
                param.optional = True
                result.append(param.to_python_parameter())

        return result


class Section(BaseModel):
    name: str
    title: str
    expanded: Optional[bool] = None
    help: Optional[str] = None
    params: List[Param]


class Inputs(BaseModel):
    params: List[Param]
    conditionals: Optional[List[Conditional]] = None
    sections: Optional[List[Section]] = None


class AssertContents(BaseModel):
    has_line: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_line_matching: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_n_lines: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_text: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_text_matching: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    not_has_text: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_n_columns: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    attribute_is: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    attribute_matches: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    element_text: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    element_text_is: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    element_text_matches: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_element_with_path: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_n_elements_with_path: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    is_valid_xml: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    xml_element: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_json_property_with_text: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_json_property_with_value: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_h5_attribute: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_h5_keys: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_archive_member: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_size: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_image_center_of_mass: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_image_channels: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_image_depth: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_image_frames: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_image_height: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_image_mean_intensity: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_image_mean_object_size: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_image_n_labels: Optional[Sequence[Union[str, Dict[str, str]]]] = None
    has_image_width: Optional[Sequence[Union[str, Dict[str, str]]]] = None

    _xml_attrs: ClassVar[Dict[str, Union[str, List[str]]]] = {
        "has_line": ["line", "n", "delta", "min", "max", "negate"],
        "has_line_matching": ["expression", "n", "delta", "min", "max", "negate"],
        "has_n_lines": ["n", "delta", "min", "max", "negate"],
        "has_text": ["text", "n", "delta", "min", "max", "negate"],
        "has_text_matching": ["expression", "n", "delta", "min", "max", "negate"],
        "not_has_text": ["text"],
        "has_n_columns": ["n", "delta", "min", "max", "sep", "comment", "negate"],
        "attribute_is": ["path", "attribute", "text", "negate"],
        "attribute_matches": ["path", "attribute", "expression", "negate"],
        "element_text": ["path", "negate"],
        "element_text_is": ["path", "text", "negate"],
        "element_text_matches": ["path", "expression", "negate"],
        "has_element_with_path": ["path", "negate"],
        "has_n_elements_with_path": ["path", "n", "delta", "min", "max", "negate"],
        "is_valid_xml": [],  # presence-only
        "xml_element": [
            "path",
            "attribute",
            "all",
            "n",
            "delta",
            "min",
            "max",
            "negate",
        ],
        "has_json_property_with_text": ["property", "text"],
        "has_json_property_with_value": ["property", "value"],
        "has_h5_attribute": ["key", "value"],
        "has_h5_keys": ["keys"],
        "has_archive_member": ["path", "all", "n", "delta", "min", "max", "negate"],
        "has_size": ["value", "size", "delta", "min", "max", "negate"],
        "has_image_center_of_mass": [
            "center_of_mass",
            "channel",
            "slice",
            "frame",
            "eps",
        ],
        "has_image_channels": ["channels", "delta", "min", "max", "negate"],
        "has_image_depth": ["depth", "delta", "min", "max", "negate"],
        "has_image_frames": ["frames", "delta", "min", "max", "negate"],
        "has_image_height": ["height", "delta", "min", "max", "negate"],
        "has_image_mean_intensity": ["mean_intensity", "eps", "min", "max"],
        "has_image_mean_object_size": [
            "mean_object_size",
            "labels",
            "exclude_labels",
            "eps",
            "min",
            "max",
        ],
        "has_image_n_labels": [
            "labels",
            "exclude_labels",
            "n",
            "delta",
            "min",
            "max",
            "negate",
        ],
        "has_image_width": ["width", "delta", "min", "max", "negate"],
    }

    @classmethod
    def xml_attrs_for(cls, field: str) -> Union[str, List[str]]:
        return cls._xml_attrs.get(field, [])

    def run_all(self, input: bytes) -> None:
        data = self.dict()
        for field, entries in data.items():
            if not entries:
                continue

            handler_name = f"_assert_{field}"
            handler = getattr(self, handler_name, None)
            if handler is None:
                raise NotImplementedError(f"No handler implemented for '{field}'")

            for entry in entries:
                if isinstance(entry, dict):
                    params = entry
                else:
                    attrs = self.xml_attrs_for(field)
                    params = {attrs[0]: entry} if attrs else {}
                handler(input, **params)

    def _assert_has_text(self, input: bytes, text: str, **kwargs):
        subject = input.decode(encoding="utf-8")
        if text not in subject:
            raise AssertionError(f"Expected to find '{text}' in subject")

    def _assert_has_text_matching(
        self,
        input: bytes,
        expression: str,
        n: int = 1,
        delta: int = 0,
        min: int | None = None,
        max: int | None = None,
        negate: bool = False,
        **kwargs,
    ):
        subject = input.decode("utf-8")
        pattern = re.compile(expression)

        count = sum(1 for _ in pattern.finditer(subject))

        expected = int(n) if n is not None and n != "" else 1

        d = int(delta) if delta not in (None, "") else 0
        lower = expected - d
        upper = expected + d

        if min not in (None, ""):
            lower = int(min)
        if max not in (None, ""):
            upper = int(max)

        if negate:
            if lower <= count <= upper:
                raise AssertionError(
                    f"Expected number of matching occurrences NOT in [{lower}, {upper}], but got {count}"
                )
        else:
            if count < lower or count > upper:
                raise AssertionError(
                    f"Expected number of matching occurrences in [{lower}, {upper}], but got {count}"
                )

    def _assert_has_n_lines(
        self,
        input: bytes,
        n: int,
        delta: int = 0,
        min: int | None = None,
        max: int | None = None,
        negate: bool = False,
        **kwargs,
    ):
        subject = input.decode("utf-8")
        count = subject.count("\n") + 1

        expected = int(n) if n is not None and n != "" else 1

        d = int(delta) if delta is not None and delta != "" else 0
        lower = expected - d
        upper = expected + d
        if min is not None and min != "":
            lower = int(min)
        if max is not None and max != "":
            upper = int(max)
        if negate:
            if lower <= count <= upper:
                raise AssertionError(
                    f"Expected line count not in [{lower}, {upper}], got {count}"
                )
        else:
            if count < lower or count > upper:
                raise AssertionError(
                    f"Expected line count in [{lower}, {upper}], got {count}"
                )

    def _assert_has_line_matching(
        self,
        input: bytes,
        expression: str,
        n: int = 1,
        delta: int = 0,
        min: int | None = None,
        max: int | None = None,
        negate: bool = False,
        **kwargs,
    ):
        lines = input.decode("utf-8").splitlines()
        pattern = re.compile(expression)

        count = sum(1 for line in lines if pattern.search(line))

        expected = int(n) if n is not None and n != "" else 1

        d = int(delta) if delta not in (None, "") else 0
        lower = expected - d
        upper = expected + d
        if min not in (None, ""):
            lower = int(min)
        if max not in (None, ""):
            upper = int(max)

        if negate:
            if lower <= count <= upper:
                raise AssertionError(
                    f"Expected number of matching lines NOT in [{lower}, {upper}], but got {count}"
                )
        else:
            if count < lower or count > upper:
                raise AssertionError(
                    f"Expected number of matching lines in [{lower}, {upper}], but got {count}"
                )

    def _assert_is_valid_xml(self, input: bytes, **kwargs):
        try:
            ET.fromstring(input.decode("utf-8"))
        except ET.ParseError as e:
            raise AssertionError(f"Invalid XML: {e}")


class DiscoveredDataset(BaseModel):
    designation: str
    ftype: str
    assert_contents: Optional[AssertContents] = None


class TestOutput(BaseModel):
    name: Optional[str] = None
    file: Optional[str] = None
    ftype: Optional[str] = None
    value: Optional[str] = None
    assert_contents: Optional[AssertContents] = None
    discovered_dataset: Optional[DiscoveredDataset] = None
    metadata: Optional[str] = None


class AssertCommand(BaseModel):
    has_text: Optional[List[str]] = None
    not_has_text: Optional[List[str]] = None


class OutputCollectionElement(BaseModel):
    name: Optional[str] = None
    file: Optional[str] = None
    element: Optional[OutputCollectionElement] = None
    assert_contents: Optional[AssertContents] = None


class OutputCollection(BaseModel):
    name: str
    type: Optional[str] = None
    count: Optional[int] = None
    elements: Optional[List[OutputCollectionElement]] = None


class Test(BaseModel):
    expect_num_outputs: int
    params: Optional[List[Param]] = None
    conditional: Optional[Conditional] = None
    outputs: Optional[List[TestOutput]] = None
    assert_command: Optional[AssertCommand] = None
    output_collection: Optional[OutputCollection] = None


class Tests(BaseModel):
    tests: List[Test]


class Command(BaseModel):
    command: str
    interpreter: str | None = None


class Tool(BaseModel):
    id: str
    name: str | None = None
    user_provided_name: str
    version: str
    profile: str
    description: str
    long_description: Optional[str] = None
    macros: Macros
    xrefs: Xrefs
    requirements: Requirements
    stdio: Stdio
    version_command: str
    command: Command
    configfiles: Optional[ConfigFiles] = None
    inputs: Inputs
    outputs: Outputs
    tests: Tests
    help: Optional[str] = None
    citations: Optional[List[str]] = None
    documentation: Optional[str] = None

    @classmethod
    def from_xml(cls, xml_input: Union[str, Path, ET.Element]) -> Tool:
        if isinstance(xml_input, (str, Path)):
            tree = ET.parse(xml_input)
            root = tree.getroot()
        elif isinstance(xml_input, ET.Element):
            root = xml_input
        else:
            raise TypeError(
                "from_xml expects a file path or an xml.etree.ElementTree.Element"
            )

        tool_id = root.get("id") or ""
        name = root.get("name") or ""
        version = root.get("version") or ""
        profile = root.get("profile") or ""

        # Description
        if description_el := root.find("description"):
            description = description_el.text or ""
        else:
            description = ""

        # Macros
        macros_el = root.find("macros")
        if macros_el is not None:
            tokens = [
                Token(name=tok.get("name") or "", value=tok.text or "")
                for tok in macros_el.findall("token")
            ]
            macros = Macros(tokens=tokens)
        else:
            macros = Macros(tokens=[])

        if macros.tokens is not None:
            macro_map = {tok.name: tok.value for tok in macros.tokens}
        else:
            macro_map = None

        def expand_str(s: str) -> str | None:
            if macro_map is not None:
                for tok, val in macro_map.items():
                    s = s.replace(tok, val)
                return s
            return None

        # Xrefs
        xrefs_el = root.find("xrefs")
        if xrefs_el is not None:
            xrefs = [
                Xref(type=x.get("type") or "", value=x.text or "")
                for x in xrefs_el.findall("xref")
            ]
            xrefs = Xrefs(xrefs=xrefs)
        else:
            xrefs = Xrefs(xrefs=[])

        # Requirements
        reqs_el = root.find("requirements")
        if reqs_el is not None:
            reqs = [
                Requirement(
                    type=r.get("type") or "",
                    version=r.get("version") or "",
                    value=r.text or "",
                )
                for r in reqs_el.findall("requirement")
            ]
            requirements = Requirements(requirements=reqs)
        else:
            requirements = Requirements(requirements=[])

        # stdio
        stdio_el = root.find("stdio")
        if stdio_el is not None:
            regs = [
                Regex(
                    match=r.get("match") or "",
                    source=r.get("source") or "",
                    level=r.get("level") or "",
                    description=r.get("description") or "",
                )
                for r in stdio_el.findall("regex")
            ]
            stdio = Stdio(regex=regs)
        else:
            stdio = Stdio(regex=[])

        # Version command
        version_command_el = root.find("version_command")
        if version_command_el is not None:
            version_command = version_command_el.text or ""
        else:
            version_command = ""

        # Command

        cmd_el = root.find("command")
        if cmd_el is not None:
            command_string = cmd_el.text or ""
            interpreter = cmd_el.get("interpreter") or None
            command = Command(command=command_string, interpreter=interpreter)
        else:
            command = Command(command="", interpreter=None)

        # Configfiles
        config_files = None
        cfs_el = root.find("configfiles")
        if cfs_el is not None:
            cf_els = cfs_el.findall("configfile")
            if cf_els is not None:
                configfiles = []
                for cf_el in cf_els:
                    configfiles.append(
                        ConfigFile(name=cf_el.get("name") or "", text=cf_el.text or "")
                    )
                config_files = ConfigFiles(configfiles=configfiles)

        # Inputs
        inputs_el = root.find("inputs")

        def parse_param(el: ET.Element) -> Param:
            opts = [
                Option(
                    value=o.get("value") or "",
                    selected=(
                        o.get("selected") == "True" or o.get("selected") == "true"
                    ),
                    text=o.text,
                )
                for o in el.findall("option")
            ] or None

            return Param(
                argument=el.get("argument"),
                name=el.get("name"),
                type=el.get("type"),
                format=el.get("format"),
                label=el.get("label"),
                help=el.get("help"),
                multiple=(el.get("multiple") == "True" or el.get("multiple") == "true"),
                optional=(el.get("optional") == "True" or el.get("optional") == "true"),
                value=el.get("value"),
                truevalue=el.get("truevalue"),
                falsevalue=el.get("falsevalue"),
                checked=(el.get("checked") == "True" or el.get("checked") == "true"),
                options=opts,
            )

        if inputs_el is not None:
            params = [parse_param(p) for p in inputs_el.findall("param")]
            conditional_els = inputs_el.findall("conditional")
            sections_els = inputs_el.findall("section")
        else:
            params = []
            conditional_els = []
            sections_els = []

        conditionals = []
        for cel in conditional_els:
            # Controlling param
            param_elem = cel.find("param")
            if param_elem is not None:
                control = parse_param(param_elem)
            else:
                control = Param()
            whens = []
            for wel in cel.findall("when"):
                wp = [parse_param(p) for p in wel.findall("param")]
                whens.append(When(value=wel.get("value") or "", params=wp))
            conditionals.append(
                Conditional(name=cel.get("name") or "", param=control, whens=whens)
            )

        sections = []
        for sec_el in sections_els:
            param_elems = sec_el.findall("param")
            section_params = []
            for p in param_elems:
                section_params.append(parse_param(p))
            sec = Section(
                name=sec_el.get("name") or "",
                title=sec_el.get("title") or "",
                expanded=(
                    sec_el.get("expanded") == "True" or sec_el.get("expanded") == "true"
                ),
                help=sec_el.get("help"),
                params=section_params,
            )
            sections.append(sec)

        inputs = Inputs(
            params=params, conditionals=conditionals or None, sections=sections
        )

        # Outputs
        outputs_el = root.find("outputs")
        data = []
        collection = []

        if outputs_el is not None:
            # Parse <data>
            for del_ in outputs_el.findall("data"):
                cf_el = del_.find("change_format")
                cf = None
                if cf_el is not None:
                    whens = [
                        ChangeFormatWhen(
                            input=w.get("input") or "",
                            value=w.get("value") or "",
                            format=w.get("format") or "",
                        )
                        for w in cf_el.findall("when")
                    ]
                    cf = ChangeFormat(whens=whens)

                dd = None
                dd_el = del_.find("discover_datasets")
                if dd_el is not None:
                    dd_el_apo = dd_el.get("assign_primary_output")
                    dd_el_fpm = dd_el.get("from_provided_metadata")
                    dd_el_recurse = dd_el.get("recurse")
                    dd_el_mrp = dd_el.get("match_relative_path")
                    dd_el_visible = dd_el.get("visible")
                    dd = DiscoverDatasets(
                        assign_primary_output=(
                            True
                            if dd_el_apo == "true" or dd_el_apo == "True"
                            else False if dd_el_apo is not None else None
                        ),
                        from_provided_metadata=(
                            True
                            if dd_el_fpm == "true" or dd_el_fpm == "True"
                            else False if dd_el_fpm is not None else None
                        ),
                        pattern=dd_el.get("pattern"),
                        directory=dd_el.get("directory"),
                        recurse=(
                            True
                            if dd_el_recurse == "true" or dd_el_recurse == "True"
                            else False if dd_el_recurse is not None else None
                        ),
                        match_relative_path=(
                            True
                            if dd_el_mrp == "true" or dd_el_mrp == "True"
                            else False if dd_el_mrp is not None else None
                        ),
                        format=dd_el.get("format"),
                        ext=dd_el.get("ext"),
                        sort_by=dd_el.get("sort_by"),
                        visible=(
                            True
                            if dd_el_visible == "true" or dd_el_visible == "True"
                            else False if dd_el_visible is not None else None
                        ),
                    )

                filters = None
                filters_el = del_.findall("filter")
                if filters_el is not None and len(filters_el) > 0:
                    filters = []
                    for filter in filters_el:
                        filters.append(OutputFilter(code=filter.text or ""))

                data.append(
                    DataOutput(
                        name=del_.get("name") or "",
                        format=del_.get("format") or "",
                        label=del_.get("label") or "",
                        from_work_dir=del_.get("from_work_dir") or "",
                        change_format=cf,
                        discover_datasets=dd,
                        filters=filters,
                    )
                )

            # Parse <collection>
            for cel in outputs_el.findall("collection"):
                collection_data = [
                    CollectionData(
                        name=d.get("name") or "",
                        format=d.get("format") or "",
                        label=d.get("label") or "",
                    )
                    for d in cel.findall("data")
                ]
                dd = None
                dd_el = cel.find("discover_datasets")
                if dd_el is not None:
                    dd_el_apo = dd_el.get("assign_primary_output")
                    dd_el_fpm = dd_el.get("from_provided_metadata")
                    dd_el_recurse = dd_el.get("recurse")
                    dd_el_mrp = dd_el.get("match_relative_path")
                    dd_el_visible = dd_el.get("visible")
                    dd = DiscoverDatasets(
                        assign_primary_output=(
                            True
                            if dd_el_apo == "true" or dd_el_apo == "True"
                            else False if dd_el_apo is not None else None
                        ),
                        from_provided_metadata=(
                            True
                            if dd_el_fpm == "true" or dd_el_fpm == "True"
                            else False if dd_el_fpm is not None else None
                        ),
                        pattern=dd_el.get("pattern"),
                        directory=dd_el.get("directory"),
                        recurse=(
                            True
                            if dd_el_recurse == "true" or dd_el_recurse == "True"
                            else False if dd_el_recurse is not None else None
                        ),
                        match_relative_path=(
                            True
                            if dd_el_mrp == "true" or dd_el_mrp == "True"
                            else False if dd_el_mrp is not None else None
                        ),
                        format=dd_el.get("format"),
                        ext=dd_el.get("ext"),
                        sort_by=dd_el.get("sort_by"),
                        visible=(
                            True
                            if dd_el_visible == "true" or dd_el_visible == "True"
                            else False if dd_el_visible is not None else None
                        ),
                    )
                collection.append(
                    CollectionOutput(
                        name=cel.get("name") or "",
                        type=cel.get("type") or "",
                        label=cel.get("label") or "",
                        data=collection_data,
                        discover_datasets=dd,
                    )
                )

        outputs = Outputs(data=data or None, collection=collection or None)

        # Tests
        tests_el = root.find("tests")
        test_list = []
        if tests_el is not None:
            for tel in tests_el.findall("test"):
                expect = int(tel.get("expect_num_outputs") or -1)

                # Test params
                tparams = [
                    Param(name=p.get("name"), value=p.get("value"))
                    for p in tel.findall("param")
                ] or None

                # Conditional in test
                tcond = None
                tcel = tel.find("conditional")
                if tcel is not None:
                    tcel_param = tcel.find("param")
                    if tcel_param is not None:
                        cp = Param(
                            name=tcel_param.get("name"), value=tcel_param.get("value")
                        )
                    else:
                        cp = Param(name="", value="")
                    whs = []
                    for wel in tcel.findall("when"):
                        ps = [
                            Param(name=p.get("name"), value=p.get("value"))
                            for p in wel.findall("param")
                        ]
                        whs.append(When(value=wel.get("value") or "", params=ps))
                    tcond = Conditional(
                        name=tcel.get("name") or "", param=cp, whens=whs
                    )

                # Outputs in test

                def parse_assert_contents(
                    element: Optional[ET.Element],
                ) -> Optional[AssertContents]:
                    if element is None:
                        return None

                    ac_data: Dict[str, Union[List[str], List[Dict[str, str]], None]] = (
                        {}
                    )
                    for name in AssertContents.__fields__:
                        attrs = AssertContents.xml_attrs_for(name)
                        elems = element.findall(name)
                        if not elems:
                            ac_data[name] = None
                            continue

                        if isinstance(attrs, str):
                            vals = [el.get(attrs) or "" for el in elems]
                        else:
                            if not attrs:
                                vals = [{} for _ in elems]
                            else:
                                vals = [
                                    {a: el.get(a) or "" for a in attrs} for el in elems
                                ]

                        ac_data[name] = vals or None

                    return AssertContents(**ac_data)

                touts: list[TestOutput] = []
                for oel in tel.findall("output"):
                    ac = parse_assert_contents(oel.find("assert_contents"))

                    ds_el = oel.find("discovered_dataset")
                    if ds_el is not None:
                        dd_ac = parse_assert_contents(ds_el)
                        ds = DiscoveredDataset(
                            designation=ds_el.get("designation") or "",
                            ftype=ds_el.get("ftype") or "",
                            assert_contents=dd_ac,
                        )
                    else:
                        ds = None

                    # Metadata placeholder
                    md_el = oel.find("metadata")
                    md = md_el.text if md_el is not None else None

                    touts.append(
                        TestOutput(
                            name=oel.get("name"),
                            file=oel.get("file"),
                            ftype=oel.get("ftype"),
                            value=oel.get("value"),
                            assert_contents=ac,
                            discovered_dataset=ds,
                            metadata=md,
                        )
                    )

                # Assert command

                acmd_el = tel.find("assert_command")
                acmd = None
                if acmd_el is not None:
                    has_ = [h.get("text") or "" for h in acmd_el.findall("has_text")]
                    not_ = [
                        n.get("text") or "" for n in acmd_el.findall("not_has_text")
                    ]
                    acmd = AssertCommand(
                        has_text=has_ or None, not_has_text=not_ or None
                    )

                # Output collection in test

                def parse_output_collection_element(
                    el: ET.Element,
                ) -> OutputCollectionElement:
                    oce = OutputCollectionElement(
                        name=el.get("name"), file=el.get("file")
                    )
                    oce_subel = el.find("element")
                    if oce_subel is not None:
                        oce.element = parse_output_collection_element(oce_subel)
                    oce.assert_contents = parse_assert_contents(
                        el.find("assert_contents")
                    )

                    return oce

                oc_el = tel.find("output_collection")
                oc = None
                if oc_el is not None:
                    count = oc_el.get("count")
                    oc = OutputCollection(
                        name=oc_el.get("name") or "",
                        type=oc_el.get("type"),
                        count=int(count) if count is not None else None,
                    )
                    oc_elem_els = oc_el.findall("element")
                    if oc_elem_els is not None:
                        oc.elements = []
                        for oc_elem_el in oc_elem_els:
                            oc.elements.append(
                                parse_output_collection_element(oc_elem_el)
                            )

                test_list.append(
                    Test(
                        expect_num_outputs=expect,
                        params=tparams,
                        conditional=tcond,
                        outputs=touts or None,
                        assert_command=acmd,
                        output_collection=oc,
                    )
                )
        tests = Tests(tests=test_list)

        # Help
        help_el = root.find("help")
        if help_el is not None:
            help_text = help_el.text
        else:
            help_text = None

        # Citations
        cites_el = root.find("citations")
        citations = None
        if cites_el is not None:
            citations = [c.text or "" for c in cites_el.findall("citation")]
        else:
            citations = []

        tool = cls(
            id=tool_id,
            user_provided_name=name,
            version=version,
            profile=profile,
            description=description,
            macros=macros,
            xrefs=xrefs,
            requirements=requirements,
            stdio=stdio,
            version_command=version_command,
            configfiles=config_files,
            command=command,
            inputs=inputs,
            outputs=outputs,
            tests=tests,
            help=help_text,
            citations=citations,
        )

        def expand_all(obj):
            if isinstance(obj, Macros):  # Don't expand the Macro itself
                return obj
            if isinstance(obj, str):
                return expand_str(obj)
            if isinstance(obj, BaseModel):
                for name, value in obj.__dict__.items():
                    setattr(obj, name, expand_all(value))
                return obj
            if isinstance(obj, list):
                return [expand_all(v) for v in obj]
            return obj

        expand_all(tool)
        return tool
