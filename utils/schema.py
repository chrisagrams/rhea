from __future__ import annotations
from typing import List, Optional, Union, Dict
from functools import wraps
from pydantic import BaseModel
import xml.etree.ElementTree as ET
from pathlib import Path

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
            children=[cls.from_element(child) for child in el]
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
            tokens.append(Token(name=token_el.get("name") or "", value=token_el.text or ""))
        
        xml_els = root.findall("xml")
        expands = []
        for xml in xml_els:
            expands.append(MacroExpand(name=xml.get("name") or "", expand=XmlNode.from_element(xml)))

        return cls(
            tokens=tokens,
            expands=expands
        )
    def apply_to_tool(self, tool_xml: ET.Element) -> ET.Element:
        '''
        Apply the macros and expands to an XML.
        As the exapands might change the layout of the XML, do this before making the XML a Tool object.
        '''
        # tool_str = ET.tostring(tool_xml, encoding='utf8')

        # if self.tokens is not None:
        #     for token in self.tokens:
        #         tool_str = tool_str.replace(token.name, token.value)
        
        # xml = ET.ElementTree(ET.fromstring(tool_str))
        # if xml is not None:
        #     root = xml.getroot()
        #     if root is not None:
        #         return root
            
        # raise Exception("Error applying macros.")
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

    regex: str


class DiscoverDatasets(BaseModel):
    pattern: str
    ext: str
    visible: bool
    assign_primary_output: Optional[bool] = None


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
    optional: Optional[bool] = None
    value: Optional[str] = None
    truevalue: Optional[str] = None
    falsevalue: Optional[str] = None
    checked: Optional[bool] = None
    options: Optional[List[Option]] = None


class When(BaseModel):
    value: str
    params: List[Param]


class Conditional(BaseModel):
    name: str
    param: Param
    whens: List[When]


class Inputs(BaseModel):
    params: List[Param]
    conditionals: Optional[List[Conditional]] = None


class AssertContents(BaseModel):
    has_text: Optional[List[str]] = None
    not_has_text: Optional[List[str]] = None


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


class Test(BaseModel):
    expect_num_outputs: int
    params: Optional[List[Param]] = None
    conditional: Optional[Conditional] = None
    outputs: Optional[List[TestOutput]] = None
    assert_command: Optional[AssertCommand] = None


class Tests(BaseModel):
    tests: List[Test]


class Tool(BaseModel):
    id: str
    name: str
    version: str
    profile: str
    description: str
    macros: Macros
    xrefs: Xrefs
    requirements: Requirements
    stdio: Stdio
    version_command: str
    command: str
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
            command = cmd_el.text or ""
        else:
            command = ""

        # Inputs
        inputs_el = root.find("inputs")

        def parse_param(el: ET.Element) -> Param:
            opts = [
                Option(
                    value=o.get("value") or "",
                    selected=(o.get("selected") == True),
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
                optional=(el.get("optional") == "True"),
                value=el.get("value"),
                truevalue=el.get("truevalue"),
                falsevalue=el.get("falsevalue"),
                checked=(el.get("checked") == "True"),
                options=opts,
            )

        if inputs_el is not None:
            params = [parse_param(p) for p in inputs_el.findall("param")]
            conditional_els = inputs_el.findall("conditional")
        else:
            params = []
            conditional_els = []

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

        inputs = Inputs(params=params, conditionals=conditionals or None)

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
                data.append(
                    DataOutput(
                        name=del_.get("name") or "",
                        format=del_.get("format") or "",
                        label=del_.get("label") or "",
                        change_format=cf,
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
                collection.append(
                    CollectionOutput(
                        name=cel.get("name") or "",
                        type=cel.get("type") or "",
                        label=cel.get("label") or "",
                        data=collection_data,
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
                touts: list[TestOutput] = []
                for oel in tel.findall("output"):
                    # Direct assert_contents
                    ac_el = oel.find("assert_contents")
                    ac = None
                    if ac_el is not None:
                        has_ = [h.get("text") or "" for h in ac_el.findall("has_text")]
                        not_ = [
                            n.get("text") or "" for n in ac_el.findall("not_has_text")
                        ]
                        ac = AssertContents(
                            has_text=has_ or None, not_has_text=not_ or None
                        )

                    # Discovered_dataset nested inside <output>
                    ds_el = oel.find("discovered_dataset")
                    ds = None
                    if ds_el is not None:
                        dd_ac_el = ds_el.find("assert_contents")
                        dd_ac = None
                        if dd_ac_el is not None:
                            has_dd = [
                                h.get("text") or ""
                                for h in dd_ac_el.findall("has_text")
                            ]
                            not_dd = [
                                n.get("text") or ""
                                for n in dd_ac_el.findall("not_has_text")
                            ]
                            dd_ac = AssertContents(
                                has_text=has_dd or None, not_has_text=not_dd or None
                            )
                        ds = DiscoveredDataset(
                            designation=ds_el.get("designation") or "",
                            ftype=ds_el.get("ftype") or "",
                            assert_contents=dd_ac,
                        )

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

                test_list.append(
                    Test(
                        expect_num_outputs=expect,
                        params=tparams,
                        conditional=tcond,
                        outputs=touts or None,
                        assert_command=acmd,
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
            name=name,
            version=version,
            profile=profile,
            description=description,
            macros=macros,
            xrefs=xrefs,
            requirements=requirements,
            stdio=stdio,
            version_command=version_command,
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
