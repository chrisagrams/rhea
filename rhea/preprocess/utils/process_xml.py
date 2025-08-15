import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError


def classify_xml_type(xml_bytes: bytes | str) -> str | None:
    if isinstance(xml_bytes, bytes):
        xml_bytes = xml_bytes.decode("utf-8", errors="ignore")

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None

    tag = root.tag.lower()

    if tag not in ("tool", "macros", "repositories"):
        return None
    return tag
