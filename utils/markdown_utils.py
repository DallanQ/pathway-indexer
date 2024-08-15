import re


def unstructured_elements_to_markdown(elements):  # noqa: C901
    texts = []
    for element in elements:
        if element["type"] in ["NarrativeText", "UncategorizedText", "Footer"]:
            text = element["text"].strip()
            if element["type"] == "Footer":
                text = re.sub(r"\d", "", text)
            if text:
                text = _clean(_fix_case(text))
                texts.append(f"\n{text}\n")
        elif element["type"] in ["Title", "Header"]:
            text = element["text"].strip()
            if element["type"] == "Header":
                text = re.sub(r"\d", "", text)
            if text:
                text = _clean(text.title())
                texts.append(f"\n## {text}\n")
        elif element["type"] == "ListItem":
            text = element["text"].strip()
            if text:
                text = _clean(_fix_case(text))
                # if text doesn't start with a number, add a bullet point
                if not text[0].isdigit():
                    text = f"- {text}"
                texts.append(f"\n{text}\n")
    return "".join(texts)


def _clean(text):
    """Convert fancy quotes to normal quotes."""
    text = text.replace("“", '"').replace("”", '"')
    text = text.replace("‘", "'").replace("’", "'")  # noqa: RUF001
    return text


def _fix_case(text):
    """Convert uppercase letters preceded by a lowercase letter to lowercase letters"""
    return re.sub(r"([a-z])([A-Z])", lambda m: m.group(1) + m.group(2).lower(), text)
