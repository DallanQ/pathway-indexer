import os
import re
from typing import Any, Optional

from collections.abc import Sequence
from llama_index.core.callbacks.base import CallbackManager
import spacy
from llama_index.core.node_parser.interface import NodeParser
from llama_index.core.bridge.pydantic import Field
from llama_index.core.schema import BaseNode, MetadataMode, TextNode
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.utils import get_tqdm_iterable
from llama_index.core import VectorStoreIndex
from llama_index.vector_stores.pinecone import PineconeVectorStore

INDEX_METADATA_KEYS = [
    "heading",
    "subheading",
]  # title often contains the question exactly and applies to only part of the document, "title"]
MD_METADATA_KEYS = [
    "header_1",
    "header_2",
    "header_3",
    "header_4",
    "header_5",
    "header_6",
]
ORDERED_LIST_ITEM_PATTERN = r"^\s*\d+\.\s"
UNORDERED_LIST_ITEM_PATTERN = r"^\s*[-*+]\s"


class AltNodeParser(NodeParser):
    """Alternate Node Parser"""

    split_by: str = Field(
        default="para",
        description=("Split by sentence, paragraph, or both."),
    )

    embed_prev_next_sentences: int = Field(
        default=0,
        description=(
            "Number of previous and next sentences to include when calculating embeddings."
        ),
    )

    embed_prev_next_paragraphs: int = Field(
        default=0,
        description=(
            "Number of previous and next paragraphs to include when calculating embeddings."
        ),
    )

    max_embed_length: int = Field(
        default=2048,
        description=(
            "Maximum length of text to embed. Used when embedding previous and next sentences or paragraphs."
        ),
    )

    embed_index_headers: bool = Field(
        default=False,
        description=(
            "Include headers from the index page when calculating embeddings."
        ),
    )

    embed_md_headers: bool = Field(
        default=False,
        description=(
            "Include headers from the markdown document when calculating embeddings."
        ),
    )

    include_prev_next_paragraphs: int = Field(
        default=0,
        description=(
            "Number of previous and next paragraphs to include in text for the LLM."
        ),
    )

    max_include_length: int = Field(
        default=2048,
        description=(
            "Maximum length of text to pass to the LLM. Used when including previous and next paragraphs."
        ),
    )

    include_index_headers: bool = Field(
        default=False,
        description=("Include headers from the index page in text for the LLM."),
    )

    include_md_headers: bool = Field(
        default=False,
        description=("Include headers from the markdown document in text for the LLM."),
    )

    @classmethod
    def from_defaults(
        cls,
        split_by: str = "para",
        embed_prev_next_sentences: int = 0,
        embed_prev_next_paragraphs: int = 0,
        max_embed_length: int = 2048,
        embed_index_headers: bool = False,
        embed_md_headers: bool = False,
        include_prev_next_paragraphs: int = 0,
        max_include_length: int = 2048,
        include_index_headers: bool = False,
        include_md_headers: bool = False,
        callback_manager: Optional[CallbackManager] = None,
    ) -> "AltNodeParser":
        callback_manager = callback_manager or CallbackManager([])

        return cls(
            split_by=split_by,
            embed_prev_next_sentences=embed_prev_next_sentences,
            embed_prev_next_paragraphs=embed_prev_next_paragraphs,
            max_embed_length=max_embed_length,
            embed_index_headers=embed_index_headers,
            embed_md_headers=embed_md_headers,
            include_prev_next_paragraphs=include_prev_next_paragraphs,
            max_include_length=max_include_length,
            include_index_headers=include_index_headers,
            include_md_headers=include_md_headers,
        )

    @classmethod
    def class_name(cls) -> str:
        """Get class name."""
        return "AltNodeParser"

    def _parse_nodes(
        self,
        nodes: Sequence[BaseNode],
        show_progress: bool = False,
        **kwargs: Any,
    ) -> list[BaseNode]:
        all_nodes: list[BaseNode] = []
        print("len of all_nodes", len(nodes))
        nodes_with_progress = get_tqdm_iterable(nodes, show_progress, "Parsing nodes")
        print("len of nodes_with_progress", len(nodes_with_progress))
        for node in nodes_with_progress:
            nodes = self.get_nodes_from_node(node, **kwargs)
            all_nodes.extend(nodes)

        return all_nodes

    def get_nodes_from_node(self, node: BaseNode, **kwargs: Any) -> list[TextNode]:
        """Split a node into a list of nodes."""

        # turn node into a list of headers and paragraphs
        headers_paragraphs = get_headers_and_paragraphs(node)
        # get paragraph nodes (with metadata for headers and context text to send to the LLM)
        nodes = get_paragraph_nodes(headers_paragraphs, node)
        # include previous and next paragraphs
        if self.include_prev_next_paragraphs > 0:
            include_prev_next_contexts(
                nodes, self.include_prev_next_paragraphs, self.max_include_length
            )
        # split by sentence, paragraph, or both
        sentence_nodes = []
        if self.split_by in ["sentence", "both"]:
            sentence_nodes = get_sentences(nodes)
            if self.embed_prev_next_sentences > 0:
                sentence_nodes = embed_prev_next(
                    sentence_nodes,
                    self.embed_prev_next_sentences,
                    self.max_embed_length,
                )
        if self.split_by == "sentence":
            nodes = sentence_nodes
        else:  # paragraph or both
            if self.embed_prev_next_paragraphs > 0:
                nodes = embed_prev_next(
                    nodes, self.embed_prev_next_paragraphs, self.max_embed_length
                )
            if self.split_by == "both":
                nodes.extend(sentence_nodes)
        # embed index and/or markdown headers
        if self.embed_index_headers or self.embed_md_headers:
            keys = []
            if self.embed_index_headers:
                keys.extend(INDEX_METADATA_KEYS)
            if self.embed_md_headers:
                keys.extend(MD_METADATA_KEYS)
            embed_metadata(nodes, keys)
        # include index and/or markdown headers in text sent to the LLM
        if self.include_index_headers or self.include_md_headers:
            keys = []
            if self.include_index_headers:
                keys.extend(INDEX_METADATA_KEYS)
            if self.include_md_headers:
                keys.extend(MD_METADATA_KEYS)
            include_metadata(nodes, keys)

        return nodes


def extract_index_metadata(node):
    """Split the document text into headers and content."""

    text = node.get_content(metadata_mode=MetadataMode.NONE)
    parts = text.split("---\\n")
    # If there are no headers, return the document as-is
    if len(parts) < 3:
        return node

    # Extract headers
    headers_text = parts[1]

    # Parse headers into a dictionary
    headers = {}
    current_key = None
    for line in headers_text.strip().split("\n"):
        if ": " in line:
            key, value = line.split(": ", 1)
            headers[key] = value
            current_key = key
        elif current_key is not None:
            # This line is a continuation of the previous value
            headers[current_key] += " " + line.strip()

    # clean up titles
    if "title" in headers:
        title = headers["title"]
        # Split the string using the corrected regex with escaped quotes
        titles = re.split(r"\s*['\"]?\s*,\s*['\"]?\s*", title)

        # Clean up each question by removing leading/trailing quotes and spaces
        titles = [re.sub(r'^[\'"\s]+|[\'"\s]+$', "", t) for t in titles]

        # Join the cleaned questions with the separator
        headers["title"] = " | ".join(titles)

    # Join the remaining parts as content
    # TODO! don't join the last part if its empty - make sure we don't still have unnecessary --- separators
    content = "---\\n".join(parts[2:])

    final_metadata = {**node.metadata, **headers}

    return TextNode(metadata=final_metadata, text=content)


def get_headers_and_paragraphs(node: BaseNode) -> list[str]:
    """Get headers and paragraphs from a node."""

    def is_table_row(line):
        # Check if line is part of a table (has | character and isn't a blockquote)
        return '|' in line and not line.lstrip().startswith('>')

    def is_table_separator(line):
        # Check if line is a table header separator (contains only |, -, and spaces)
        stripped = line.strip()
        return stripped and all(c in '|-: ' for c in stripped)

    # get text from the node
    text = node.get_content(metadata_mode=MetadataMode.NONE)
    results = []
    paragraph_lines = []
    blank_line = False
    in_table = False
    table_buffer = []

    # split text by newline
    for line in text.split("\n"):
        line = line.rstrip()
        
        # Table handling
        if is_table_row(line) or is_table_separator(line):
            if not in_table:
                # Start of a new table
                if paragraph_lines:
                    results.append("\n".join(paragraph_lines))
                    paragraph_lines = []
                in_table = True
            table_buffer.append(line)
        elif in_table:
            # Allow blank lines between table header and rows
            if line.strip() == "":
                continue
            elif not is_table_row(line) and not is_table_separator(line):
                # End of table when a non-table line appears
                if table_buffer:
                    results.append("\n".join(table_buffer))
                    table_buffer = []
                in_table = False
                paragraph_lines.append(line)
            else:
                table_buffer.append(line)
        else:
            # Regular text handling (existing logic)
            if len(line) == 0:
                blank_line = True
            elif line.startswith("#"):
                if paragraph_lines:
                    results.append("\n".join(paragraph_lines))
                    paragraph_lines = []
                results.append(line)
                blank_line = False
            elif re.match(ORDERED_LIST_ITEM_PATTERN, line) or re.match(
                UNORDERED_LIST_ITEM_PATTERN, line
            ):
                if blank_line and paragraph_lines:
                    paragraph_lines.append("")
                paragraph_lines.append(line)
                blank_line = False
            else:
                if blank_line and paragraph_lines:
                    results.append("\n".join(paragraph_lines))
                    paragraph_lines = []
                paragraph_lines.append(line)
                blank_line = False

    # Add remaining table or paragraph
    if table_buffer:
        results.append("\n".join(table_buffer))
    if paragraph_lines:
        results.append("\n".join(paragraph_lines))

    # Clean up results
    results = [result.strip() for result in results if result.strip()]

    # Handle orphan headers or short paragraphs
    for i in range(len(results) - 2):
        if results[i].startswith("# ") and (
            (len(results[i + 1].split()) <= 5 and results[i + 2].startswith("# "))
            or results[i + 1].startswith("# ")
        ):
            results[i] = results[i].lstrip("# ")

    # Remove duplicates or join short paragraphs
    for i in range(len(results) - 1):
        if results[i] == results[i + 1]:
            results[i] = ""
        elif not results[i].startswith("#") and len(results[i].split()) <= 5:
            results[i] = f"{results[i-1]}\n{results[i]}"
            results[i - 1] = ""
    return [result for result in results if result]



def update_headers(par: str, headers: dict) -> None:
    """Set headers for a paragraph"""

    # Check if paragraph starts with a header (e.g. '# ', '## ', etc.)
    for level in range(1, 7):
        if par.startswith("#" * level + " "):
            headers[f"header_{level}"] = par.lstrip("# ")
            # Reset lower-level headers
            for lower_level in range(level + 1, 7):
                sub_header = f"header_{lower_level}"
                if sub_header in headers:
                    del headers[sub_header]
            # Exit loop after finding the correct header level
            break


def get_paragraph_nodes(
    headers_paragraphs: list[str], doc_node: BaseNode
) -> list[TextNode]:
    """Get paragraph nodes with header metadata from a list of headers
    and paragraphs and the original document node."""

    nodes = []
    headers = {}
    doc_metadata = {key: value for key, value in doc_node.metadata.items()}
    filepath = doc_metadata.get("filepath")
    for par in headers_paragraphs:
        # if this is a header, update the headers
        if par.startswith("#"):
            update_headers(par, headers)
        else:
            metadata = {
                **headers,
                **doc_metadata,
                "context": par,
            }
            if filepath is not None:
                metadata["filepath"] = filepath
            node = TextNode(metadata=metadata, text=par)
            nodes.append(node)
    return nodes


def _equal_headers(metadata1: dict, metadata2: dict) -> bool:
    """Check if two metadata dictionaries have the same headers."""

    for key in MD_METADATA_KEYS:
        if metadata1.get(key, "") != metadata2.get(key, ""):
            return False
    return True


def include_prev_next_contexts(
    nodes: list[TextNode], count: int, max_length: int
) -> None:
    """
    Include up to count previous and next contexts in the context of each node
    while they have the same headers and the combined length is less than the max length.
    """

    node_contexts = [node.metadata["context"] for node in nodes]
    for i, node in enumerate(nodes):
        node_context = node_contexts[i]
        for j in range(1, count + 1):
            prev_node = None if i - j < 0 else nodes[i - j]
            next_node = None if i + j >= len(nodes) else nodes[i + j]
            prev_context = ""
            next_context = ""
            if prev_node and _equal_headers(node.metadata, prev_node.metadata):
                prev_context = node_contexts[i - j]
            if next_node and _equal_headers(node.metadata, next_node.metadata):
                next_context = node_contexts[i + j]
            if len(prev_context) + len(node_context) + len(next_context) <= max_length:
                node_context = (
                    f"{prev_context}\n\n{node_context}\n\n{next_context}".strip()
                )
            else:
                break
        node.metadata["context"] = node_context


def get_sentences(nodes: list[TextNode]) -> list[TextNode]:
    """Get sentence nodes from paragraph nodes."""

    nlp = spacy.load("en_core_web_sm")
    sentence_nodes = []
    for node in nodes:
        doc = nlp(node.text)
        for sent in doc.sents:
            metadata = {key: value for key, value in node.metadata.items()}
            sentence_nodes.append(TextNode(metadata=metadata, text=sent.text))
    return sentence_nodes


def embed_prev_next(
    nodes: list[TextNode], count: int, max_length: int
) -> list[TextNode]:
    """
    Include up to count previous and next texts in the text of each node
    while they have the same headers and the combined length is less than the max length.
    """

    embed_nodes = []
    for i, node in enumerate(nodes):
        node_text = node.text
        for j in range(1, count + 1):
            prev_node = None if i - j < 0 else nodes[i - j]
            next_node = None if i + j >= len(nodes) else nodes[i + j]
            prev_text = ""
            next_text = ""
            if prev_node and _equal_headers(node.metadata, prev_node.metadata):
                prev_text = prev_node.text
            if next_node and _equal_headers(node.metadata, next_node.metadata):
                next_text = next_node.text
            if len(prev_text) + len(node_text) + len(next_text) <= max_length:
                node_text = f"{prev_text}\n\n{node_text}\n\n{next_text}".strip()
            else:
                break
        node = TextNode(
            metadata={key: value for key, value in node.metadata.items()},
            text=node_text,
        )
        embed_nodes.append(node)
    return embed_nodes


def embed_metadata(nodes: list[TextNode], metadata_keys: list[str]):
    """Include metadata in the text of each node."""

    for node in nodes:
        headers = []
        for key in metadata_keys:
            value = node.metadata.get(key, "")
            if value:
                headers.append(value)
        if len(headers) > 0:
            node.text = f"{'/ '.join(headers)}\n\n{node.text}"


def include_metadata(nodes: list[TextNode], metadata_keys: list[str]):
    """Include metadata in the context of each node."""

    for node in nodes:
        headers = []
        for key in metadata_keys:
            value = node.metadata.get(key, "")
            if value:
                headers.append(value)
        if len(headers) > 0:
            node.metadata["context"] = (
                f"{'/ '.join(headers)}\n\n{node.metadata.get('context', '')}"
            )


def run_pipeline(documents, splitter, embed_model, vector_store, include_prev_next_rel):
    """Run the ingestion pipeline to split documents, generate embeddings, and insert into an index."""
    pipeline = IngestionPipeline(
        transformations=[
            splitter,
            embed_model,
        ]
    )
    index = VectorStoreIndex.from_vector_store(
        vector_store,
        embed_model=embed_model,
    )
    nodes = pipeline.run(documents=documents, show_progress=False)

    if include_prev_next_rel:
        for i in range(0, len(nodes)):
            if i > 0:
                nodes[i].metadata["prev"] = nodes[i - 1].text
            if i < len(nodes) - 1:
                nodes[i].metadata["next"] = nodes[i + 1].text

    # from the metadata, remove the "context" key
    for node in nodes:
        if "context" in node.metadata:
            node.text = node.metadata["context"]
            del node.metadata["context"]

    url = nodes[0].metadata['url']
    sequence = 1

    for node in nodes:
        
        # ignore files without a URL
        if 'url' not in node.metadata:
            print(f"Node without URL: {node.metadata}")
            continue
        
        if url == node.metadata['url']:
            node.metadata['sequence'] = sequence
            sequence += 1
        else:
            url = node.metadata['url']
            sequence = 1
            node.metadata['sequence'] = sequence
            sequence += 1

    index.insert_nodes(nodes)
    print(f"Nodes inserted: {len(nodes)}")
    return index, nodes  # CHANGED

def get_vector_store():
    api_key = os.getenv("PINECONE_API_KEY")
    index_name = os.getenv("PINECONE_INDEX_NAME")
    environment = os.getenv("PINECONE_ENVIRONMENT")
    if not api_key or not index_name or not environment:
        raise ValueError(
            "Please set PINECONE_API_KEY, PINECONE_INDEX_NAME, and PINECONE_ENVIRONMENT"
            " to your environment variables or config them in the .env file"
        )
    store = PineconeVectorStore(
        api_key=api_key,
        index_name=index_name,
        environment=environment,
    )
    return store