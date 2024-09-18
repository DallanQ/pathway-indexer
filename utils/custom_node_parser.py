from collections.abc import Sequence
from typing import Any, Optional

import frontmatter
import spacy
from llama_index.core.bridge.pydantic import Field
from llama_index.core.callbacks.base import CallbackManager
from llama_index.core.node_parser.interface import NodeParser
from llama_index.core.schema import BaseNode, MetadataMode, TextNode
from llama_index.core.utils import get_tqdm_iterable


class CustomNodeParser(NodeParser):
    """Custom Node Parser"""

    add_metadata_to_text: bool = Field(
        default=False,
        description=(
            "Indicates whether to prepend metadata to the node's text content. "
            "If set to True, metadata such as headers "
            "included at the beginning of the text for each node."
        ),
    )

    split_by_sentence: bool = Field(
        default=False,
        description=(
            "Specifies whether to divide the text into individual sentences. "
            "When enabled (True), the text will be split at sentence boundaries, "
            "producing smaller units for more granular processing."
        ),
    )

    @classmethod
    def from_defaults(
        cls,
        include_metadata: bool = True,
        include_prev_next_rel: bool = True,
        add_metadata_to_text: bool = False,
        split_by_sentence: bool = False,
        callback_manager: Optional[CallbackManager] = None,
    ) -> "CustomNodeParser":
        callback_manager = callback_manager or CallbackManager([])

        return cls(
            include_metadata=include_metadata,
            include_prev_next_rel=include_prev_next_rel,
            callback_manager=callback_manager,
            add_metadata_to_text=add_metadata_to_text,
            split_by_sentence=split_by_sentence,
        )

    @classmethod
    def class_name(cls) -> str:
        """Get class name."""
        return "CustomNodeParser"

    def _parse_nodes(
        self,
        nodes: Sequence[BaseNode],
        show_progress: bool = False,
        **kwargs: Any,
    ) -> list[BaseNode]:
        all_nodes: list[BaseNode] = []
        nodes_with_progress = get_tqdm_iterable(nodes, show_progress, "Parsing nodes")

        for node in nodes_with_progress:
            nodes = self.get_nodes_from_node(node, **kwargs)
            all_nodes.extend(nodes)

        return all_nodes

    def _set_headers(self, par: str, header_levels: dict) -> dict:
        """Set headers for a paragraph"""

        # Check if paragraph starts with a header (e.g. '# ', '## ', etc.)
        for level in range(1, 7):
            if par.startswith("#" * level + " "):
                header_levels[level] = par
                # Reset lower-level headers
                for lower_level in range(level + 1, 7):
                    header_levels[lower_level] = None
                # Exit loop after finding the correct header level
                break

        # Build headers dictionary dynamically
        headers = {f"header_{i}": header_levels[i] for i in range(1, 7) if header_levels[i]}
        return headers

    def split_document_text(
        self,
        paragraphs: list[str],
        md_metadata: dict,
        add_metadata_to_text: bool = False,
        split_by_sentence: bool = False,
    ) -> list[TextNode]:
        """Split text into paragraphs"""
        result = []
        headers = {}
        header_levels = {1: None, 2: None, 3: None, 4: None, 5: None, 6: None}
        nlp = spacy.load("en_core_web_sm")

        for par in paragraphs:
            headers = self._set_headers(par, header_levels)

            metadata = {
                **md_metadata,
                **headers,
                "paragraph": par,
            }

            # use spacy to split paragraph into sentences
            if split_by_sentence:
                doc = nlp(par)
                for sent in doc.sents:
                    if add_metadata_to_text:
                        text = ""
                        for key, value in metadata.items():
                            if key != "paragraph":
                                text += str(value) + "\n"
                        text += sent.text
                    else:
                        text = sent.text

                    node = TextNode(metadata=metadata, text=text)
                    result.append(node)

            else:
                if add_metadata_to_text:
                    text = ""
                    for value in metadata.values():
                        text += str(value) + "\n"
                    text += par
                else:
                    text = par
                # Create a TextNode and add to result
                node = TextNode(metadata=metadata, text=text)
                result.append(node)

        return result

    def get_nodes_from_node(self, node: BaseNode, **kwargs: Any) -> list[TextNode]:
        """Get nodes from a node"""
        text = node.get_content(metadata_mode=MetadataMode.NONE)
        post = frontmatter.loads(text)
        metadata = post.metadata
        paragraphs = post.content.split("\n\n")
        return self.split_document_text(
            md_metadata=metadata,
            paragraphs=paragraphs,
            add_metadata_to_text=self.add_metadata_to_text,
            split_by_sentence=self.split_document_text,
        )
