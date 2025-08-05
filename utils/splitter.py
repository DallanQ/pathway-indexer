import csv
import os

import frontmatter
import spacy
from llama_index.core.schema import TextNode


def read_file(file_path: str) -> str:
    with open(file_path) as f:
        post = frontmatter.load(f)
        return post


def set_headers(par: str, header_levels: dict) -> dict:
    """Set headers for a paragraph"""

    # Check if paragraph starts with a header (e.g., '# ', '## ', etc.)
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
    paragraphs: list[str], md_metadata: dict, add_metadata_to_text: bool = False, split_by_sentence: bool = False
) -> list[TextNode]:
    """Split text into paragraphs"""
    result = []
    headers = {}
    header_levels = {1: None, 2: None, 3: None, 4: None, 5: None, 6: None}
    nlp = spacy.load("en_core_web_sm")

    for par in paragraphs:
        headers = set_headers(par, header_levels)

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


def splitter(folder: list, split_by_sentence: bool = False, add_metadata_to_text: bool = False) -> list[TextNode]:
    """Split text into paragraphs and log the node count for each file."""
    files = os.listdir(folder)
    files = [f for f in files if os.path.isfile(os.path.join(folder, f))]

    nodes = []
    log_data = []
    log_file_path = "data/nodes_split_log.csv"

    # Ensure the directory for the log file exists
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    for file in files:
        path = os.path.join(folder, file)
        post = read_file(path)
        metadata = post.metadata
        paragraphs = post.content.split("\n\n")

        # Generate nodes for the current file
        file_nodes = split_document_text(paragraphs, metadata, split_by_sentence, add_metadata_to_text)

        # Log the file path and the number of nodes generated
        log_data.append([path, len(file_nodes)])

        # Add the generated nodes to the main list
        nodes.extend(file_nodes)

    # Write the log data to a CSV file
    with open(log_file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["file_path", "nodes_generated"])
        writer.writerows(log_data)

    print(f"Node splitting log saved to {log_file_path}")

    return nodes
