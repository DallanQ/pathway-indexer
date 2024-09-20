import logging
import os
import re

import nest_asyncio
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from llama_index.core import SimpleDirectoryReader
from llama_parse import LlamaParse
from markdownify import markdownify as md
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared
from unstructured_client.models.errors import SDKError

from utils.markdown_utils import unstructured_elements_to_markdown

# Set the logging level to WARNING or higher to suppress INFO messages
logging.basicConfig(level=logging.WARNING)
nest_asyncio.apply()
load_dotenv()


# Helper functions for cleaning and parsing HTML and PDF content
def clean_html(soup):
    # Extract the title text
    title_text = soup.title.string if soup.title else None

    # Remove unnecessary elements
    for tag in soup(["head", "style", "script", "img", "svg", "meta", "link", "iframe", "noscript"]):
        tag.decompose()

    # Determine the content container (main or body)
    content = soup.main or soup.body

    if content and title_text:
        # Create a title header and insert it at the beginning
        title_header = soup.new_tag("h1")
        title_header.string = title_text
        content.insert(0, title_header)

    return content or soup  # Return the cleaned content or the entire soup as a fallback


def clean_text(text):
    cleaned_text = re.sub(r"\x00", "th", text)
    return cleaned_text


def parse_pdf_to_txt(file):
    """
    Parse PDF file to a text file.
    """
    s = UnstructuredClient(
        api_key_auth=os.environ["UNSTRUCTURED_API_KEY"],
        server_url=os.environ["UNSTRUCTURED_SERVER_URL"],
    )
    file_path = file["path"]
    print("Processing PDF file:", file_path)

    with open(file_path, "rb") as f:
        files = shared.Files(content=f.read(), file_name=file_path)

    req = shared.PartitionParameters(
        files=files,
        strategy="fast",
        languages=["eng"],
        encoding="utf-8",
    )

    try:
        resp = s.general.partition(req)
    except SDKError as e:
        print(e)
        return True
    except Exception as e:
        print("Another exception", e)
        return True

    simple_md = unstructured_elements_to_markdown(resp.elements)
    simple_md = clean_text(simple_md)

    file["size"] = len(simple_md)
    file_out = file["path"].replace(".pdf", ".txt")

    with open(file_out, "w") as f:
        f.write(simple_md)

    return False


def convert_html_to_markdown(file_path):
    """
    Converts HTML content from a file to Markdown and saves it to a new .txt file.
    """
    with open(file_path, encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")
    cleaned_soup = clean_html(soup)
    markdown_content = md(str(cleaned_soup))

    base_filename = os.path.basename(file_path)
    markdown_file_path = os.path.join("try", "txt", base_filename.replace(".html", ".txt"))

    os.makedirs(os.path.dirname(markdown_file_path), exist_ok=True)

    with open(markdown_file_path, "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print(f"Converted HTML to Markdown and saved to: {markdown_file_path}")


def create_file_extractor():
    parser = LlamaParse(
        result_type="markdown",
        parsing_instruction=(
            "Convert the provided text into accurate and well-structured Markdown format, strictly preserving the original structure. "
            "Use headers from H1 to H3 only where they naturally occur in the text..."
        ),
    )
    file_extractor = {".txt": parser}
    return file_extractor


def parse_txt_to_md(file):
    """
    Parses a .txt file to a Markdown (.md) file using LlamaParse.
    """
    documents = SimpleDirectoryReader(input_files=[file], file_extractor=create_file_extractor()).load_data()

    size = sum([len(doc.text) for doc in documents])

    base_filename = os.path.basename(file)
    out_name = os.path.join("try", "md", base_filename.replace(".txt", ".md"))

    os.makedirs(os.path.dirname(out_name), exist_ok=True)

    with open(out_name, "w", encoding="utf-8") as f:
        for doc in documents:
            f.write(doc.text)
            f.write("\n\n")
        print(f"{out_name} saved.")

    return size


def process_file(file_path):
    """
    Processes a file based on its extension: PDF or HTML.
    """
    if file_path.lower().endswith(".pdf"):
        # Handle PDF file
        file_info = {"path": file_path}
        parse_pdf_to_txt(file_info)

        # Parse the resulting .txt to .md
        txt_file_path = file_info["path"].replace(".pdf", ".txt")
        parse_txt_to_md(txt_file_path)

    elif file_path.lower().endswith(".html"):
        # Handle HTML file
        convert_html_to_markdown(file_path)

        # Parse the resulting .txt to .md
        txt_file_path = os.path.join("try", "txt", os.path.basename(file_path).replace(".html", ".txt"))
        parse_txt_to_md(txt_file_path)


def process_directory(directory):
    """
    Processes all HTML and PDF files in the specified directory.
    """
    for root, _dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith((".html", ".pdf")):
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                process_file(file_path)


def main(directory="try/files"):
    """
    Main function to process a directory containing HTML and PDF files.
    """
    if not os.path.isdir(directory):
        print(f"Directory not found: {directory}")
        return

    # Process all HTML and PDF files in the directory
    process_directory(directory)


if __name__ == "__main__":
    main()
