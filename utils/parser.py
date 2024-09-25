import csv
import logging
import os
import re
import time

import nest_asyncio
import yaml
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from llama_index.core import SimpleDirectoryReader
from llama_parse import LlamaParse
from markdownify import markdownify as md
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared
from unstructured_client.models.errors import SDKError

from utils.markdown_utils import unstructured_elements_to_markdown
from utils.tools import get_files

# Set the logging level to WARNING or higher to suppress INFO messages
logging.basicConfig(level=logging.WARNING)
nest_asyncio.apply()
load_dotenv()


# Helper functions for cleaning and parsing HTML and PDF content
def clean_html(soup):
    # Extract the title text
    title_text = soup.title.string if soup.title else None

    # Remove unnecessary elements
    for tag in soup(
        ["head", "style", "script", "img", "svg", "meta", "link", "iframe", "noscript"]
    ):
        tag.decompose()

    # Determine the content container (main or body)
    content = soup.main or soup.body

    if content and title_text:
        # Create a title header and insert it at the beginning
        title_header = soup.new_tag("h1")
        title_header.string = title_text
        content.insert(0, title_header)

    return (
        content or soup
    )  # Return the cleaned content or the entire soup as a fallback


def clean_text(text):
    """
    Cleans the input text by performing the following operations:
    - Replaces null characters with 'th'.
    - Removes square brackets and quotes.
    - Trims leading and trailing whitespace.

    Parameters:
    text (str): The input text to clean.

    Returns:
    str: The cleaned text.
    """
    if not isinstance(text, str):
        return ""

    # Replace null characters
    text = re.sub(r"\x00", "th", text)

    # Remove leading and trailing whitespace
    text = text.strip()

    # Remove leading and trailing square brackets
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1].strip()

    # Remove leading and trailing quotes (both single and double)
    if (text.startswith("'") and text.endswith("'")) or (
        text.startswith('"') and text.endswith('"')
    ):
        text = text[1:-1].strip()
        text = text.replace("'", "").replace(",", " |")

    return text


def parse_pdf_to_txt(filepath, out_folder):
    """
    Parse PDF file to a text file.
    """
    s = UnstructuredClient(
        api_key_auth=os.environ["UNSTRUCTURED_API_KEY"],
        server_url=os.environ["UNSTRUCTURED_SERVER_URL"],
    )

    file_path = filepath
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
        return "Error"
    except Exception as e:
        print("Another exception", e)
        return "Error"

    simple_md = unstructured_elements_to_markdown(resp.elements)
    simple_md = clean_text(simple_md)

    if not simple_md:
        return "Error"

    # filepath["size"] = len(simple_md)
    file_out = os.path.join(
        out_folder, "from_pdf", os.path.basename(file_path).replace(".pdf", ".txt")
    )  # filepath["path"].replace(".pdf", ".txt")

    with open(file_out, "w", encoding="utf-8") as f:
        f.write(simple_md)

    print(f"Parsed PDF to TXT and saved to: {file_out}")
    return file_out


def convert_html_to_markdown(file_path, out_folder):
    """
    Converts HTML content from a file to Markdown and saves it to a new .txt file.
    """
    with open(file_path, encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")
    cleaned_soup = clean_html(soup)
    markdown_content = md(str(cleaned_soup))

    file_out = os.path.join(
        out_folder, "from_html", os.path.basename(file_path).replace(".html", ".txt")
    )

    # base_filename = os.path.basename(file_path)
    # markdown_file_path = os.path.join("try", "txt", base_filename.replace(".html", ".txt"))

    # os.makedirs(os.path.dirname(markdown_file_path), exist_ok=True)

    # if the content is empty, return "Error"
    if not markdown_content:
        return "Error"

    with open(file_out, "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print(f"Converted HTML to TXT and saved to: {file_out}")

    return file_out


def create_file_extractor(parse_type="pdf"):
    """Create a file extractor based on the parsing type (pdf or html)"""

    if parse_type == ".pdf":
        parser = LlamaParse(
            result_type="markdown",
            parsing_instruction=(
                "Convert the provided text into accurate and well-structured Markdown format, closely resembling the original PDF structure. "
                "Use headers from H1 to H3, with H1 for main titles, H2 for sections, and H3 for subsections. "
                "Detect any bold, large, or all-uppercase text as headers. "
                "Preserve bullet points and numbered lists with proper indentation to reflect nested lists. "
                "if it is not a header, ensure that bold and italic text is properly formatted using double **asterisks** for bold and single *asterisks* for italic"
                "Detect and correctly format blockquotes using the '>' symbol for any quoted text. "
                "When processing text, pay attention to line breaks that may incorrectly join or split words. "
                "Automatically correct common errors, such as wrongly concatenated words or broken lines, to ensure the text reads naturally"
                "If code snippets or technical commands are found, enclose them in triple backticks ``` for proper formatting. "
                "If any tables are detected, parse them as a title (bold header) followed by list items"
                "If you see the same header multiple times, merge them into one."
                "If images contain important text, transcribe only the highlighted or boxed text and ignore general background text. "
                "Do not enclose fragments of code/Markdown or any other content in triple backticks unless they are explicitly formatted as code blocks in the original text. "
                "The final output should be a clean, concise Markdown document closely reflecting the original PDF's intent and structure without adding any extra text."
            ),
        )
    if parse_type == ".html":
        parser = LlamaParse(
            result_type="markdown",  # "markdown" and "text" are available
            parsing_instruction=(
                "Convert the provided text into accurate and well-structured Markdown format, strictly preserving the original structure. "
                "Use headers from H1 to H3 only where they naturally occur in the text, and do not create additional headers or modify existing ones. "
                "Do not split the text into multiple sections or alter the sequence of content. "
                "Detect bold, large, or all-uppercase text as headers only if they represent a natural section break in the original text. "
                "Preserve all links, ensuring that they remain correctly formatted and in their original place in the text. "
                "Maintain bullet points and numbered lists with proper indentation to reflect any nested lists, ensuring list numbers remain in sequence. "
                "If the text is not a header, ensure that bold and italic text is properly formatted using double **asterisks** for bold and single *asterisks* for italic. "
                "Detect and correctly format blockquotes using the '>' symbol for any quoted text, but do not reformat text that is already in correct Markdown format. "
                "Respect the original line breaks and text flow, avoiding unnecessary splits, merges, or reordering of content. "
                "If any tables are detected, parse them as a title (bold header) followed by list items, but do not reformat existing Markdown tables. "
                "Merge identical headers only if they represent the same section and their content is identical, ensuring no changes to the order of the text. "
                "Do not enclose fragments of code/Markdown or any other content in triple backticks unless they are explicitly formatted as code blocks in the original text. "
                "Ensure that the final output is a clean, concise Markdown document that closely reflects the original text's intent and structure, without adding or omitting any content."
            ),
        )
    file_extractor = {".txt": parser}
    return file_extractor


def parse_txt_to_md(file_path, file_extension):
    """
    Parses a .txt file to a Markdown (.md) file using LlamaParse.
    """
    # get the file extension

    documents = SimpleDirectoryReader(
        input_files=[file_path], file_extractor=create_file_extractor(file_extension)
    ).load_data()

    size = sum([len(doc.text) for doc in documents])

    # base_filename = os.path.basename(file_path)
    out_name = file_path.replace(".txt", ".md")

    # os.makedirs(os.path.dirname(out_name), exist_ok=True)

    with open(out_name, "w", encoding="utf-8") as f:
        for doc in documents:
            f.write(doc.text)
            f.write("\n\n")
        print(f"Parsed TXT to MD and saved to: {out_name}")

    return size


def associate_markdown_with_metadata(data_path, markdown_dirs, csv_file):
    """
    Associates Markdown files with metadata from a CSV file.

    Parameters:
    - markdown_dirs (list): List of directories containing Markdown files.
    - csv_file (str): Path to the CSV file containing metadata.

    Returns:
    - dict: Mapping of Markdown file paths to their corresponding metadata.
    """
    # create the csv_path and open it, the data_path is related to the root but has 
    csv_path = os.path.join(data_path, csv_file)
    # Read the CSV file and store the file paths, URLs, headings, and subheadings in a dictionary
    file_metadata_mapping = {}
    with open(csv_path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Extract the filename without the extension and use it as the key
            filename_with_ext = os.path.basename(row["filename"])
            filename_without_ext = os.path.splitext(filename_with_ext)[0]

            # Store metadata using filename without extension as the key
            file_metadata_mapping[filename_without_ext] = {
                "url": row["URL"],
                "heading": clean_text(row["Section"]),
                "subheading": (
                    clean_text(row["Subsection"])
                    if clean_text(row["Subsection"]) != "Missing"
                    else ""
                ),
                "title": clean_text(row["Title"]),
            }

    # Now go through the markdown files in each directory and associate them with the metadata
    markdown_metadata_mapping = {}

    # Loop through each directory provided

    all_files = get_files(markdown_dirs)

    for markdown_path in all_files:
        # Get the markdown filename without the extension
        markdown_filename_without_ext = os.path.splitext(os.path.basename(markdown_path))[0]

        # Check if the filename matches any entry in the CSV dictionary
        if markdown_filename_without_ext in file_metadata_mapping:
            # Store the path relative to the directory
            # full_path = os.path.join(markdown_dir, markdown_filename)
            markdown_metadata_mapping[markdown_path] = file_metadata_mapping[
                markdown_filename_without_ext
            ]
        else:
            print(f"No metadata found for {markdown_path}. Skipping.")

    # Debugging: Print the mapping
    print("Markdown files and their metadata:")
    for path, meta in markdown_metadata_mapping.items():
        print(f"{path}: {meta}")

    return markdown_metadata_mapping


def remove_existing_yaml_frontmatter(content):
    """
    Removes existing YAML front matter from the given content.
    Assumes that front matter is enclosed between '---' markers.
    """
    yaml_pattern = re.compile(r"^---[\s\S]*?---\s", re.MULTILINE)
    return re.sub(yaml_pattern, "", content, count=1)


def attach_metadata_to_markdown_directories(markdown_dirs, metadata_dict):
    """
    Attaches metadata as YAML front matter to Markdown files.

    Parameters:
    - markdown_dirs (list): List of directories containing Markdown files.
    - metadata_dict (dict): Mapping of Markdown file paths to their corresponding metadata.
    """
    # Loop through each directory provided

    all_files = get_files(markdown_dirs, ignored="error/")

    # Loop through each markdown file in the directory
    for file_path in all_files:
        if file_path.endswith(".md"):
            if file_path in metadata_dict:  # Check if full path is in metadata_dict
                # Extract metadata
                metadata = metadata_dict[file_path]

                # Open the markdown file, remove existing YAML front matter, and prepend new metadata
                with open(file_path, "r+", encoding="utf-8") as file:
                    content = file.read()
                    # Remove any existing front matter
                    content_without_frontmatter = remove_existing_yaml_frontmatter(
                        content
                    )
                    # Prepare the new YAML front matter
                    yaml_metadata = yaml.dump(
                        metadata, default_flow_style=False, allow_unicode=True
                    )
                    front_matter = f"---\n{yaml_metadata}---\n"
                    # Write the new front matter and content back to the file
                    file.seek(0, 0)
                    file.write(front_matter + content_without_frontmatter)
                    file.truncate()  # Ensure the file doesn't retain any old content beyond the new content
                print(f"Metadata attached to {file_path}")
            else:
                print(f"No metadata found for {file_path}. Skipping.")


def process_file(file_path, out_folder):
    """
    Processes a file based on its extension: PDF or HTML.
    """

    txt_file_path = ""
    # get the file extension
    file_extension = os.path.splitext(file_path)[1]

    if file_path.lower().endswith(".pdf"):
        # Handle PDF file
        # Intenta un maximo de 3 veces ejecutar el parse_pdf_to_txt solo si se obtiene "Error"
        for _ in range(3):
            txt_file_path = parse_pdf_to_txt(file_path, out_folder)
            if txt_file_path != "Error":
                break
            print("Error parsing PDF file. Retrying...")
            time.sleep(4)

        # txt_file_path = parse_pdf_to_txt(file_path, out_folder)

        # Parse the resulting .txt to .md
        # txt_file_path = file_info["path"].replace(".pdf", ".txt")
        # txt_file_path = os.path.join(out_path, "from_pdf", file_path.replace(".pdf", ".txt"))

    elif file_path.lower().endswith(".html"):
        # Handle HTML file
        # convert_html_to_markdown(file_path)
        for _ in range(3):
            txt_file_path = convert_html_to_markdown(file_path, out_folder)
            if txt_file_path != "Error":
                break
            print("Error converting HTML file. Retrying...")
            time.sleep(4)

        # Parse the resulting .txt to .md
        # txt_file_path = os.path.join("try", "txt", os.path.basename(file_path).replace(".html", ".txt"))
        # txt_file_path = os.path.join(out_folder, "from_html", file_path.replace(".pdf", ".txt"))

    # parse_txt_to_md(txt_file_path, file_extension)
    # try a maximum of 3 times to parse the txt file to md
    for _ in range(3):
        size = parse_txt_to_md(txt_file_path, file_extension)
        if size:
            #remove the txt file
            os.remove(txt_file_path)
            return
        print("Error parsing TXT file to MD. Retrying...")
        time.sleep(4)

    # move the txt file to the error folder
    error_folder = os.path.join(out_folder, "error")
    # os.makedirs(error_folder, exist_ok=True)
    os.rename(txt_file_path, os.path.join(error_folder, os.path.basename(txt_file_path)))
    print(f"Error parsing TXT file to MD. Moved to {error_folder}")


def process_directory(origin_path, out_folder):
    """
    Processes all HTML and PDF files in the specified directory.
    """
    for root, _dirs, files in os.walk(origin_path):
        for file in files:
            if file.lower().endswith((".html", ".pdf")):
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                process_file(file_path, out_folder)
