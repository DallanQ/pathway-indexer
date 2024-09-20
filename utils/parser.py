import csv
import logging
import os
import re

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
    if (text.startswith("'") and text.endswith("'")) or (text.startswith('"') and text.endswith('"')):
        text = text[1:-1].strip()
        text = text.replace("'", "").replace(",", " |")

    return text


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

    with open(file_out, "w", encoding="utf-8") as f:
        f.write(simple_md)

    print(f"Parsed PDF to TXT and saved to: {file_out}")
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

    print(f"Converted HTML to TXT and saved to: {markdown_file_path}")


def create_file_extractor():
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
        print(f"Parsed TXT to MD and saved to: {out_name}")

    return size


def associate_markdown_with_metadata(markdown_dirs, csv_file):
    """
    Associates Markdown files with metadata from a CSV file.

    Parameters:
    - markdown_dirs (list): List of directories containing Markdown files.
    - csv_file (str): Path to the CSV file containing metadata.

    Returns:
    - dict: Mapping of Markdown file paths to their corresponding metadata.
    """
    # Read the CSV file and store the file paths, URLs, headings, and subheadings in a dictionary
    file_metadata_mapping = {}
    with open(csv_file, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Extract the filename without the extension and use it as the key
            filename_with_ext = os.path.basename(row["filename"])
            filename_without_ext = os.path.splitext(filename_with_ext)[0]

            # Store metadata using filename without extension as the key
            file_metadata_mapping[filename_without_ext] = {
                "url": row["URL"],
                "heading": clean_text(row["Section"]),
                "subheading": clean_text(row["Subsection"]) if clean_text(row["Subsection"]) != "Missing" else "",
                "title": clean_text(row["Title"]),
            }

    # Now go through the markdown files in each directory and associate them with the metadata
    markdown_metadata_mapping = {}

    # Loop through each directory provided
    for markdown_dir in markdown_dirs:
        for markdown_filename in os.listdir(markdown_dir):
            # Get the markdown filename without the extension
            markdown_filename_without_ext = os.path.splitext(markdown_filename)[0]

            # Check if the filename matches any entry in the CSV dictionary
            if markdown_filename_without_ext in file_metadata_mapping:
                # Store the path relative to the directory
                full_path = os.path.join(markdown_dir, markdown_filename)
                markdown_metadata_mapping[full_path] = file_metadata_mapping[markdown_filename_without_ext]
            else:
                print(f"No metadata found for {markdown_filename}. Skipping.")

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
    for directory_path in markdown_dirs:
        # Loop through each markdown file in the directory
        for filename in os.listdir(directory_path):
            if filename.endswith(".md"):
                file_path = os.path.join(directory_path, filename)
                if file_path in metadata_dict:  # Check if full path is in metadata_dict
                    # Extract metadata
                    metadata = metadata_dict[file_path]

                    # Open the markdown file, remove existing YAML front matter, and prepend new metadata
                    with open(file_path, "r+", encoding="utf-8") as file:
                        content = file.read()
                        # Remove any existing front matter
                        content_without_frontmatter = remove_existing_yaml_frontmatter(content)
                        # Prepare the new YAML front matter
                        yaml_metadata = yaml.dump(metadata, default_flow_style=False, allow_unicode=True)
                        front_matter = f"---\n{yaml_metadata}---\n"
                        # Write the new front matter and content back to the file
                        file.seek(0, 0)
                        file.write(front_matter + content_without_frontmatter)
                        file.truncate()  # Ensure the file doesn't retain any old content beyond the new content
                    print(f"Metadata attached to {file_path}")
                else:
                    print(f"No metadata found for {file_path}. Skipping.")


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


def main(input_directory="try/files", metadata_csv="all_links.csv"):
    """
    Main function to process a directory containing HTML and PDF files and attach metadata.

    Parameters:
    - input_directory (str): Path to the directory containing HTML and PDF files.
    - metadata_csv (str): Path to the CSV file containing metadata.
    """
    if not os.path.isdir(input_directory):
        print(f"Directory not found: {input_directory}")
        return

    # Step 1: Process all HTML and PDF files in the directory
    print("Starting file processing...")
    process_directory(input_directory)
    print("File processing completed.\n")

    # Step 2: Associate Markdown files with metadata from CSV
    print("Associating Markdown files with metadata...")
    markdown_dirs = [os.path.join("try", "md")]  # Directory where Markdown files are saved
    metadata_dict = associate_markdown_with_metadata(markdown_dirs, metadata_csv)
    print("Metadata association completed.\n")

    # Step 3: Attach metadata to Markdown files as YAML front matter
    print("Attaching metadata to Markdown files...")
    attach_metadata_to_markdown_directories(markdown_dirs, metadata_dict)
    print("Metadata attachment completed.\n")

    print("All tasks completed successfully.")


if __name__ == "__main__":
    # You can modify these paths as needed
    INPUT_DIRECTORY = "try/files"  # Directory containing HTML and PDF files
    METADATA_CSV = "all_links.csv"  # Path to your metadata CSV file

    main(input_directory=INPUT_DIRECTORY, metadata_csv=METADATA_CSV)
