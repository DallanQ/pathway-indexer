import csv
import logging
import os
import re
import time

import nest_asyncio
import yaml
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from llama_index.core import Document, SimpleDirectoryReader
from llama_parse import LlamaParse
from markdownify import markdownify as md
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared
from unstructured_client.models.errors import SDKError

from utils.markdown_utils import unstructured_elements_to_markdown
from utils.tools import get_domain, get_files

# Set the logging level to WARNING or higher to suppress INFO messages
logging.basicConfig(level=logging.WARNING)
nest_asyncio.apply()
load_dotenv()


def clean_title(title):
    # replace enters with spaces
    title = title.replace("\n", " ")
    # replace a lot of spaces with one space
    title = " ".join(title.split())
    # trim the text
    title = title.strip()

    return title


def is_empty_content(content):
    content = content.replace("\n", "").replace(" ", "")
    return not content


def clean_markdown(text):
    text = re.sub(r"```markdown+", "", text)

    # Remove Markdown backticks
    text = re.sub(r"```+", "", text)

    # Remove inline code backticks (`text`)
    text = re.sub(r"`+", "", text)

    text = re.sub(r"\[Print\]\(javascript:window\.print\(\)\)", "", text)

    # Remove list of links with same anchors
    text = re.sub(r"(?:(https?:\/\/[^\s]+)\s+){2,}", "", text)  # Remove repeated links

    # Replace [link](#) and [link](url) with link text only
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", text)

    # Remove lists of links to the same page (e.g., [All](#) [Web Pages](#))
    text = re.sub(r"(\[([^\]]+)\]\(#\))+(?:\s|,)*", "", text)

    # Regular expression to remove unnecessary text from
    # knowledge base articles
    # Remove specific table headers
    text = re.sub(r"\| \*\*Bot Information\*\* \|\n\| --- \|", "", text)
    text = re.sub(r"\| \*\*Information\*\* \|\n\| --- \|", "", text)
    text = re.sub(r"Views:\n\n\|\s*Article Overview\s*\|\s*\n\|\s*---\s*\|\s*\n\|.*?\|", "", text, flags=re.DOTALL)
    text = re.sub(r"\|\s*Information\s*\|\s*\n\|\s*---\s*\|\s*\n\|.*?\|", "", text, flags=re.DOTALL)
    text = re.sub(r"\|\s*Bot Information\s*\|\s*\n\|\s*---\s*\|\s*\n\|.*?\|", "", text, flags=re.DOTALL)
    text = re.sub(r"\n\s*\*\*Information\*\*\s*\n", "\n", text)
    text = re.sub(r"##? Views:\n\n\| \*\*Article Overview\*\* \|\n\| --- \|\n\|.*?\|", "", text, flags=re.DOTALL)
    text = re.sub(r"Views:\n\n\| \*\*Article Overview\*\* \|\n\| --- \|\n\|.*?\|", "", text, flags=re.DOTALL)
    text = re.sub(r"^\| Information \|\n", "", text, flags=re.MULTILINE)
    text = re.sub(r"\*\s*(Home|Knowledge Base - Home|KA-\d+)\s*\n", "", text)
    text = re.sub(
        r"(You’re offline.*?Knowledge Articles|Contoso, Ltd\.|BYU-Pathway Worldwide|Toggle navigation[.\w\s\*\+\-\:]+|Search Filter|Search\n|Knowledge Article Key:)",
        "",
        text,
    )
    text = re.sub(r"You’re offline\. This is a read only version of the page\.", "", text)

    # Others regular expressions to remove unnecessary text
    # Remove empty headers
    text = re.sub(r"^#+\s*$", "", text, flags=re.MULTILINE)

    # Remove text from WhatsApp navigation
    text = re.sub(r"Copy link\S*", "Copy link", text)

    # Remove text from the hall foundation menu
    # text = re.sub(r"(Skip to content|Menu|[*+-].*)\n", '', text, flags=re.MULTILINE)

    # Remove broken links
    text = re.sub(r"\[([^\]]+)\]\.\n\n\((http[^\)]+)\) \(([^)]+)\)\.", r"\1 (\3).", text)

    # Remove consecutive blank lines
    text = re.sub(r"\n\s*\n\s*\n", "\n\n", text)

    return text


# Helper functions for cleaning and parsing HTML and PDF content
def clean_html(soup):
    """Cleans the HTML content by removing unnecessary elements and extracting the title text."""
    # Extract the title text
    title_text = soup.title.string if soup.title else None

    # Remove unnecessary elements
    for tag in soup([
        "head",
        "style",
        "script",
        "img",
        "svg",
        "meta",
        "link",
        "iframe",
        "noscript",
        "footer",
        "nav",
        "ps-header",
    ]):
        tag.decompose()

    # Create selectors to remove elements
    selectors = [
        '[aria-label="Search Filter"]',
        '[aria-label*="Menu"]',
        '[aria-label*="menu"]',
        '[class*="menu"]',
        '[class*="Menu"]',
        '[role="region"]',
        '[role="dialog"]',
        ".sr-only",
        ".navbar",
        ".breadcrumb",
        ".btn-toolbar",
        ".skip-link",
    ]

    # Remove elements by selectors
    for selector in selectors:
        for tag in soup.select(selector):
            tag.decompose()
    # Determine the content container (main or body)
    content = soup.main or soup.body

    if content and title_text:
        # Create a title header and insert it at the beginning
        title_header = soup.new_tag("title")
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
        text = text.replace("'", "").replace(",", " |").replace("\n", " ")

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

    os.makedirs(os.path.dirname(file_out), exist_ok=True)

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

    title = soup.contents[0]
    title_tag = title.text if title.name == "title" else ""
    if title_tag:
        title.decompose()

    markdown_content = md(str(cleaned_soup), heading_style="ATX")
    markdown_content = re.sub(r"\n{2,}", "\n\n", markdown_content)

    file_out = os.path.join(out_folder, "from_html", os.path.basename(file_path).replace(".html", ".txt"))
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(file_out), exist_ok=True)

    if is_empty_content(markdown_content):
        return file_path, "Error parsing."

    with open(file_out, "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print(f"Converted HTML to TXT and saved to: {file_out}")


    return file_out, title_tag


def create_file_extractor(parse_type="pdf"):
    """Create a file extractor based on the parsing type (pdf or html)"""

    if parse_type == ".pdf":
        parser = LlamaParse(
            api_key=os.environ["LLAMA_CLOUD_API_KEY"],
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
            api_key=os.environ["LLAMA_CLOUD_API_KEY"],
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

def has_markdown_tables(content):
    """Check if content contains markdown tables"""
    table_patterns = [
        r'\|.*\|.*\|',          # Table row with cells
        r'\|[\s-]*\|[\s-]*\|'   # Table header separator
    ]
    return all(re.search(pattern, content, re.MULTILINE) for pattern in table_patterns)

def parse_txt_to_md(file_path, file_extension, title_tag=""):
    """
    Parses a .txt file to a Markdown (.md) file using LlamaParse.
    """
    # get the file extension

    with open(file_path, encoding="utf-8") as f:
        content = f.read()

    print(f"Attempting LlamaParse for file: {file_path}")
    try:
        #get the file extension
        file_extractor = create_file_extractor(file_extension)
        documents = SimpleDirectoryReader(
                input_files=[file_path], file_extractor=file_extractor
            ).load_data()
        is_empty = all(is_empty_content(doc.text) for doc in documents)
        if is_empty:
            print(f"LlamaParse returned empty document for {file_path}, falling back to original text.")
            documents = [Document(text=content)]
        

    except Exception as e:
        print(f"LlamaParse failed for {file_path}: {e}, falling back to original text.")
        documents = [Document(text=content)]

    # size = sum([len(doc.text) for doc in documents])
    # validate if the content is empty

    is_empty = all(is_empty_content(doc.text) for doc in documents)

    # base_filename = os.path.basename(file_path)
    out_name = file_path.replace(".txt", ".md")

    title_tag = clean_title(title_tag)

    # os.makedirs(os.path.dirname(out_name), exist_ok=True)

    with open(out_name, "w", encoding="utf-8") as f:
        if title_tag:
            f.write(f"title: {title_tag}\n")
        for doc in documents:
            f.write(doc.text)
            f.write("\n\n")
        print(f"Parsed TXT to MD and saved to: {out_name}")

    return is_empty


def associate_markdown_with_metadata(data_path, markdown_dirs, csv_file, excluded_domains):
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

    all_files = get_files(markdown_dirs)
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
                "subheading": (clean_text(row["Subsection"]) if clean_text(row["Subsection"]) != "Missing" else ""),
                "title": clean_text(row["Title"]),
                "role": row["role"],
            }

    # Now go through the markdown files in each directory and associate them with the metadata
    markdown_metadata_mapping = {}
    # List to save files without metadata
    no_metadata = []

    for markdown_path in all_files:
        # Get the markdown filename without the extension
        markdown_filename_without_ext = os.path.splitext(os.path.basename(markdown_path))[0]

        # Check if the filename matches any entry in the CSV dictionary
        if markdown_filename_without_ext in file_metadata_mapping:
            # Store the path relative to the directory
            # full_path = os.path.join(markdown_dir, markdown_filename)
            markdown_metadata_mapping[markdown_path] = file_metadata_mapping[markdown_filename_without_ext]
            # open the file, and read if the first line begins with "title: "
            with open(markdown_path, encoding="utf-8") as file:
                lines = file.readlines()

            if len(lines) == 0:
                continue
            # Revisar si la primera línea contiene el título
            first_line = lines[0].strip()

            # get the url from the metadata
            url = markdown_metadata_mapping[markdown_path]["url"]
            if first_line.startswith("title: "):
                # Extraer el título de la primera línea
                title = first_line.replace("title: ", "")
                if get_domain(url) not in excluded_domains:
                    markdown_metadata_mapping[markdown_path]["title_tag"] = title

                # Eliminar la primera línea (la que contiene el título)
                lines = lines[1:]

                # Guardar el archivo sin la primera línea
                with open(markdown_path, "w", encoding="utf-8") as file:
                    file.writelines(lines)

            # clean the markdown file and save it
            with open(markdown_path, encoding="utf-8") as file:
                content = file.read()
                content = clean_markdown(content)
            with open(markdown_path, "w", encoding="utf-8") as file:
                file.write(content)

        else:
            print(f"No metadata found for {markdown_path}. Skipping.")
            no_metadata.append(markdown_path)

    # Guardamos en CSV las rutas de Markdown sin metadata
    no_metadata_csv_path = os.path.join(data_path, "no_metadata.csv")
    with open(no_metadata_csv_path, mode="w", newline="", encoding="utf-8") as nm_file:
        writer = csv.writer(nm_file)
        writer.writerow(["markdown_path"])
        for nm_path in no_metadata:
            writer.writerow([nm_path])

    print("\nMarkdown files and their metadata:")
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


def process_file(file_path, out_folder):
    """
    Processes a file based on its extension: PDF or HTML.
    """

    txt_file_path = ""
    title_tag = ""
    # get the file extension
    file_extension = os.path.splitext(file_path)[1]

    if file_path.lower().endswith(".pdf"):
        # Handle PDF file
        for _ in range(3):
            txt_file_path = parse_pdf_to_txt(file_path, out_folder)
            if txt_file_path != "Error":
                break
            print("Error parsing PDF file. Retrying...")
            time.sleep(4)

    elif file_path.lower().endswith(".html"):
        # Handle HTML file
        for _ in range(3):
            txt_file_path, title_tag = convert_html_to_markdown(file_path, out_folder)
            if title_tag != "Error parsing.":
                break
            print("Error converting HTML file. Retrying...")
            time.sleep(4)

    if title_tag != "Error parsing.":
        # try a maximum of 3 times to parse the txt file to md
        for _ in range(3):
            is_empty = parse_txt_to_md(txt_file_path, file_extension, title_tag)
            if txt_file_path and not is_empty:
                # remove the txt file
                os.remove(txt_file_path)
                return
            print("Error parsing TXT file to MD. Retrying...")
            time.sleep(4)

    # move the txt file to the error folder
    error_folder = os.path.join(out_folder, "error")
    if txt_file_path:
        os.rename(txt_file_path, os.path.join(error_folder, os.path.basename(txt_file_path)))  # moving the file
        print(f"Error parsing TXT file to MD. Moved to {error_folder}")


def process_directory(origin_path, out_folder):
    """
    Processes all HTML and PDF files in the specified directory.
    """
    for root, _dirs, files in os.walk(origin_path):
        if "error" in root:
            continue
        for file in files:
            if file.lower().endswith((".html", ".pdf")):
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                process_file(file_path, out_folder)


def add_titles_tag(input_directory, out_folder):
    all_files = get_files(input_directory)
    # save only html files
    html_files = [file for file in all_files if file.endswith(".html")]
    # ignore the error folder
    html_files = [file for file in html_files if "error" not in file]
    out_files = get_files(out_folder)

    print(f"=== input directory: {input_directory}===")
    # Load a soup object from each html, get the title, and add it to the first line of the markdown file
    for file_path in html_files:
        with open(file_path, encoding="utf-8") as file:
            content = file.read()
        soup = BeautifulSoup(content, "html.parser")
        title = soup.title.string if soup.title else ""
        title = clean_title(title)

        if not title:
            continue

        print(f"title exist in {file_path}")

        # get the markdown file by filename
        filename = os.path.basename(file_path).replace(".html", ".md")
        md_file = [file for file in out_files if filename in file]

        if len(md_file) == 0:
            print(f"Markdown file not found for {filename}")
            continue
        # open the file
        with open(md_file[0], encoding="utf-8") as file:
            content = file.read()

        with open(md_file[0], "w", encoding="utf-8") as f:
            f.write(f"title: {title}\n")

            f.write(content)

        print(f"Title added to {filename}")
        print()
