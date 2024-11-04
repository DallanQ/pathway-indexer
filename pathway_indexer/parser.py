import os 
from utils.parser import (
    process_directory,
    associate_markdown_with_metadata,
    attach_metadata_to_markdown_directories,
    add_titles_tag
)

DATA_PATH = os.getenv("DATA_PATH")
OUT_PATH = os.path.join(DATA_PATH, "out")
EXCLUDED_PATH = os.path.join(DATA_PATH, "excluded_domains.txt")

def parse_files_to_md(input_directory=DATA_PATH, out_folder=OUT_PATH, metadata_csv="all_links.csv", excluded_domains_path=EXCLUDED_PATH):
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
    process_directory(input_directory, out_folder)
    print("File processing completed.\n")

    # Add titles
    add_titles_tag(input_directory,out_folder)

    # Step 2: Associate Markdown files with metadata from CSV
    print("Associating Markdown files with metadata...")
    # load excluded domains as list
    if os.path.exists(excluded_domains_path):
        with open(excluded_domains_path, "r", encoding="UTF-8") as f:
            excluded_domains = f.read().splitlines()
    else:
        excluded_domains = []
    metadata_dict = associate_markdown_with_metadata(input_directory, out_folder, metadata_csv, excluded_domains)
    # print the metadata dictionary with a json format
    print("Metadata association completed.\n")

    # Step 3: Attach metadata to Markdown files as YAML front matter
    print("Attaching metadata to Markdown files...")
    attach_metadata_to_markdown_directories(out_folder, metadata_dict)
    print("Metadata attachment completed.\n")

    print("All tasks completed successfully.")
