import re
import os
import shutil
import csv
import logging

import pandas as pd

from utils.parser import (
    add_titles_tag,
    associate_markdown_with_metadata,
    attach_metadata_to_markdown_directories,
    process_directory,
)

from utils.calendar_format import calendar_format

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_PATH = os.getenv("DATA_PATH")
OUT_PATH = os.path.join(DATA_PATH, "out")
EXCLUDED_PATH = os.path.join(DATA_PATH, "excluded_domains.txt")


def parse_files_to_md(
    last_data_json,
    input_directory=DATA_PATH,
    out_folder=OUT_PATH,
    metadata_csv="all_links.csv",
    excluded_domains_path=EXCLUDED_PATH,
    last_output_data_path="data/last_output_data.csv",
):
    """
    Main function to process a directory containing HTML and PDF files and attach metadata, avoiding parsing files with unchanged content.
    """
    output_data_path = os.path.join(DATA_PATH, "output_data.csv")

    files_to_process = analyze_file_changes(
        output_data_path, last_output_data_path, out_folder, last_data_json
    )
    llama_parse_count, indexed_count, empty_files_count, documents_retried, documents_rescued_by_fallback, documents_failed_after_fallback, documents_sent_to_llamaparse_initial = 0, 0, 0, 0, 0, 0, 0
    if not files_to_process.empty:
        llama_parse_count, indexed_count, empty_files_count, documents_retried, documents_rescued_by_fallback, documents_failed_after_fallback, documents_sent_to_llamaparse_initial = process_modified_files(
            input_directory, out_folder, metadata_csv, excluded_domains_path
        )

    logging.info("All tasks completed successfully.")
    files_with_only_metadata, files_with_error_messages, files_with_empty_content = analyze_markdown_quality(out_folder)
    logging.info(f"Markdown quality analysis: {files_with_only_metadata} files with only metadata, {files_with_error_messages} files with error messages, {files_with_empty_content} empty files.")
    return llama_parse_count, indexed_count, empty_files_count, documents_retried, documents_rescued_by_fallback, documents_failed_after_fallback, files_with_only_metadata, files_with_error_messages, files_with_empty_content, documents_sent_to_llamaparse_initial

def analyze_file_changes(
    output_data_path, last_output_data_path, out_folder, last_data_json
):
    """
    Analyze file changes by comparing current and last output data based on Content Hash,
    only for HTML files. PDF files are always included in files_to_process.

    Parameters:
    - output_data_path (str): Path to the current output data CSV file.
    - last_output_data_path (str): Path to the last output data CSV file for comparison.
    - out_folder (str): Output folder for Markdown files.

    Returns:
    - files_to_process (DataFrame): DataFrame of files that have changed and need processing.
    """
    if not os.path.exists(output_data_path):
        logging.error(f"Output data file not found: {output_data_path}")
        return pd.DataFrame()  # Return empty DataFrame

    current_df = pd.read_csv(output_data_path)
    logging.info(f"Loaded {len(current_df)} records from {output_data_path}")

    # Separate HTML and PDF files
    html_df = current_df[current_df["Content Type"] == "html"]
    pdf_df = current_df[current_df["Content Type"] == "pdf"]
    logging.info(f"Found {len(html_df)} HTML files and {len(pdf_df)} PDF files.")

    if not os.path.exists(last_output_data_path):
        logging.warning("Last output data file not found; processing all files.")
        return current_df  # Process all files if no last output data

    last_df = pd.read_csv(last_output_data_path)
    logging.info(f"Loaded {len(last_df)} records from {last_output_data_path} for comparison.")
    last_hash_dict = (
        last_df[last_df["Content Type"] == "html"]
        .set_index("URL")["Content Hash"]
        .to_dict()
    )

    # Apply hash check only to HTML files
    def has_changes(row):
        last_hash = last_hash_dict.get(row["URL"])
        if last_hash is None:
            logging.info(f"New file, needs processing: {row['Filepath']}")
            return True
        if row["Content Hash"] != last_hash:
            logging.info(f"Content hash changed for {row['Filepath']}, needs reprocessing.")
            return True
        return False

    html_df["HasChanged"] = html_df.apply(has_changes, axis=1)
    changed_html_files = html_df[html_df["HasChanged"]]
    unchanged_html_files = html_df[~html_df["HasChanged"]]

    logging.info(f"{len(changed_html_files)} HTML files have changed and will be re-processed.")
    logging.info(f"{len(unchanged_html_files)} HTML files are unchanged and will be reused.")

    # Copy unchanged HTML files and remove them from input directory
    for _, row in unchanged_html_files.iterrows():
        logging.info(f"Re-using unchanged file: {row['Filepath']}")
        pathname = os.path.basename(row["Filepath"]).replace(".html", ".md")
        src_path = os.path.join(
            last_data_json["last_folder_crawl"], "out", "from_html", pathname
        )
        dst_path = os.path.join(out_folder, "from_html", pathname)

        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        if os.path.exists(src_path):
            shutil.copyfile(src_path, dst_path)
            logging.info(f"Copied pre-parsed markdown from {src_path} to {dst_path}")
        else:
            logging.warning(f"Source markdown file not found for unchanged file: {src_path}. This file will be skipped.")

        if os.path.exists(row["Filepath"]):
            os.remove(row["Filepath"])
            logging.info(f"Removed unchanged source file from input directory: {row['Filepath']}")

    # Combine changed HTML files with all PDF files for processing
    files_to_process = pd.concat([changed_html_files, pdf_df], ignore_index=True)
    logging.info(f"Total files to process (changed HTML + all PDF): {len(files_to_process)}")

    return files_to_process


def process_modified_files(
    input_directory, out_folder, metadata_csv, excluded_domains_path
):
    """
    Process modified files and associate metadata with Markdown files.
    """
    if is_directory_empty(input_directory):
        logging.warning("No modified files found; skipping file processing.")
        return 0, 0, 0, 0, 0, 0, 0

    logging.info("Starting file processing for modified files...")
    (
        llama_parse_count,
        indexed_count,
        empty_files_count,
        documents_retried,
        documents_rescued_by_fallback,
        documents_failed_after_fallback,
        documents_sent_to_llamaparse_initial,
    ) = process_directory(
        input_directory, out_folder
    )  # convert the files to md
    logging.info(f"File processing completed. LlamaParse count: {llama_parse_count}, Indexed count: {indexed_count}, Empty files: {empty_files_count}")

    add_titles_tag(input_directory, out_folder)

    logging.info("Associating Markdown files with metadata...")
    excluded_domains = []
    if os.path.exists(excluded_domains_path):
        with open(excluded_domains_path, encoding="UTF-8") as f:
            excluded_domains = f.read().splitlines()
        logging.info(f"Loaded {len(excluded_domains)} excluded domains.")

    metadata_dict = associate_markdown_with_metadata(
        input_directory, out_folder, metadata_csv, excluded_domains
    )
    logging.info("Metadata association completed.")

    logging.info("Attaching metadata to Markdown files...")
    attach_metadata_to_markdown_directories(out_folder, metadata_dict)
    logging.info("Metadata attachment completed.")

    logging.info("Processing special formats...")
    calendar_format(input_directory, metadata_csv)

    return (
        llama_parse_count,
        indexed_count,
        empty_files_count,
        documents_retried,
        documents_rescued_by_fallback,
        documents_failed_after_fallback,
        documents_sent_to_llamaparse_initial,
    )

def is_directory_empty(directory_path):
    return not os.listdir(directory_path)

def analyze_markdown_quality(out_folder):
    files_with_only_metadata = 0
    files_with_error_messages = 0
    files_with_empty_content = 0

    md_files = []
    for root, _, files in os.walk(out_folder):
        for file in files:
            if file.lower().endswith(".md"):
                md_files.append(os.path.join(root, file))

    for md_file_path in md_files:
        with open(md_file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for only metadata (YAML frontmatter)
        if content.strip().startswith("---") and content.strip().endswith("---") and len(content.strip().splitlines()) < 10:
            logging.warning(f"File contains only metadata: {md_file_path}")
            files_with_only_metadata += 1

        # Check for error messages
        if "Could not parse" in content or "Error parsing." in content:
            logging.error(f"File contains parsing errors: {md_file_path}")
            files_with_error_messages += 1

        # Check for empty content (after removing metadata and potential error messages)
        cleaned_content = re.sub(r"^---\n.*?---\n", "", content, flags=re.DOTALL) # Remove YAML frontmatter
        cleaned_content = cleaned_content.replace("Could not parse", "").replace("Error parsing.", "")
        if not cleaned_content.strip():
            logging.warning(f"File has empty content after cleaning: {md_file_path}")
            files_with_empty_content += 1

    return files_with_only_metadata, files_with_error_messages, files_with_empty_content

