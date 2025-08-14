import os
import shutil

import pandas as pd

from utils.calendar_format import calendar_format
from utils.parser import (
    add_titles_tag,
    associate_markdown_with_metadata,
    attach_metadata_to_markdown_directories,
    process_directory,
)

DATA_PATH = os.getenv("DATA_PATH")
OUT_PATH = os.path.join(DATA_PATH, "out")
EXCLUDED_PATH = os.path.join(DATA_PATH, "excluded_domains.txt")


def parse_files_to_md(
    last_data_json,
    stats,
    detailed_log_path,
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

    # print(last_data_json["last_folder_crawl"])

    files_to_process = analyze_file_changes(output_data_path, last_output_data_path, out_folder, last_data_json, stats)
    if not files_to_process.empty:
        empty_llamaparse_files_counted = set()
        process_modified_files(
            input_directory,
            out_folder,
            metadata_csv,
            excluded_domains_path,
            stats,
            empty_llamaparse_files_counted,
            detailed_log_path,
        )

    # Save current_df as last_output_data.csv for next run
    # files_to_process.drop(columns=["HasChanged"], inplace=True)
    print("All tasks completed successfully.")


def analyze_file_changes(output_data_path, last_output_data_path, out_folder, last_data_json, stats):
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
        print(f"Output data file not found: {output_data_path}")
        return pd.DataFrame()  # Return empty DataFrame

    current_df = pd.read_csv(output_data_path)

    # Separate HTML and PDF files
    html_df = current_df[current_df["Content Type"] == "html"]
    pdf_df = current_df[current_df["Content Type"] == "pdf"]

    if not os.path.exists(last_output_data_path):
        print("Last output data file not found; processing all files.")
        stats["files_processed"] = len(current_df)
        stats["files_skipped_due_to_no_change"] = 0
        stats["pdf_files_always_processed"] = len(current_df[current_df["Content Type"] == "pdf"])
        with open(os.path.join(DATA_PATH, "processed_files.log"), "w") as f:
            for _, row in current_df.iterrows():
                f.write(f"{row["URL"]}\n")
        return current_df  # Process all files if no last output data

    last_df = pd.read_csv(last_output_data_path)
    last_hash_dict = last_df[last_df["Content Type"] == "html"].set_index("URL")["Content Hash"].to_dict()

    # Apply hash check only to HTML files
    def has_changes(row):
        last_hash = last_hash_dict.get(row["URL"])
        return row["Content Hash"] != last_hash

    html_df["HasChanged"] = html_df.apply(has_changes, axis=1)
    changed_html_files = html_df[html_df["HasChanged"]]
    unchanged_html_files = html_df[~html_df["HasChanged"]]

    # Copy unchanged HTML files and remove them from input directory
    for _, row in unchanged_html_files.iterrows():
        print("Skipping unchanged file:", row["Filepath"])
        pathname = os.path.basename(row["Filepath"]).replace(".html", ".md")
        src_path = os.path.join(last_data_json["last_folder_crawl"], "out", "from_html", pathname)
        dst_path = os.path.join(out_folder, "from_html", pathname)

        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        # Copy unchanged file as .md in out_folder
        if os.path.exists(src_path):
            shutil.copyfile(src_path, dst_path)
            print(f"Copied {src_path} to {dst_path}")

        # Remove unchanged file from input_directory
        if os.path.exists(row["Filepath"]):
            os.remove(row["Filepath"])
            print(f"Removed {row['Filepath']} from input_directory")

    # Combine changed HTML files with all PDF files for processing
    files_to_process = pd.concat([changed_html_files, pdf_df], ignore_index=True)

    stats["files_skipped_due_to_no_change"] = len(unchanged_html_files)
    stats["pdf_files_always_processed"] = len(pdf_df)

    # Log skipped files
    with open(os.path.join(DATA_PATH, "skipped_files.log"), "w") as f:
        for _, row in unchanged_html_files.iterrows():
            f.write(f"{row["URL"]}\n")

    # Log files to be processed
    with open(os.path.join(DATA_PATH, "processed_files.log"), "w") as f:
        for _, row in files_to_process.iterrows():
            f.write(f"{row["URL"]}\n")

    stats["files_processed"] = len(files_to_process)
    return files_to_process


def process_modified_files(
    input_directory,
    out_folder,
    metadata_csv,
    excluded_domains_path,
    stats,
    empty_llamaparse_files_counted,
    detailed_log_path,
):
    """
    Process modified files and associate metadata with Markdown files.
    """
    if is_directory_empty(input_directory):
        print("No modified files found; skipping file processing.")
        return

    print("Starting file processing for modified files...")

    stats["files_processed_by_directory"] = process_directory(
        input_directory, out_folder, stats, empty_llamaparse_files_counted, detailed_log_path
    )  # convert the files to md

    print("File processing for modified files completed.")

    add_titles_tag(input_directory, out_folder)

    print("Associating Markdown files with metadata...")
    excluded_domains = []
    if os.path.exists(excluded_domains_path):
        with open(excluded_domains_path, encoding="UTF-8") as f:
            excluded_domains = f.read().splitlines()

    all_links_path = os.path.join(DATA_PATH, metadata_csv)
    if not os.path.exists(all_links_path):
        print(f"Error: {all_links_path} not found. Cannot attach metadata.")
        return

    metadata_dict = associate_markdown_with_metadata(out_folder, all_links_path, excluded_domains)
    print("Metadata association completed.")

    print("Attaching metadata to Markdown files...")
    attach_metadata_to_markdown_directories(out_folder, metadata_dict)
    print("Metadata attachment completed.")

    print("Processing special formats...")
    calendar_format(input_directory, metadata_csv)


def is_directory_empty(directory_path):
    return not os.listdir(directory_path)
