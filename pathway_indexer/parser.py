import os
import shutil

import pandas as pd

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

    print(last_data_json["last_folder_crawl"])

    files_to_process = analyze_file_changes(output_data_path, last_output_data_path, out_folder, last_data_json)
    if not files_to_process.empty:
        process_modified_files(input_directory, out_folder, metadata_csv, excluded_domains_path)

    # Save current_df as last_output_data.csv for next run
    # files_to_process.drop(columns=["HasChanged"], inplace=True)
    print("All tasks completed successfully.")


def analyze_file_changes(output_data_path, last_output_data_path, out_folder, last_data_json):
    """
    Analyze file changes by comparing current and last output data based on Content Hash.

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

    if not os.path.exists(last_output_data_path):
        print("Last output data file not found; processing all files.")
        return current_df  # Process all files if there's no last output data

    last_df = pd.read_csv(last_output_data_path)
    last_hash_dict = last_df.set_index("URL")["Content Hash"].to_dict()

    def has_changes(row):
        last_hash = last_hash_dict.get(row["URL"])
        return row["Content Hash"] != last_hash

    current_df["HasChanged"] = current_df.apply(has_changes, axis=1)
    files_to_process = current_df[current_df["HasChanged"]]
    files_to_skip = current_df[~current_df["HasChanged"]]

    # Copy unchanged files and remove them from input directory
    for _, row in files_to_skip.iterrows():
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

    return files_to_process


def process_modified_files(input_directory, out_folder, metadata_csv, excluded_domains_path):
    """
    Process modified files and associate metadata with Markdown files.

    Parameters:
    - input_directory (str): Path to the directory containing HTML and PDF files.
    - out_folder (str): Output folder for Markdown files.
    - metadata_csv (str): Path to the CSV file containing metadata.
    - excluded_domains_path (str): Path to the file containing excluded domains.
    """
    if is_directory_empty(input_directory):
        print("No modified files found; skipping file processing.")
        return

    print("Starting file processing for modified files...")
    process_directory(input_directory, out_folder)
    print("File processing for modified files completed.")

    add_titles_tag(input_directory, out_folder)

    print("Associating Markdown files with metadata...")
    excluded_domains = []
    if os.path.exists(excluded_domains_path):
        with open(excluded_domains_path, encoding="UTF-8") as f:
            excluded_domains = f.read().splitlines()

    metadata_dict = associate_markdown_with_metadata(input_directory, out_folder, metadata_csv, excluded_domains)
    print("Metadata association completed.")

    print("Attaching metadata to Markdown files...")
    attach_metadata_to_markdown_directories(out_folder, metadata_dict)
    print("Metadata attachment completed.")


def is_directory_empty(directory_path):
    return not os.listdir(directory_path)
