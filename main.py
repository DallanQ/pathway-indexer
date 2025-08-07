import json
import os
import time

from dotenv import load_dotenv

from pathway_indexer.crawler import crawl_data
from pathway_indexer.get_indexes import get_indexes
from pathway_indexer.memory import (
    copy_output_csv,
    initialize_json_file,
    update_crawl_timestamp,
)
from pathway_indexer.parser import parse_files_to_md


def inspect_md_files(stats):
    # Reset counter for files with only metadata
    stats["files_with_only_metadata"] = 0
    md_file_count = 0
    out_folder = os.path.join(os.getenv("DATA_PATH"), "out")
    for root, _dirs, files in os.walk(out_folder):
        for file in files:
            if file.endswith(".md"):
                md_file_count += 1
                file_path = os.path.join(root, file)
                with open(file_path, encoding="utf-8") as f:
                    content = f.read()
                # Check for files with only metadata
                if content.strip().startswith("---"):
                    parts = content.strip().split("---")
                    if len(parts) >= 3:
                        actual_content = "---".join(parts[2:])
                        if not actual_content.strip():
                            stats["files_with_only_metadata"] += 1
    stats["md_files_generated"] = md_file_count


load_dotenv()
DATA_PATH = os.getenv("DATA_PATH")


def main():
    start_time = time.time()
    stats = {
        "total_documents_crawled": 0,
        "unique_files_processed": 0,
        "documents_sent_to_llamaparse": 0,
        "documents_empty_from_llamaparse": 0,
        "documents_rescued_by_fallback": 0,
        "documents_failed_after_fallback": 0,
        "md_files_generated": 0,
        "files_with_only_metadata": 0,
        "files_processed_outside_change_detection": 0,
    }
    detail_json_path = "data/last_crawl_detail.json"
    output_data_path = "data/last_output_data.csv"
    detailed_log_path = os.path.join(DATA_PATH, "pipeline_detailed_log.jsonl")

    # Ensure the parent directory for the detailed log file exists
    os.makedirs(os.path.dirname(detailed_log_path), exist_ok=True)
    # Initialize detailed log file
    with open(detailed_log_path, "w") as f:
        f.write("")  # Clear content from previous runs

    print("Initializing JSON file...")
    last_data_json = initialize_json_file(detail_json_path, output_data_path)

    print("Getting indexes...\n")
    get_indexes()

    print("Crawler Started...\n")
    crawl_data(stats, detailed_log_path)

    print("===>Starting parser...\n")
    parse_files_to_md(last_data_json=last_data_json, stats=stats, detailed_log_path=detailed_log_path)
    stats["files_processed_outside_change_detection"] = (
        stats["files_processed_by_directory"] - stats["unique_files_processed"]
    )

    print("===>Updating crawl timestamp...\n")
    update_crawl_timestamp(detail_json_path, DATA_PATH)

    print("===>Copying output CSV...\n")
    copy_output_csv(DATA_PATH, output_data_path)

    print("===>Inspecting generated .md files...\n")
    inspect_md_files(stats)

    end_time = time.time()
    execution_seconds = end_time - start_time
    hours = execution_seconds // 3600
    minutes = (execution_seconds % 3600) // 60
    seconds = int(execution_seconds % 60)
    hours_str = f"{int(hours)} hour" if int(hours) == 1 else f"{int(hours)} hours"
    minutes_str = f"{int(minutes)} minute" if int(minutes) == 1 else f"{int(minutes)} minutes"
    seconds_str = f"{int(seconds)} second" if int(seconds) == 1 else f"{int(seconds)} seconds"
    stats["execution_time"] = f"{hours_str}, {minutes_str}, {seconds_str}"

    print("===>Process completed")
    print(json.dumps(stats, indent=4))


if __name__ == "__main__":
    main()
