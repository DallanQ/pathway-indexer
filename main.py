import os
import time
import json
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
    out_folder = os.path.join(os.getenv("DATA_PATH"), "out")
    for root, _dirs, files in os.walk(out_folder):
        for file in files:
            if file.endswith(".md"):
                file_path = os.path.join(root, file)
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                # Check for files with empty content (literally nothing or only whitespace)
                if not content.strip():
                    stats["files_with_empty_content"] += 1
                    continue  # Don't check other conditions if it's empty

                # Check for files with only metadata
                if content.strip().startswith("---"):
                    parts = content.strip().split("---")
                    # A valid frontmatter block will result in at least 3 parts
                    if len(parts) >= 3:
                        # The actual content is everything after the second '---'
                        actual_content = "---".join(parts[2:])
                        if not actual_content.strip():
                            stats["files_with_only_metadata"] += 1

                # Check for files with error messages in the content
                if "Could not parse" in content or "Rate limit exceeded" in content:
                    stats["files_with_error_messages"] += 1



load_dotenv()
DATA_PATH = os.getenv("DATA_PATH")


def main():
    start_time = time.time()
    stats = {
        "total_documents_crawled": 0,
        "unique_files_processed": 0,
        "documents_sent_to_llamaparse": 0,
        "documents_retried": {},
        "documents_empty_from_llamaparse": 0,
        "documents_rescued_by_fallback": 0,
        "documents_failed_after_fallback": 0,
        "md_files_generated": 0,
        "files_with_only_metadata": 0,
        "files_with_error_messages": 0,
        "files_with_empty_content": 0,
    }
    detail_json_path = "data/last_crawl_detail.json"
    output_data_path = "data/last_output_data.csv"


    print("Initializing JSON file...")
    last_data_json = initialize_json_file(detail_json_path, output_data_path)

    print("Getting indexes...\n")
    get_indexes()

    print("Crawler Started...\n")
    crawl_data(stats)

    print("===>Starting parser...\n")
    parse_files_to_md(last_data_json=last_data_json, stats=stats)

    print("===>Updating crawl timestamp...\n")
    update_crawl_timestamp(detail_json_path, DATA_PATH)

    print("===>Copying output CSV...\n")
    copy_output_csv(DATA_PATH, output_data_path)

    print("===>Inspecting generated .md files...\n")
    inspect_md_files(stats)

    end_time = time.time()
    execution_seconds = end_time - start_time
    hours, rem = divmod(execution_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    stats["execution_time"] = f"{int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds"

    print("===>Process completed")
    print(json.dumps(stats, indent=4))


if __name__ == "__main__":
    main()
