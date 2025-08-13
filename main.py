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
from utils.log_analyzer import analyze_logs


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
        "files_skipped_due_to_no_change": 0,
        "files_processed": 0,
        "documents_sent_to_llamaparse": 0,
        "documents_empty_from_llamaparse": 0,
        "documents_successful_after_retries": 0,
        "documents_failed_after_retries": 0,
        "md_files_generated": 0,
        "files_with_only_metadata": 0,
        "files_processed_outside_change_detection": 0,
    }
    detail_json_path = os.path.join(DATA_PATH, "last_crawl_detail.json")
    output_data_path = os.path.join(DATA_PATH, "last_output_data.csv")
    detailed_log_path = os.path.join(DATA_PATH, "pipeline_detailed_log.jsonl")

    # Ensure the parent directory for the detailed log file exists
    os.makedirs(os.path.dirname(detailed_log_path), exist_ok=True)
    # Initialize detailed log file
    with open(detailed_log_path, "w") as f:
        f.write("")  # Clear content from previous runs
    print(f"Detailed pipeline log will be saved to: {os.path.relpath(detailed_log_path, start=os.getcwd())}")

    print("Initializing JSON file...")
    last_data_json = initialize_json_file(detail_json_path, output_data_path)

    print("===>Getting indexes...\n")
    stats["total_documents_crawled"] = get_indexes()

    print("Crawler Started...\n")
    crawl_data(stats, detailed_log_path)

    print("===>Starting parser...\n")
    # Ensure the key exists before parsing
    stats["files_processed_by_directory"] = 0
    parse_files_to_md(last_data_json=last_data_json, stats=stats, detailed_log_path=detailed_log_path)
    # files_processed_outside_change_detection is set in parser.py as len(pdf_df)

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
    # Write metrics explanation to metrics_explanation.log (overwrite)
    metrics_explanation_path = os.path.join(DATA_PATH, "metrics_explanation.log")
    metrics_explanation = f"""
Here’s what each metric means in your pipeline and indexer results:

Pipeline Metrics

=> total_documents_crawled: {stats.get("total_documents_crawled", "N/A")}
Number of URLs found and listed for crawling.

=> files_skipped_due_to_no_change: {stats.get("files_skipped_due_to_no_change", "N/A")}
Number of files that were not changed and therefore not processed again.

=> files_processed: {stats.get("files_processed", "N/A")}
Number of files that were processed (either new or changed).

=> documents_sent_to_llamaparse: {stats.get("documents_sent_to_llamaparse", "N/A")}
Number of files sent to LlamaParse for conversion to markdown.

=> documents_empty_from_llamaparse: {stats.get("documents_empty_from_llamaparse", "N/A")}
Number of times LlamaParse returned empty content (likely due to unsupported, blank input or API limits).

=> documents_successful_after_retries: {stats.get("documents_successful_after_retries", "N/A")}
Number of files that were rescued by retry logic after LlamaParse failed.

=> documents_failed_after_retries: {stats.get("documents_failed_after_retries", "N/A")}
Number of files that failed to produce content even after all retry attempts.

=> md_files_generated: {stats.get("md_files_generated", "N/A")}
Total markdown files created (one per input file).

=> files_with_only_metadata: {stats.get("files_with_only_metadata", "N/A")}
Markdown files that contain only metadata (no actual content).

=> files_processed_outside_change_detection: {stats.get("files_processed_outside_change_detection", "N/A")}
All files were processed outside change detection.

=> files_processed_by_directory: {stats.get("files_processed_by_directory", "N/A")}
Total files processed by the directory parser (should match input count).

=> execution_time: {stats.get("execution_time", "N/A")}
Total time taken for the pipeline run.
--------------------------------------------------------
    """
    with open(metrics_explanation_path, "w") as f:
        f.write(metrics_explanation)
    # Print path relative to repo root, starting from DATA_PATH
    rel_path = os.path.relpath(metrics_explanation_path, start=os.getcwd())
    analyze_logs()

    print(f"\nWhat do these numbers mean? See ./{rel_path}")


if __name__ == "__main__":
    main()
