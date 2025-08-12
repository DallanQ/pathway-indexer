import json
import os
from collections import defaultdict

import pandas as pd


def analyze_logs():
    """
    Analyzes the pipeline's detailed log file and prints a summary of the results.
    """
    DATA_PATH = os.getenv("DATA_PATH")
    detailed_log_path = os.path.join(DATA_PATH, "pipeline_detailed_log.jsonl")
    all_links_path = os.path.join(DATA_PATH, "all_links.csv")

    if not os.path.exists(detailed_log_path):
        print(f"Detailed log file not found: {detailed_log_path}")
        return

    if not os.path.exists(all_links_path):
        print(f"all_links.csv not found: {all_links_path}")
        return

    crawl_counts = defaultdict(int)
    parse_counts = defaultdict(int)
    failed_http_errors = []
    direct_loads = []
    log_urls = []
    parse_filepaths = set()

    with open(detailed_log_path) as f:
        for line in f:
            try:
                log_entry = json.loads(line)
                stage = log_entry.get("stage")
                status = log_entry.get("status")

                if stage == "crawl":
                    crawl_counts[status] += 1
                    log_urls.append(log_entry.get("url"))
                elif stage == "parse" or stage == "parse_txt_to_md":
                    parse_counts[status] += 1
                    if status in ["HTML_PROCESSING_ATTEMPT", "PDF_PROCESSING_ATTEMPT"]:
                        if log_entry.get("filepath"):
                            parse_filepaths.add(log_entry.get("filepath"))

                if log_entry.get("status") == "FAILED_HTTP_ERROR":
                    failed_http_errors.append(log_entry)
                elif log_entry.get("message") == "Loaded TXT file directly without LlamaParse.":
                    direct_loads.append(log_entry)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON line: {line.strip()}")

    all_links_df = pd.read_csv(all_links_path)
    all_links_urls = set(all_links_df["URL"])
    log_urls_set = set(log_urls)
    missing_urls = all_links_urls - log_urls_set

    print("\n--- Pipeline Log Analysis ---")

    print(f"\nTotal URLs in all_links.csv: {len(all_links_urls)}")
    print(f"Total URLs processed by crawler: {len(log_urls_set)}")
    print(f"Number of missing URLs: {len(missing_urls)}")

    if missing_urls:
        print("\nMissing URLs (in all_links.csv but not in log):")
        for url in missing_urls:
            print(f"    - {url}")

    print("\nCrawl Stage Results:")
    print(f"Total files processed: {sum(crawl_counts.values())}")
    for status, count in crawl_counts.items():
        print(f"{status}: {count}")

    print("\nParse Stage Results:")
    print(f"Total unique files processed: {len(parse_filepaths)}")
    for status, count in parse_counts.items():
        print(f"{status}: {count}")

    if failed_http_errors:
        print(f"\nFAILED_HTTP_ERROR: {len(failed_http_errors)}")
        for error in failed_http_errors:
            filepath = error.get("filepath") if error.get("filepath") is not None else "N/A"
            print(f"    - URL: {error.get('url')}, Filepath: {filepath}")
        print('\nmessage: "Failed to download content due to HTTP errors."')
        print(
            "*The pipeline encountered HTTP errors when trying to download the content from the URLs listed above. This could be due to various reasons, such as the URL being invalid, the server being unavailable, or a lack of permissions to access the content.*"
        )

    if direct_loads:
        print(f"\nDIRECT_LOAD: {len(direct_loads)}")
        for load in direct_loads:
            filepath = load.get("filepath") if load.get("filepath") is not None else "N/A"
            print(f"    - Filepath: {filepath}, URL: {load.get('url')}")
        print('\nmessage: "Loaded TXT file directly without LlamaParse."')
        print(
            "*The pipeline detected that the following file[s] was a plain text file and did not require markdown conversion via LlamaParse. Instead, it was read and processed as-is. So these TXT files are handled by direct loading, bypassing LlamaParse, since they are already in a simple text format suitable for further processing.*"
        )


if __name__ == "__main__":
    analyze_logs()
