import json
import os
from collections import defaultdict

def analyze_logs():
    """
    Analyzes the pipeline's detailed log file and prints a summary of the results.
    """
    detailed_log_path = os.path.join(os.getenv("DATA_PATH"), "pipeline_detailed_log.jsonl")

    if not os.path.exists(detailed_log_path):
        print(f"Detailed log file not found: {detailed_log_path}")
        return

    crawl_counts = defaultdict(int)
    parse_counts = defaultdict(int)
    failed_http_errors = []
    direct_loads = []

    with open(detailed_log_path, "r") as f:
        for line in f:
            try:
                log_entry = json.loads(line)
                stage = log_entry.get('stage')
                status = log_entry.get('status')

                if stage == 'crawl':
                    crawl_counts[status] += 1
                elif stage == 'parse' or stage == 'parse_txt_to_md':
                    parse_counts[status] += 1

                if log_entry.get("status") == "FAILED_HTTP_ERROR":
                    failed_http_errors.append(log_entry)
                elif log_entry.get("message") == "Loaded TXT file directly without LlamaParse.":
                    direct_loads.append(log_entry)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON line: {line.strip()}")

    print("\n--- Pipeline Log Analysis ---")

    print("\nCrawl Stage Results:")
    print(f"Total files processed: {sum(crawl_counts.values())}")
    for status, count in crawl_counts.items():
        print(f"{status}: {count}")

    print("\nParse Stage Results:")
    print(f"Total files processed: {sum(parse_counts.values())}")
    for status, count in parse_counts.items():
        print(f"{status}: {count}")

    if failed_http_errors:
        print(f"\nFAILED_HTTP_ERROR: {len(failed_http_errors)}")
        for error in failed_http_errors:
            filepath = error.get('filepath') if error.get('filepath') is not None else 'N/A'
            print(f"    - URL: {error.get('url')}, Filepath: {filepath}")
        print("\nmessage: \"Failed to download content due to HTTP errors.\"")
        print("*The pipeline encountered HTTP errors when trying to download the content from the URLs listed above. This could be due to various reasons, such as the URL being invalid, the server being unavailable, or a lack of permissions to access the content.*")

    if direct_loads:
        print(f"\nDIRECT_LOAD: {len(direct_loads)}")
        for load in direct_loads:
            filepath = load.get('filepath') if load.get('filepath') is not None else 'N/A'
            print(f"    - URL: {load.get('url')}, Filepath: {filepath}")
        print("\nmessage: \"Loaded TXT file directly without LlamaParse.\"")
        print("*The pipeline detected that the following file[s] was a plain text file and did not require markdown conversion via LlamaParse. Instead, it was read and processed as-is. So these TXT files are handled by direct loading, bypassing LlamaParse, since they are already in a simple text format suitable for further processing.*")


if __name__ == "__main__":
    analyze_logs()