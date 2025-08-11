import json
import os


def analyze_logs():
    """
    Analyzes the pipeline's detailed log file and prints a summary of the results.
    """
    detailed_log_path = os.path.join(os.getenv("DATA_PATH"), "pipeline_detailed_log.jsonl")

    if not os.path.exists(detailed_log_path):
        print(f"Detailed log file not found: {detailed_log_path}")
        return

    failed_http_errors = []
    direct_loads = []

    with open(detailed_log_path) as f:
        for line in f:
            try:
                log_entry = json.loads(line)
                if log_entry.get("status") == "FAILED_HTTP_ERROR":
                    failed_http_errors.append(log_entry)
                elif log_entry.get("message") == "Loaded TXT file directly without LlamaParse.":
                    direct_loads.append(log_entry)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON line: {line.strip()}")

    print("\n--- Pipeline Log Analysis ---")

    if failed_http_errors:
        print(f"\nFAILED_HTTP_ERROR: {len(failed_http_errors)}")
        for error in failed_http_errors:
            print(f"    - URL: {error.get('url')}")
            print(f"    - Filepath: {error.get('filepath')}")
        print('\nmessage: "Failed to download content due to HTTP errors."')
        print(
            "*The pipeline encountered HTTP errors when trying to download the content from the URLs listed above. This could be due to various reasons, such as the URL being invalid, the server being unavailable, or a lack of permissions to access the content.*"
        )

    if direct_loads:
        print(f"\nDIRECT_LOAD: {len(direct_loads)}")
        for load in direct_loads:
            print(f"    - URL: {load.get('url')}")
            print(f"    - Filepath: {load.get('filepath')}")
        print('\nmessage: "Loaded TXT file directly without LlamaParse."')
        print(
            "*The pipeline detected that the following file[s] was a plain text file and did not require markdown conversion via LlamaParse. Instead, it was read and processed as-is. So these TXT files are handled by direct loading, bypassing LlamaParse, since they are already in a simple text format suitable for further processing.*"
        )


if __name__ == "__main__":
    analyze_logs()
