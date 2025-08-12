import json
import os
from collections import defaultdict

import pandas as pd


def analyze_logs():
    """
    Analyzes the pipeline's detailed log file and prints a summary of the results.
    """

    import dotenv

    dotenv.load_dotenv()
    DATA_PATH = os.getenv("DATA_PATH")

    # Prepare metrics_explanation.log path
    metrics_explanation_path = os.path.join(DATA_PATH, "metrics_explanation.log")

    # Directly read ACM, Missionary, Help, and Student Services link counts from all_links.csv
    all_links_csv = os.path.join(DATA_PATH, "all_links.csv")
    acm_links = 0
    missionary_links = 0
    help_links = 0
    student_services_links = 0
    if os.path.exists(all_links_csv):
        df = pd.read_csv(all_links_csv)
        if "Role" in df.columns:
            acm_links = (df["Role"] == "ACM").sum()
            missionary_links = (df["Role"] == "missionary").sum()
            help_links = (df["Role"] == "help").sum()
            student_services_links = (df["Role"] == "student_services").sum()
    # Prepare index summary string in the previous format
    index_summary = "\n--- Index Counts ---\n"
    index_summary += f"ACM links: {acm_links}\n"
    index_summary += f"Missionary links: {missionary_links}\n"
    index_summary += f"Help links: {help_links}\n"
    index_summary += f"Student Services links: {student_services_links}\n"

    # Collect log analyzer output into a string
    output_lines = []
    output_lines.append("\n--- Pipeline Log Analysis ---\n")

    crawl_counts = defaultdict(int)
    parse_counts = defaultdict(int)
    failed_http_errors = []
    direct_loads = []
    log_urls = []
    parse_filepaths = set()

    detailed_log_path = os.path.join(DATA_PATH, "pipeline_detailed_log.jsonl")
    all_links_path = os.path.join(DATA_PATH, "all_links.csv")

    if not os.path.exists(detailed_log_path):
        output_lines.append(f"Detailed log file not found: {detailed_log_path}\n")
    elif not os.path.exists(all_links_path):
        output_lines.append(f"all_links.csv not found: {all_links_path}\n")
    else:
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
                    output_lines.append(f"Skipping invalid JSON line: {line.strip()}\n")

        all_links_df = pd.read_csv(all_links_path)
        all_links_urls = set(all_links_df["URL"])
        log_urls_set = set(log_urls)
        missing_urls = all_links_urls - log_urls_set

        output_lines.append(f"\nTotal URLs in all_links.csv: {len(all_links_urls)}\n")
        output_lines.append(f"Total URLs processed by crawler: {len(log_urls_set)}\n")
        output_lines.append(f"Number of missing URLs: {len(missing_urls)}\n")

        if missing_urls:
            output_lines.append("\nMissing URLs (in all_links.csv but not in log):\n")
            for url in missing_urls:
                output_lines.append(f"    - {url}\n")

        output_lines.append("\nCrawl Stage Results:\n")
        output_lines.append(f"Total files processed: {sum(crawl_counts.values())}\n")
        for status, count in crawl_counts.items():
            output_lines.append(f"{status}: {count}\n")

        output_lines.append("\nParse Stage Results:\n")
        output_lines.append(f"Total unique files processed: {len(parse_filepaths)}\n")
        for status, count in parse_counts.items():
            output_lines.append(f"{status}: {count}\n")

        if failed_http_errors:
            output_lines.append(f"\nFAILED_HTTP_ERROR: {len(failed_http_errors)}\n")
            for error in failed_http_errors:
                filepath = error.get("filepath") if error.get("filepath") is not None else "N/A"
                output_lines.append(f"    - URL: {error.get('url')}, Filepath: {filepath}\n")
            output_lines.append('\n       message: "Failed to download content due to HTTP errors."\n')
            output_lines.append(
                "       *The pipeline encountered HTTP errors when trying to download the content from the URLs listed above. This could be due to various reasons, such as \n       the URL being invalid, the server being unavailable, or a lack of permissions to access the content.*\n"
            )

        if direct_loads:
            output_lines.append(f"\nDIRECT_LOAD: {len(direct_loads)}\n")
            for load in direct_loads:
                filepath = load.get("filepath") if load.get("filepath") is not None else "N/A"
                output_lines.append(f"    - Filepath: {filepath}, URL: {load.get('url')}\n")
            output_lines.append('\n       message: "Loaded TXT file directly without LlamaParse."\n')
            output_lines.append(
                "       *The pipeline detected that the following file[s] was a plain text file and did not require markdown conversion via LlamaParse. Instead, it was \n       read and processed as-is. So these TXT files are handled by direct loading, bypassing LlamaParse, since they are already in a simple text format \n       suitable for further processing.*\n"
            )

    # Write combined output to metrics_explanation.log
    with open(metrics_explanation_path, "a") as f:
        f.write(index_summary)
        f.write("\n--- log_analyzer ---\n")
        f.write("".join(output_lines))


if __name__ == "__main__":
    analyze_logs()
