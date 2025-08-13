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

    # Get ACM, Missionary, Help, and Student Services link counts by reading individual index CSVs
    index_path = os.path.join(DATA_PATH, "index")
    acm_path = os.path.join(index_path, "acm.csv")
    missionary_path = os.path.join(index_path, "missionary.csv")
    help_path = os.path.join(index_path, "help.csv")
    student_services_path = os.path.join(index_path, "student_services.csv")

    acm_links = missionary_links = help_links = student_services_links = 0

    if os.path.exists(acm_path):
        acm_df = pd.read_csv(acm_path)
        acm_links = len(acm_df)
    if os.path.exists(missionary_path):
        missionary_df = pd.read_csv(missionary_path)
        missionary_links = len(missionary_df)
    if os.path.exists(help_path):
        help_df = pd.read_csv(help_path)
        help_links = len(help_df)
    if os.path.exists(student_services_path):
        student_services_df = pd.read_csv(student_services_path)
        student_services_links = len(student_services_df)
    # Prepare index summary string listing all links (professional format)
    index_summary = f"""
========================================================
                INDEX COUNTS SUMMARY
========================================================
ACM Links:              {acm_links}
Missionary Links:       {missionary_links}
Help Links:             {help_links}
Student Services Links: {student_services_links}
========================================================
"""

    # Collect log analyzer output into a string (professional format)
    output_lines = []
    output_lines.append(
        "\n========================================================\n                PIPELINE LOG ANALYSIS\n========================================================\n"
    )

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

        # Create error folder if it doesn't exist and save missing URLs to CSV
        error_folder = os.path.join(DATA_PATH, "error")
        os.makedirs(error_folder, exist_ok=True)
        
        if missing_urls:
            missing_urls_df = pd.DataFrame({"URL": list(missing_urls)})
            missing_urls_csv_path = os.path.join(error_folder, "filtered_missing_sharepoint_links.csv")
            missing_urls_df.to_csv(missing_urls_csv_path, index=False)

        output_lines.append(f"Total URLs in all_links.csv:           {len(all_links_urls)}\n")
        output_lines.append(f"Total URLs processed by crawler:       {len(log_urls_set)}\n")
        output_lines.append(f"Number of missing URLs:                {len(missing_urls)}\n")

        if missing_urls:
            output_lines.append("\nMissing URLs (in all_links.csv but not in log):\n")
            for url in missing_urls:
                output_lines.append(f"    • {url}\n")

        output_lines.append(
            "\n--------------------------------------------------------\n                CRAWL STAGE RESULTS\n--------------------------------------------------------\n"
        )
        output_lines.append(f"Total files processed:                 {sum(crawl_counts.values())}\n")
        for status, count in crawl_counts.items():
            output_lines.append(f"{status:30}: {count}\n")

        output_lines.append(
            "\n--------------------------------------------------------\n                PARSE STAGE RESULTS\n--------------------------------------------------------\n"
        )
        output_lines.append(f"Total unique files processed:           {len(parse_filepaths)}\n")
        for status, count in parse_counts.items():
            output_lines.append(f"{status:30}: {count}\n")

        if failed_http_errors:
            output_lines.append(
                "\n--------------------------------------------------------\n                HTTP ERRORS\n--------------------------------------------------------\n"
            )
            output_lines.append(f"FAILED_HTTP_ERROR ({len(failed_http_errors)}):\n")
            for error in failed_http_errors:
                filepath = error.get("filepath") if error.get("filepath") is not None else "N/A"
                output_lines.append(f"    • URL: {error.get('url')}\n      Filepath: {filepath}\n")
            output_lines.append('  Message: "Failed to download content due to HTTP errors."\n')
            output_lines.append(
                "  *The pipeline encountered HTTP errors when trying to download the content from the URLs listed above. This could be due to various reasons, such as the URL being invalid, the server being unavailable, or a lack of permissions to access the content.*\n"
            )

        if direct_loads:
            output_lines.append(
                "\n--------------------------------------------------------\n                DIRECT LOADS\n--------------------------------------------------------\n"
            )
            output_lines.append(f"DIRECT_LOAD ({len(direct_loads)}):\n")
            for load in direct_loads:
                filepath = load.get("filepath") if load.get("filepath") is not None else "N/A"
                output_lines.append(f"    • {filepath}\n      URL: {load.get('url')}\n")
            output_lines.append('  Message: "Text files loaded directly without LlamaParse due to markdown table detection."\n')
            output_lines.append(
                "  *These files were processed by the has_markdown_tables() function in utils/parser.py and loaded directly via SimpleDirectoryReader because they contain markdown tables (patterns like |...|...|). Since they are already well-formatted, LlamaParse conversion is skipped to preserve table structure and avoid unnecessary processing.*\n"
            )

    # Write combined output to metrics_explanation.log (append to the file)
    with open(metrics_explanation_path, "a") as f:
        f.write(index_summary)
        f.write("".join(output_lines))


if __name__ == "__main__":
    analyze_logs()
