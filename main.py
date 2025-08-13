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
from store import main as store_main

load_dotenv()
DATA_PATH = os.getenv("DATA_PATH")


def main():
    start_time = time.time()
    detail_json_path = "data/last_crawl_detail.json"
    output_data_path = "data/last_output_data.csv"

    print("Initializing JSON file...")
    last_data_json = initialize_json_file(detail_json_path, output_data_path)

    print("Getting indexes...\n")
    get_indexes()

    print("Crawler Started...\n")
    expected_total_documents, total_documents_crawled, failed_documents, missing_documents = crawl_data()
    total_documents_sent_for_parsing = total_documents_crawled

    print("===>Starting parser...\n")
    llama_parse_count, indexed_count, empty_files_count, documents_retried, documents_rescued_by_fallback, documents_failed_after_fallback, files_with_only_metadata, files_with_error_messages, files_with_empty_content, documents_sent_to_llamaparse_initial, successfully_parsed_documents, documents_not_sent_to_llamaparse = parse_files_to_md(last_data_json=last_data_json)

    print("\n--- Crawl and Parser Summary ---")
    print(f"Expected total documents: {expected_total_documents}")
    print(f"Total documents crawled: {total_documents_crawled}")
    print(f"Total documents failed during crawl: {len(failed_documents)}")
    print(f"Total documents missing: {len(missing_documents)}")

    if failed_documents:
        print("\n--- Failed Documents ---")
        for doc in failed_documents:
            print(f"  - ID: {doc['id']}, Reason: {doc['reason']}")

    if missing_documents:
        print("\n--- Missing Documents ---")
        for doc in missing_documents:
            print(f"  - ID: {doc['id']}, Reason: {doc['reason']}")

    print(f"Total documents sent for parsing: {total_documents_sent_for_parsing}")
    print(f"Documents sent to LlamaParse (initial attempts): {documents_sent_to_llamaparse_initial}")
    print(f"Documents not sent to LlamaParse: {len(documents_not_sent_to_llamaparse)}")
    if documents_not_sent_to_llamaparse:
        print("\n--- Documents Not Sent to LlamaParse ---")
        for doc in documents_not_sent_to_llamaparse:
            print(f"  - ID: {doc['id']}, Reason: {doc['reason']}")
    print(f"Documents sent to LlamaParse (including retries): {llama_parse_count}")
    print(f"Documents successfully parsed: {successfully_parsed_documents}")
    print(f"Documents successfully indexed: {indexed_count}")
    print(f"Documents that came back empty: {empty_files_count}")
    print(f"Documents retried: {documents_retried}")
    print(f"Documents rescued by fallback: {documents_rescued_by_fallback}")
    print(f"Documents failed after fallback: {documents_failed_after_fallback}")
    print(f"Files with only metadata: {files_with_only_metadata}")
    print(f"Files with error messages: {files_with_error_messages}")
    print(f"Files with empty content: {files_with_empty_content}")
    print("----------------------\n")

    print("===>Updating crawl timestamp...\n")
    update_crawl_timestamp(detail_json_path, DATA_PATH)

    print("===>Copying output CSV...\n")
    copy_output_csv(DATA_PATH, output_data_path)

    

    end_time = time.time()
    duration = end_time - start_time
    hours = int(duration // 3600)
    minutes = int((duration % 3600) // 60)
    seconds = int(duration % 60)

    print("===> Process completed\n")
    print(f"Total execution time: {hours} hours, {minutes} minutes, and {seconds} seconds")
    print ()


if __name__ == "__main__":
    main()