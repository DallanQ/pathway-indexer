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
    crawl_data()

    print("===>Starting parser...\n")
    llama_parse_count, indexed_count = parse_files_to_md(last_data_json=last_data_json)

    print("\n--- Parser Summary ---")
    print(f"Documents sent to LlamaParse: {llama_parse_count}")
    print(f"Documents successfully indexed: {indexed_count}")
    print("----------------------\n")

    print("===>Updating crawl timestamp...\n")
    update_crawl_timestamp(detail_json_path, DATA_PATH)

    print("===>Copying output CSV...\n")
    copy_output_csv(DATA_PATH, output_data_path)

    end_time = time.time()
    duration = end_time - start_time
    minutes = int(duration // 60)
    seconds = int(duration % 60)

    print("===> Process completed\n")
    print(f"Total execution time: {minutes} minutes and {seconds} seconds")


if __name__ == "__main__":
    main()
