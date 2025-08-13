import asyncio
import os

import dotenv
import pandas as pd

from utils.crawl import crawl_csv

dotenv.load_dotenv()


def crawl_data():
    """Crawl the data from the csv file."""
    # load the path
    DATA_PATH = os.getenv("DATA_PATH")
    # crawl_path = os.path.join(DATA_PATH, "crawl")

    async def main():
        """Crawl the index and get the data."""
        df = pd.read_csv(os.path.join(DATA_PATH, "all_links.csv"))
        # filter only the urls from whatsapp
        expected_total_documents, total_documents_crawled, failed_documents, missing_documents = await crawl_csv(df=df, base_dir=DATA_PATH)
        return expected_total_documents, total_documents_crawled, failed_documents, missing_documents

    loop = asyncio.get_event_loop()
    expected_total_documents, total_documents_crawled, failed_documents, missing_documents = loop.run_until_complete(main())
    return expected_total_documents, total_documents_crawled, failed_documents, missing_documents