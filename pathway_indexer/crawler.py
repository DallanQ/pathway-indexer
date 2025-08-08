import asyncio
import os

import dotenv
import pandas as pd

from utils.crawl import crawl_csv

dotenv.load_dotenv()


def crawl_data(stats, detailed_log_path):
    """Crawl the data from the csv file."""
    # load the path
    DATA_PATH = os.getenv("DATA_PATH")
    # crawl_path = os.path.join(DATA_PATH, "crawl")

    async def main():
        """Crawl the index and get the data."""
        df = pd.read_csv(os.path.join(DATA_PATH, "all_links.csv"))
        stats["total_documents_crawled"] = len(df)
        # filter only the urls from whatsapp
        await crawl_csv(df=df, base_dir=DATA_PATH, detailed_log_path=detailed_log_path)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())