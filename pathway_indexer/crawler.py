import os
import asyncio
import dotenv
import pandas as pd

from utils.crawl import (
  crawl_csv
)

dotenv.load_dotenv()

def craw_data():
    """Crawl the data from the csv file."""
    #load the path
    DATA_PATH = os.getenv("DATA_PATH")
    # crawl_path = os.path.join(DATA_PATH, "crawl")

    async def main():
        """Crawl the index and get the data."""
        df = pd.read_csv(os.path.join(DATA_PATH, "all_links.csv"))
        # filter only the urls from whatsapp
        await crawl_csv(df, DATA_PATH)

    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())


craw_data()
