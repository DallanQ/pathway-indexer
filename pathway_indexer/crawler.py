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

        # --- TEMPORARY: LIMIT LINKS FOR TESTING ---
        # To process approximately one-third of the links, uncomment the line below.
        # Remember to remove or comment this line out after testing!
        df = df.head(len(df) // 3)
        # ------------------------------------------

        # filter only the urls from whatsapp
        await crawl_csv(df=df, base_dir=DATA_PATH)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
