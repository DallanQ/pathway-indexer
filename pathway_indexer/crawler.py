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

        # --- TEMPORARY: FILTER FOR ACM LINKS FOR TESTING ---
        # To process only ACM links, the line below filters the DataFrame.
        # Remember to remove or comment this line out after testing!
        df = df[df["Role"] == "ACM"]
        # -------------------------------------------------

        # filter only the urls from whatsapp
        await crawl_csv(df=df, base_dir=DATA_PATH)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
