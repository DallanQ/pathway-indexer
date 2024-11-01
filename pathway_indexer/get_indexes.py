import csv
import os

import dotenv
import pandas as pd

from utils.indexes import Selectors, crawl_index, create_root_folders
from utils.tools import generate_hash_filename

dotenv.load_dotenv()


def get_indexes():
    """Get the indexes from the websites."""
    # first, create the necessary folders
    DATA_PATH = os.getenv("DATA_PATH")
    print(DATA_PATH)

    create_root_folders(DATA_PATH)

    # General Variables
    ACM_URL = "https://missionaries.prod.byu-pathway.psdops.com/ACC-site-index"
    MISSIONARY_URL = "https://missionaries.prod.byu-pathway.psdops.com/missionary-services-site-index"
    acm_path = f"{DATA_PATH}/index/acm.csv"
    missionary_path = f"{DATA_PATH}/index/missionary.csv"

    # Selectors
    acm_selectors = Selectors(
        header='span[style="font-size:18.0pt"]',
        sub_header="b > i",
        link="a",
        text="a > span",
    )

    #! THERE WAS CHANGES IN THE MISSIONARY SELECTORS
    missionary_selectors = Selectors(
        header="h1",
        sub_header="h2",
        link="a",
        text="a > span",
    )

    # Crawling Process
    acm_data = crawl_index(ACM_URL, acm_selectors)
    print("Acm data collected!")
    print(f"Lenght of acm data: {len(acm_data)}")
    print()

    missionary_data = crawl_index(MISSIONARY_URL, missionary_selectors)
    print("Missionary data collected!")
    print(f"Lenght of missionary data: {len(missionary_data)}")
    print()

    # Save the data
    with open(acm_path, "w", newline="", encoding="UTF-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Section", "Subsection", "Title", "URL"])
        writer.writerows(acm_data)

    with open(missionary_path, "w", newline="", encoding="UTF-8") as csvfile:
        writer = csv.writer(csvfile)
        # write headers
        writer.writerow(["Section", "Subsection", "Title", "URL"])
        writer.writerows(missionary_data[2:])

    # *****Create the final dataframe*****

    index_path = os.path.join(DATA_PATH, "index")

    # Load the data into Dataframes
    df = pd.read_csv(f"{index_path}/acm.csv")
    df2 = pd.read_csv(f"{index_path}/missionary.csv")

    df = pd.concat([df, df2], ignore_index=True)

    df.fillna("Missing", inplace=True)

    df_merged = (
        df.groupby("URL")
        .agg({
            "Section": list,
            "Subsection": list,
            "Title": list,
        })
        .reset_index()
    )

    ## add a final column with the hash filename
    df_merged["filename"] = df_merged["URL"].apply(generate_hash_filename)
    # save the files as "all_links.csv"
    df_merged.to_csv(os.path.join(DATA_PATH, "all_links.csv"), index=False)

    print("All data collected and saved!")
    print(f"All links saved in {DATA_PATH}/all_links.csv")
    print("Process finished! Links ready to be crawled.")
    print()
