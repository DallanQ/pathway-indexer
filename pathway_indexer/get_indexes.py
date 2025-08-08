import asyncio
import csv
import os

import dotenv
import pandas as pd

from utils.indexes import (
    Selectors,
    crawl_index,
    create_root_folders,
    get_help_links,
    get_services_links,
)
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
    HELP_URL = "https://help.byupathway.edu/knowledgebase/"
    STUDENT_SERVICES_URL = "https://student-services.catalog.prod.coursedog.com/"

    acm_path = f"{DATA_PATH}/index/acm.csv"
    missionary_path = f"{DATA_PATH}/index/missionary.csv"
    help_path = f"{DATA_PATH}/index/help.csv"
    student_services_path = f"{DATA_PATH}/index/student_services.csv"

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

    HELP_SELECTOR = "#articleList"

    # Crawling Process
    # TEST_COMMENT: acm_data = crawl_index(ACM_URL, acm_selectors)
    # TEST_COMMENT: print("ACM data collected!")
    # TEST_COMMENT: print(f"Length of acm data: {len(acm_data)}")
    # TEST_COMMENT: print()

    # TEST_COMMENT: missionary_data = crawl_index(MISSIONARY_URL, missionary_selectors)
    # TEST_COMMENT: print("Missionary data collected!")
    # TEST_COMMENT: print(f"Length of missionary data: {len(missionary_data)}")
    # TEST_COMMENT: print()

    # TEST_COMMENT: help_data = asyncio.run(get_help_links(HELP_URL, HELP_SELECTOR))
    # TEST_COMMENT: print("Help data collected!")
    # TEST_COMMENT: print(f"Length of help data: {len(help_data)}")
    # TEST_COMMENT: print()

    student_services_data = asyncio.run(get_services_links(STUDENT_SERVICES_URL))
    print("Student Services data collected!")
    print(f"Length of Student Services data: {len(student_services_data)}")
    print()

    # Save the data
    # TEST_COMMENT: with open(acm_path, "w", newline="", encoding="UTF-8") as csvfile:
    # TEST_COMMENT:     writer = csv.writer(csvfile)
    # TEST_COMMENT:     writer.writerow(["Section", "Subsection", "Title", "URL", "Role"])
    # TEST_COMMENT:     for row in acm_data:
    # TEST_COMMENT:         writer.writerow([*row, "ACM"])

    # TEST_COMMENT: with open(missionary_path, "w", newline="", encoding="UTF-8") as csvfile:
    # TEST_COMMENT:     writer = csv.writer(csvfile)
    # TEST_COMMENT:     # write headers
    # TEST_COMMENT:     writer.writerow(["Section", "Subsection", "Title", "URL", "Role"])
    # TEST_COMMENT:     for row in missionary_data[2:]:
    # TEST_COMMENT:         writer.writerow([*row, "missionary"])

    # TEST_COMMENT: with open(help_path, "w", newline="", encoding="UTF-8") as csvfile:
    # TEST_COMMENT:     writer = csv.writer(csvfile)
    # TEST_COMMENT:     writer.writerow(["Section", "Subsection", "Title", "URL", "Role"])
    # TEST_COMMENT:     for row in help_data:
    # TEST_COMMENT:         writer.writerow([*row, "missionary"])

    with open(student_services_path, "w", newline="", encoding="UTF-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Section", "Subsection", "Title", "URL", "Role"])
        for row in student_services_data:
            writer.writerow([*row, "missionary"])

    # *****Create the final dataframe*****

    index_path = os.path.join(DATA_PATH, "index")

    # Load the data into Dataframes
    # TEST_COMMENT: df = pd.read_csv(f"{index_path}/acm.csv")
    # TEST_COMMENT: df2 = pd.read_csv(f"{index_path}/missionary.csv")
    # TEST_COMMENT: df3 = pd.read_csv(f"{index_path}/help.csv")
    df4 = pd.read_csv(f"{index_path}/student_services.csv")

    df = pd.concat([df4], ignore_index=True)

    df.fillna("Missing", inplace=True)

    # remove from the urls, the # and everything after it
    df["URL"] = df["URL"].str.split("#").str[0]

    df_merged = (
        df.groupby("URL")
        .agg({
            "Section": list,
            "Subsection": list,
            "Title": list,
            "Role": "first",
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
