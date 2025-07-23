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
    """Get the indexes from the websites and tag them with a role."""
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
    missionary_selectors = Selectors(
        header="h1",
        sub_header="h2",
        link="a",
        text="a > span",
    )

    HELP_SELECTOR = "#articleList"

    # --- Crawling Process ---

    # 1. Crawl ACM data
    acm_data = crawl_index(ACM_URL, acm_selectors)
    print("ACM data collected!")
    print(f"Length of ACM data: {len(acm_data)}")
    acm_data_with_role = [[*row, "ACM"] for row in acm_data]
    print()

    # 2. Crawl Missionary data
    missionary_data = crawl_index(MISSIONARY_URL, missionary_selectors)
    print("Missionary data collected!")
    print(f"Length of missionary data: {len(missionary_data)}")
    missionary_data_with_role = [[*row, "missionary"] for row in missionary_data[2:]]
    print()

    # 3. Crawl Help data
    help_data = asyncio.run(get_help_links(HELP_URL, HELP_SELECTOR))
    print("Help data collected!")
    print(f"Length of help data: {len(help_data)}")
    help_data_with_role = [[*row, "missionary"] for row in help_data]
    print()

    # 4. Crawl Student Services data
    student_services_data = asyncio.run(get_services_links(STUDENT_SERVICES_URL))
    print("Student Services data collected!")
    print(f"Length of Student Services data: {len(student_services_data)}")
    student_services_data_with_role = [[*row, "missionary"] for row in student_services_data]
    print()

    # --- Save the data ---
    csv_headers = ["Section", "Subsection", "Title", "URL", "filename", "Role"]

    # Write each index CSV now including 'filename'
    for path, rows in [
        (acm_path, acm_data_with_role),
        (missionary_path, missionary_data_with_role),
        (help_path, help_data_with_role),
        (student_services_path, student_services_data_with_role),
    ]:
        with open(path, "w", newline="", encoding="UTF-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(csv_headers)
            # for each row: append the computed filename at the end
            for row in rows:
                url = row[3]  # URL is the fourth element
                writer.writerow([*row, generate_hash_filename(url)])

    # --- Create the final dataframe ---
    print("Combining all data into final dataframe...")
    index_path = os.path.join(DATA_PATH, "index")

    # Load the data into DataFrames
    df_acm = pd.read_csv(f"{index_path}/acm.csv")
    df_missionary = pd.read_csv(f"{index_path}/missionary.csv")
    df_help = pd.read_csv(f"{index_path}/help.csv")
    df_student_services = pd.read_csv(f"{index_path}/student_services.csv")

    # Concatenate all
    df = pd.concat([df_acm, df_missionary, df_help, df_student_services], ignore_index=True)

    df.fillna("Missing", inplace=True)

    # Strip off anchors
    df["URL"] = df["URL"].str.split("#").str[0]

    # Group and keep first role
    df_merged = (
        df.groupby("URL")
        .agg({
            "Section": list,
            "Subsection": list,
            "Title": list,
            "Role": "first",
            "filename": "first",
        })
        .reset_index()
    )

    # Ensure our filename column is consistent
    df_merged["filename"] = df_merged["URL"].apply(generate_hash_filename)

    # Save including filename
    df_merged.to_csv(os.path.join(DATA_PATH, "all_links.csv"), index=False)

    print("\nAll data collected and saved!")
    print(f"All links saved in {os.path.join(DATA_PATH, 'all_links.csv')}")
    print("Process finished! Links ready to be crawled.")
    print()
