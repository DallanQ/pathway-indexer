import os
import csv
import pandas as pd
import dotenv

from utils.indexes import (
  Selectors,
  crawl_index,
  create_root_folders,
  get_soup_content,
  get_handbook_data
)

from utils.tools import generate_hash_filename

dotenv.load_dotenv()

# first, create the necessary folders
DATA_PATH = os.getenv("DATA_PATH")
print(DATA_PATH)

create_root_folders(DATA_PATH)

# General Variables
ACM_URL = "https://missionaries.prod.byu-pathway.psdops.com/ACC-site-index"
MISSIONARY_URL = "https://missionaries.prod.byu-pathway.psdops.com/missionary-services-site-index"
HANDBOOK_URL = "https://www.byupathway.edu/policies/handbook/"
acm_path = f"{DATA_PATH}/index/acm.csv"
missionary_path = f"{DATA_PATH}/index/missionary.csv"
handbook_path = f"{DATA_PATH}/index/handbook.csv"

# Selectors
acm_selectors = Selectors(
    header='span[style="font-size:18.0pt"]',
    sub_header="b > i",
    link="a",
    text="a > span",
)

#! THERE WAS CHANGES IN THE MISSIONARY SELECTORS
missionary_selectors = Selectors(
    header="b > span",
    sub_header='span[style="font-size:16.0pt;line-height:150%"]',
    link="a",
    text="a > span",
)

HANDNOOK_SELECTOR = "bsp-book>ul>li>bsp-chapter>ul>li>bsp-chapter"



# Crawling Process
acm_data = crawl_index(ACM_URL, acm_selectors)
print("Acm data collected!")
print(f"Lenght of acm data: {len(acm_data)}")
print()

missionary_data = crawl_index(MISSIONARY_URL, missionary_selectors)
print("Missionary data collected!")
print(f"Lenght of missionary data: {len(missionary_data)}")
print()

handbook_soup = get_soup_content(HANDBOOK_URL)
handbook_data = get_handbook_data(handbook_soup, HANDNOOK_SELECTOR)
print("Handbook data collected!")
print(f"Lenght of handbook data: {len(handbook_data)}")
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

# to save the handbook data we need to convert it to dataframe
handbook_df = pd.DataFrame(handbook_data, columns=["Section", "Title", "URL"])
handbook_df.insert(1, "Subsection", "")
handbook_df = handbook_df[~handbook_df["URL"].str.contains("#")]

handbook_df.to_csv(handbook_path, index=False)


# *****Create the final dataframe*****

index_path = os.path.join(DATA_PATH, "index")

# Load the data into Dataframes
df = pd.read_csv(f"{index_path}/acm.csv")
df2 = pd.read_csv(f"{index_path}/handbook.csv")
df3 = pd.read_csv(f"{index_path}/missionary.csv")

df = pd.concat([df, df2, df3], ignore_index=True)

df.fillna("Missing", inplace=True)

df_merged = (
    df.groupby("URL")
    .agg(
        {
            "Section": list,
            "Subsection": list,
            "Title": list,
        }
    )
    .reset_index()
)

## add a final column with the hash filename
df_merged["filename"] = df_merged["URL"].apply(generate_hash_filename)
# save the files as "all_links.csv"
df_merged.to_csv(f"{DATA_PATH}/all_links.csv", index=False)

print("All data collected and saved!")
print("All links saved in all_links.csv")
print("Process finished!")
