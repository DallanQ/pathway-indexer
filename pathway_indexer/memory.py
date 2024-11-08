import datetime
import json
import os
import shutil

import pandas as pd
from dotenv import load_dotenv

from utils.tools import create_folder, generate_hash_filename

load_dotenv()
DATA_PATH = os.getenv("DATA_PATH")


def initialize_json_file(detail_json_path, output_data_path):
    """Initialize the JSON file if it doesn't exist, otherwise load it."""
    if not os.path.exists(detail_json_path):
        if os.path.exists(output_data_path):
            os.remove(output_data_path)
        os.makedirs(os.path.dirname(detail_json_path), exist_ok=True)
        last_data_json = {"last_crawl_detail": "Never", "last_folder_crawl": "Never"}
        with open(detail_json_path, "w") as file:
            json.dump(last_data_json, file)
    else:
        with open(detail_json_path) as file:
            last_data_json = json.load(file)
    return last_data_json


def update_crawl_timestamp(detail_json, data_path):
    """Update the last crawl timestamp in the JSON file."""
    current_time = datetime.datetime.now().isoformat()
    os.makedirs(os.path.dirname(detail_json), exist_ok=True)
    with open(detail_json, "w", encoding="UTF-8") as timefile:
        json.dump({"last_crawl_detail": current_time, "last_folder_crawl": data_path}, timefile)


def copy_output_csv(data_path, output_data_path):
    """Copy output CSV file to the specified location."""
    shutil.copyfile(os.path.join(data_path, "output_data.csv"), output_data_path)


# Mock functions for generating data
def generate_data_frame():
    """Generate a DataFrame with the required data structure."""
    data = {
        "Section": ["Section1", "Section1", "Section2", "Section2"],
        "Subsection": ["Sub1", "Sub1", "Sub2", "Sub2"],
        "Title": ["Title1", "Title2", "Title3", "Title4"],
        "URL": [
            "http://127.0.0.1:5500/pathway_indexer/test1.html",
            "http://127.0.0.1:5500/pathway_indexer/test2.html",
            "http://127.0.0.1:5500/pathway_indexer/test3.html",
            "http://127.0.0.1:5500/pathway_indexer/test4.html",
        ],
    }
    return pd.DataFrame(data)


def save_links_csv(df, data_path):
    """Process and save DataFrame to CSV."""
    create_folder(data_path)
    df_merged = (
        df.groupby("URL")
        .agg({
            "Section": list,
            "Subsection": list,
            "Title": list,
        })
        .reset_index()
    )
    df_merged["filename"] = df_merged["URL"].apply(generate_hash_filename)
    df_merged.to_csv(os.path.join(data_path, "all_links.csv"), index=False)
