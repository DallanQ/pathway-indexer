"""
Langfuse Data Processor

Extracts user inputs from Langfuse CSV data, including metadata, and saves them to a structured CSV file.
"""

import csv
import datetime
import json
import os
from typing import Any, Dict, List


def extract_user_inputs_from_csv(csv_path: str, input_columns: List[str] = None) -> List[Dict[str, Any]]:
    """
    Extract user inputs and metadata from a Langfuse CSV file.

    Args:
        csv_path: Path to the CSV file.
        input_columns: List of column names to extract from (default: ['input']).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing the user input and associated metadata.
    """
    if input_columns is None:
        input_columns = ["input"]

    user_inputs = []

    if not os.path.exists(csv_path):
        print(f"[WARNING] CSV file not found: {csv_path}")
        return user_inputs

    print(f">>> Processing {csv_path}")

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            for column in input_columns:
                value = row.get(column, "")
                if value:
                    input_text = str(value).strip()
                    if input_text:
                        # Extract metadata
                        metadata_str = row.get("metadata", "{}")
                        metadata = {}
                        try:
                            metadata = json.loads(metadata_str) if metadata_str else {}
                        except json.JSONDecodeError:
                            print(f"[WARNING] Could not decode metadata JSON: {metadata_str}")

                        # Extract desired fields
                        timestamp = row.get("timestamp", "")
                        country = metadata.get("country", "")
                        user_language = metadata.get("user_language", "")
                        state = metadata.get("state", "")

                        user_inputs.append({
                            "Question": input_text,
                            "Date": timestamp,
                            "Country": country,
                            "User Language": user_language,
                            "State": state,
                        })

    return user_inputs


def process_langfuse_data(traces_csv: str, observations_csv: str, output_folder: str) -> str:
    """
    Process Langfuse CSV files and extract user inputs with metadata.

    Args:
        traces_csv: Path to traces CSV file.
        observations_csv: Path to observations CSV file.
        output_folder: Folder to save the output CSV file.

    Returns:
        str: Path to the output CSV file.
    """
    all_user_inputs = []

    print(">>> Extracting user inputs from Langfuse data...")

    # Process traces
    if traces_csv and os.path.exists(traces_csv):
        print(f"   Processing traces: {traces_csv}")
        trace_inputs = extract_user_inputs_from_csv(traces_csv, ["input"])
        all_user_inputs.extend(trace_inputs)
        print(f"   Found {len(trace_inputs)} user inputs in traces")
    else:
        print("   No traces CSV file to process")

    # Process observations
    if observations_csv and os.path.exists(observations_csv):
        print(f"   Processing observations: {observations_csv}")
        obs_inputs = extract_user_inputs_from_csv(observations_csv, ["input"])
        all_user_inputs.extend(obs_inputs)
        print(f"   Found {len(obs_inputs)} user inputs in observations")
    else:
        print("   No observations CSV file to process")

    # Generate output filename with today's date
    today = datetime.datetime.now().strftime("%m_%d_%y")
    output_file = os.path.join(output_folder, f"extracted_user_inputs_{today}.csv")

    # Save to CSV file
    os.makedirs(output_folder, exist_ok=True)

    print(f">>> Saving {len(all_user_inputs)} user inputs to {output_file}")

    if not all_user_inputs:
        print("[WARNING] No user inputs to save.")
        return output_file

    fieldnames = ["Date", "Country", "User Language", "State", "Question"]

    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_user_inputs)

    print("[SUCCESS] User inputs extracted successfully!")
    print(f"   Total user inputs saved: {len(all_user_inputs)}")
    print(f"   Output file: {output_file}")

    return output_file
