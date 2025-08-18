"""
Langfuse Data Processor

Extracts all user inputs from Langfuse CSV data and saves them to a clean text file.
Simple extraction - gets all inputs from specified columns without complex filtering.
"""

import csv
import os
import datetime
from typing import List


def extract_user_inputs_from_csv(csv_path: str, input_columns: List[str] = None) -> List[str]:
    """
    Extract all user inputs from Langfuse CSV file.
    
    Args:
        csv_path: Path to the CSV file
        input_columns: List of column names to extract from (default: ['input'])
        
    Returns:
        List[str]: List of all user inputs found
    """
    if input_columns is None:
        input_columns = ['input']
    
    user_inputs = []
    
    if not os.path.exists(csv_path):
        print(f"⚠️  CSV file not found: {csv_path}")
        return user_inputs
    
    print(f"📖 Processing {csv_path}")
    
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            for column in input_columns:
                input_text = row.get(column, "").strip()
                if input_text:
                    user_inputs.append(input_text)
    
    return user_inputs


def process_langfuse_data(traces_csv: str, observations_csv: str, output_folder: str) -> str:
    """
    Process Langfuse CSV files and extract all user inputs.
    
    Args:
        traces_csv: Path to traces CSV file
        observations_csv: Path to observations CSV file
        output_folder: Folder to save the output text file
        
    Returns:
        str: Path to the output text file
    """
    all_user_inputs = []
    
    print(">>> Extracting user inputs from Langfuse data...")
    
    # Process traces - look for 'input' column
    if os.path.exists(traces_csv):
        print(f"   Processing traces: {traces_csv}")
        trace_inputs = extract_user_inputs_from_csv(traces_csv, ['input'])
        all_user_inputs.extend(trace_inputs)
        print(f"   Found {len(trace_inputs)} user inputs in traces")
    
    # Process observations - look for 'input' column  
    if os.path.exists(observations_csv):
        print(f"   Processing observations: {observations_csv}")
        obs_inputs = extract_user_inputs_from_csv(observations_csv, ['input'])
        all_user_inputs.extend(obs_inputs)
        print(f"   Found {len(obs_inputs)} user inputs in observations")
    
    # Generate output filename with today's date
    today = datetime.datetime.now().strftime("%m_%d_%y")
    output_file = os.path.join(output_folder, f"extracted_user_inputs_{today}.txt")
    
    # Save to text file
    os.makedirs(output_folder, exist_ok=True)
    
    print(f">>> Saving {len(all_user_inputs)} user inputs to {output_file}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for user_input in all_user_inputs:
            f.write(f"{user_input}\n")
    
    print(f"[SUCCESS] User inputs extracted successfully!")
    print(f"   Total user inputs saved: {len(all_user_inputs)}")
    print(f"   Output file: {output_file}")
    
    return output_file


if __name__ == "__main__":
    # For testing
    import dotenv
    dotenv.load_dotenv()
    
    data_path = os.getenv("DATA_PATH", "./data")
    langfuse_folder = os.path.join(data_path, "langfuse")
    
    # Generate expected filenames
    today = datetime.datetime.now().strftime("%m_%d_%y")
    traces_csv = os.path.join(langfuse_folder, f"langfuse_traces_{today}.csv")
    observations_csv = os.path.join(langfuse_folder, f"langfuse_observations_{today}.csv")
    
    process_langfuse_data(traces_csv, observations_csv, langfuse_folder)
