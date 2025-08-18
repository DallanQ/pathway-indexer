"""
Langfuse Data Downloader

Downloads trace data from Langfuse for the past month and saves it to CSV format.
This script handles authentication, data retrieval, and proper error handling.
"""

import csv
import datetime
import json
import os
from typing import Any, Dict, List

try:
    from langfuse import Langfuse
except ImportError:
    print("Warning: langfuse package not installed. Run: pip install langfuse")
    exit(1)


def get_langfuse_client() -> Langfuse:
    """
    Initialize Langfuse client using environment variables.

    Returns:
        Langfuse: Authenticated Langfuse client
    """
    public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
    secret_key = os.getenv("LANGFUSE_SECRET_KEY")
    host = os.getenv("LANGFUSE_HOST")

    if not all([public_key, secret_key, host]):
        raise ValueError(
            "Missing Langfuse credentials. Ensure LANGFUSE_PUBLIC_KEY, "
            "LANGFUSE_SECRET_KEY, and LANGFUSE_HOST are set in .env file"
        )

    print(f"🔐 Connecting to Langfuse at {host}")
    return Langfuse(public_key=public_key, secret_key=secret_key, host=host)


def get_time_range(days: int = 30) -> tuple:
    """
    Generate ISO timestamp range for the past N days.

    Args:
        days: Number of days to look back (default: 30)

    Returns:
        tuple: (from_timestamp, to_timestamp) in ISO format
    """
    to_ts = datetime.datetime.utcnow()
    from_ts = to_ts - datetime.timedelta(days=days)

    from_iso = from_ts.isoformat() + "Z"
    to_iso = to_ts.isoformat() + "Z"

    print(f"📅 Fetching data from {from_ts.strftime('%Y-%m-%d')} to {to_ts.strftime('%Y-%m-%d')}")
    return from_iso, to_iso


def fetch_traces(langfuse_client: Langfuse, from_ts: str, to_ts: str) -> List[Dict[str, Any]]:
    """
    Fetch all traces within the specified time range.

    Args:
        langfuse_client: Authenticated Langfuse client
        from_ts: Start timestamp in ISO format
        to_ts: End timestamp in ISO format

    Returns:
        List[Dict]: List of trace data
    """
    all_traces = []
    page = 1
    limit = 100

    print("📥 Downloading traces from Langfuse...")

    while True:
        try:
            print(f"   Fetching page {page} (limit: {limit})")
            traces_page = langfuse_client.get_traces(from_timestamp=from_ts, to_timestamp=to_ts, limit=limit, page=page)

            if not traces_page.data:
                break

            all_traces.extend([trace.__dict__ for trace in traces_page.data])
            print(f"   Retrieved {len(traces_page.data)} traces (total: {len(all_traces)})")

            # Check if we've reached the end
            if len(traces_page.data) < limit:
                break

            page += 1

        except Exception as e:
            print(f"❌ Error fetching traces on page {page}: {e}")
            break

    print(f"✅ Successfully downloaded {len(all_traces)} traces")
    return all_traces


def fetch_observations_for_traces(langfuse_client: Langfuse, traces: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Fetch all observations (generations, spans) for the given traces.

    Args:
        langfuse_client: Authenticated Langfuse client
        traces: List of trace data

    Returns:
        List[Dict]: List of observation data with trace context
    """
    all_observations = []

    print("📥 Downloading observations from traces...")

    for i, trace in enumerate(traces, 1):
        trace_id = trace.get("id")
        if not trace_id:
            continue

        try:
            if i % 10 == 0:  # Progress indicator every 10 traces
                print(f"   Processing trace {i}/{len(traces)}")

            observations = langfuse_client.get_observations(trace_id=trace_id, limit=50)

            for obs in observations.data:
                obs_data = obs.__dict__
                # Add trace context to observation
                obs_data["trace_id"] = trace_id
                obs_data["trace_name"] = trace.get("name", "")
                obs_data["trace_user_id"] = trace.get("user_id", "")
                obs_data["trace_timestamp"] = trace.get("timestamp", "")
                all_observations.append(obs_data)

        except Exception as e:
            print(f"   Warning: Error fetching observations for trace {trace_id}: {e}")
            continue

    print(f"✅ Successfully downloaded {len(all_observations)} observations")
    return all_observations


def save_to_csv(data: List[Dict[str, Any]], output_path: str) -> None:
    """
    Save data to CSV file with proper encoding and error handling.

    Args:
        data: List of dictionaries to save
        output_path: Path to output CSV file
    """
    if not data:
        print("⚠️  No data to save")
        return

    # Get all unique keys from all dictionaries
    all_keys = set()
    for item in data:
        all_keys.update(item.keys())

    all_keys = sorted(list(all_keys))

    print(f"💾 Saving {len(data)} records to {output_path}")

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=all_keys)
            writer.writeheader()

            for item in data:
                # Convert complex objects to JSON strings
                row = {}
                for key in all_keys:
                    value = item.get(key, "")
                    if isinstance(value, (dict, list)):
                        row[key] = json.dumps(value, default=str)
                    else:
                        row[key] = str(value) if value is not None else ""
                writer.writerow(row)

        print(f"✅ Successfully saved data to {output_path}")

    except Exception as e:
        print(f"❌ Error saving to CSV: {e}")
        raise


def download_langfuse_data(output_folder: str, days: int = 30) -> tuple:
    """
    Main function to download Langfuse data for the past N days.

    Args:
        output_folder: Folder to save the CSV files
        days: Number of days to look back (default: 30)

    Returns:
        tuple: (traces_csv_path, observations_csv_path)
    """
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Generate filenames with today's date
    today = datetime.datetime.now().strftime("%m_%d_%y")
    traces_csv = os.path.join(output_folder, f"langfuse_traces_{today}.csv")
    observations_csv = os.path.join(output_folder, f"langfuse_observations_{today}.csv")

    try:
        # Initialize Langfuse client
        langfuse_client = get_langfuse_client()

        # Get time range
        from_ts, to_ts = get_time_range(days)

        # Fetch traces
        traces = fetch_traces(langfuse_client, from_ts, to_ts)

        # Fetch observations
        observations = fetch_observations_for_traces(langfuse_client, traces)

        # Save to CSV files
        save_to_csv(traces, traces_csv)
        save_to_csv(observations, observations_csv)

        print("\n🎉 Langfuse data download completed successfully!")
        print(f"   Traces saved to: {traces_csv}")
        print(f"   Observations saved to: {observations_csv}")

        return traces_csv, observations_csv

    except Exception as e:
        print(f"❌ Error during Langfuse data download: {e}")
        raise


if __name__ == "__main__":
    # For testing - use DATA_PATH from environment
    import dotenv

    dotenv.load_dotenv()

    data_path = os.getenv("DATA_PATH", "./data")
    langfuse_folder = os.path.join(data_path, "langfuse")

    download_langfuse_data(langfuse_folder)
