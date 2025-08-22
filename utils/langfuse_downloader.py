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
    print("Warning: langfuse package not installed. Run: poetry add langfuse")
    raise ImportError("langfuse package not installed. Run: poetry add langfuse")


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

    print(f">>> Connecting to Langfuse at {host}")
    return Langfuse(public_key=public_key, secret_key=secret_key, host=host)


def get_time_range(days: int = 30) -> tuple:
    """
    Generate datetime range for the past N days.

    Args:
        days: Number of days to look back (default: 30)

    Returns:
        tuple: (from_timestamp, to_timestamp) as datetime objects
    """
    to_ts = datetime.datetime.utcnow()
    from_ts = to_ts - datetime.timedelta(days=days)

    print(f">>> Fetching data from {from_ts.strftime('%Y-%m-%d')} to {to_ts.strftime('%Y-%m-%d')}")
    return from_ts, to_ts


def fetch_traces(langfuse_client: Langfuse, from_ts: datetime.datetime, to_ts: datetime.datetime) -> List[Dict[str, Any]]:
    """
    Fetch all traces within the specified time range.

    Args:
        langfuse_client: Authenticated Langfuse client
        from_ts: Start timestamp as datetime object
        to_ts: End timestamp as datetime object

    Returns:
        List[Dict]: List of trace data
    """
    all_traces = []
    page = 1
    limit = 100

    print(">>> Downloading traces from Langfuse...")

    while True:
        try:
            print(f"   Fetching page {page} (limit: {limit})")
            traces_page = langfuse_client.api.trace.list(
                from_timestamp=from_ts, 
                to_timestamp=to_ts, 
                limit=limit, 
                page=page
            )

            if not traces_page.data:
                break

            all_traces.extend([trace.__dict__ for trace in traces_page.data])
            print(f"   Retrieved {len(traces_page.data)} traces (total: {len(all_traces)})")

            # Check if we've reached the end
            if len(traces_page.data) < limit:
                break

            page += 1

        except Exception as e:
            print(f"[ERROR] Error fetching traces on page {page}: {e}")
            break

    print(f"[SUCCESS] Successfully downloaded {len(all_traces)} traces")
    return all_traces


def fetch_scores(langfuse_client: Langfuse) -> List[Dict[str, Any]]:
    """
    Fetch all scores (including user feedback) from Langfuse.

    Args:
        langfuse_client: Authenticated Langfuse client

    Returns:
        List[Dict]: List of score data
    """
    all_scores = []
    page = 1
    limit = 100

    print(">>> Downloading scores from Langfuse...")

    while True:
        try:
            print(f"   Fetching scores page {page} (limit: {limit})")
            scores_page = langfuse_client.api.score_v_2.get(
                limit=limit, 
                page=page
            )

            if not scores_page.data:
                break

            all_scores.extend([score.__dict__ for score in scores_page.data])
            print(f"   Retrieved {len(scores_page.data)} scores (total: {len(all_scores)})")

            # Check if we've reached the end
            if len(scores_page.data) < limit:
                break

            page += 1

        except Exception as e:
            print(f"[ERROR] Error fetching scores on page {page}: {e}")
            break

    print(f">>> Downloaded {len(all_scores)} total scores")
    return all_scores


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

    print(">>> Downloading observations from traces...")

    for i, trace in enumerate(traces, 1):
        trace_id = trace.get("id")
        if not trace_id:
            continue

        try:
            if i % 10 == 0:  # Progress indicator every 10 traces
                print(f"   Processing trace {i}/{len(traces)}")

            observations = langfuse_client.api.observations.get_many(trace_id=trace_id, limit=50)

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

    print(f"[SUCCESS] Successfully downloaded {len(all_observations)} observations")
    return all_observations


def add_user_feedback_to_traces(traces: List[Dict[str, Any]], scores: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Add user_feedback column to traces by merging score data.

    Args:
        traces: List of trace dictionaries
        scores: List of score dictionaries

    Returns:
        List[Dict]: Traces with user_feedback column added
    """
    print(">>> Adding user feedback to traces...")
    
    # Create a mapping from trace_id to user feedback scores
    feedback_map = {}
    for score in scores:
        if score.get('name') == 'user_feedback' and score.get('trace_id'):
            trace_id = score['trace_id']
            feedback_value = score.get('string_value', '')
            comment = score.get('comment', '')
            
            # Combine feedback value and comment if both exist
            if feedback_value and comment:
                feedback_text = f"{feedback_value}: {comment}"
            elif feedback_value:
                feedback_text = feedback_value
            elif comment:
                feedback_text = comment
            else:
                feedback_text = ""
            
            # If multiple feedback scores exist for same trace, concatenate them
            if trace_id in feedback_map:
                feedback_map[trace_id] += f" | {feedback_text}"
            else:
                feedback_map[trace_id] = feedback_text

    # Add user_feedback column to each trace
    feedback_added = 0
    for trace in traces:
        trace_id = trace.get('id', '')
        trace['user_feedback'] = feedback_map.get(trace_id, '')
        if feedback_map.get(trace_id):
            feedback_added += 1

    print(f"   Added user feedback to {feedback_added}/{len(traces)} traces")
    return traces


def save_to_csv(data: List[Dict[str, Any]], output_path: str) -> bool:
    """
    Save data to CSV file with proper encoding and error handling.

    Args:
        data: List of dictionaries to save
        output_path: Path to output CSV file
        
    Returns:
        bool: True if file was saved, False if no data to save
    """
    if not data:
        print("[WARNING] No data to save")
        return False

    # Get all unique keys from all dictionaries
    all_keys = set()
    for item in data:
        all_keys.update(item.keys())

    all_keys = sorted(list(all_keys))

    print(f">>> Saving {len(data)} records to {output_path}")

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

        print(f"[SUCCESS] Successfully saved data to {output_path}")
        return True

    except Exception as e:
        print(f"[ERROR] Error saving to CSV: {e}")
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

        # Fetch scores (including user feedback)
        scores = fetch_scores(langfuse_client)

        # Add user feedback to traces
        traces = add_user_feedback_to_traces(traces, scores)

        # Fetch observations
        observations = fetch_observations_for_traces(langfuse_client, traces)

        # Save to CSV files
        traces_saved = save_to_csv(traces, traces_csv)
        observations_saved = save_to_csv(observations, observations_csv)

        print("\n[SUCCESS] Langfuse data download completed successfully!")
        if traces_saved:
            print(f"   Traces saved to: {traces_csv}")
        else:
            print(f"   No traces to save (file not created)")
            
        if observations_saved:
            print(f"   Observations saved to: {observations_csv}")
        else:
            print(f"   No observations to save (file not created)")

        return traces_csv if traces_saved else None, observations_csv if observations_saved else None

    except Exception as e:
        print(f"[ERROR] Error during Langfuse data download: {e}")
        raise


if __name__ == "__main__":
    # For testing - use DATA_PATH from environment
    import dotenv

    dotenv.load_dotenv()

    data_path = os.getenv("DATA_PATH", "./data")
    langfuse_folder = os.path.join(data_path, "langfuse")

    download_langfuse_data(langfuse_folder)
