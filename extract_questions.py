import argparse
import os
import datetime
from dotenv import load_dotenv

from utils.langfuse_downloader import download_langfuse_data
from utils.langfuse_processor import process_langfuse_data

def main():
    load_dotenv()
    DATA_PATH = os.getenv("DATA_PATH")

    parser = argparse.ArgumentParser(description="Extract user questions from Langfuse data.")
    parser.add_argument("--days", type=int, default=7,
                        help="Number of past days to process Langfuse data for. Defaults to 7.")
    args = parser.parse_args()

    print("\n" + "="*60)
    print("*** LANGFUSE DATA EXTRACTION ***")
    print("="*60)

    try:
        langfuse_folder = os.path.join(DATA_PATH, "langfuse")

        print(f">>> Starting Langfuse data download (past {args.days} days)...")
        traces_csv, observations_csv = download_langfuse_data(langfuse_folder, days=args.days)

        print(">>> Processing Langfuse data to extract user inputs...")
        user_inputs_file = process_langfuse_data(traces_csv, observations_csv, langfuse_folder)

        print("\n[SUCCESS] Langfuse data extraction completed!")
        print(f"   >> Langfuse folder: {os.path.relpath(langfuse_folder, start=os.getcwd())}")

        if traces_csv:
            print(f"   >> Raw traces: {os.path.basename(traces_csv)}")
        else:
            print(f"   >> Raw traces: No traces file created")

        if observations_csv:
            print(f"   >> Raw observations: {os.path.basename(observations_csv)}")
        else:
            print(f"   >> Raw observations: No observations file created")

        print(f"   >> Extracted user inputs: {os.path.basename(user_inputs_file)}")

    except ImportError:
        print("[WARNING] Langfuse package not installed. Skipping Langfuse data extraction.")
        print("   To enable this feature, install: pip install langfuse")

    except Exception as e:
        print(f"[ERROR] Error during Langfuse data extraction: {e}")
        print("   Please check your Langfuse environment variables and network connection.")

    print("\n" + "="*60)
    print("*** EXTRACTION COMPLETE ***")
    print("="*60)

if __name__ == "__main__":
    main()
