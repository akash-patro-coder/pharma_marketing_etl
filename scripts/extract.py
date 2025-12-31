import pandas as pd
import os
import shutil
import logging
from typing import Dict, List

# Setup logging config - good for production-style tracking
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Configuration: Easier to change this list later if requirements change
REQUIRED_FILES = [
    'brands.csv',
    'campaigns.csv',
    'channel_performance.csv',
    'channels.csv',
    'hcp_engagements.csv',
    'website_metrics.csv'
]

def get_project_root() -> str:
    """Helper to find the project root dynamically."""
    current_script = os.path.abspath(__file__)
    # Go up two levels: scripts -> pharma_marketing_etl
    return os.path.dirname(os.path.dirname(current_script))

def extract_data(raw_folder: str) -> Dict[str, pd.DataFrame]:
    """
    Validation and Staging Step.
    Checks for required files in raw/, logs their counts, and moves them to staging.
    """
    
    # We infer the staging path relative to the raw path to keep it dynamic
    staging_folder = raw_folder.replace('raw', 'extractRawFiles')
    
    # Ensure staging exists, just in case
    os.makedirs(staging_folder, exist_ok=True)
    
    extracted_dfs = {}

    print(f"\n--- Starting Extraction Phase ---")
    print(f"Reading from: {raw_folder}")
    print(f"Staging to:   {staging_folder}\n")

    for filename in REQUIRED_FILES:
        src_path = os.path.join(raw_folder, filename)
        dest_path = os.path.join(staging_folder, filename)

        if not os.path.exists(src_path):
            logging.warning(f"⚠️  MISSING: {filename} not found.")
            continue

        try:
            # 1. Read Data
            df = pd.read_csv(src_path)
            
            # 2. Audit/Log
            record_count = df.shape[0]
            logging.info(f"✔  EXTRACTED: {filename:<25} | Records: {record_count}")

            # 3. Stage File (Copying is safer than moving for raw data)
            shutil.copy2(src_path, dest_path)

            # 4. Store for downstream usage if needed
            extracted_dfs[filename] = df

        except Exception as e:
            logging.error(f"❌ FAILED: Could not process {filename}. Error: {str(e)}")

    print(f"--- Extraction Complete ---\n")
    return extracted_dfs

if __name__ == "__main__":
    # Robust path handling so this runs from anywhere
    root_dir = get_project_root()
    raw_data_path = os.path.join(root_dir, 'data', 'raw')
    
    extract_data(raw_data_path)