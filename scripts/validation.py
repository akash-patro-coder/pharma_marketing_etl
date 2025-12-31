import pandas as pd
import logging
import os

# Setup simple logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def validate_data(processed_dfs: dict, project_root_path: str) -> dict:
    """
    Performs data quality checks on transformed dataframes.
    Returns the dictionary if valid, raises error if critical checks fail.
    """
    print("--- Starting Data Quality Validation ---")
    
    # 1. Setup Paths correctly
    staging_path = os.path.join(project_root_path, 'data', 'extractRawFiles')
    
    # 2. Extract DataFrames
    df_campaigns = processed_dfs.get('fact_campaign_performance')
    df_hcp = processed_dfs.get('fact_hcp_engagement')
    
    # LOAD RAW FILES FOR VALIDATION (using the corrected path)
    try:
        df_brands = pd.read_csv(os.path.join(staging_path, 'brands.csv'))
        df_channels = pd.read_csv(os.path.join(staging_path, 'channels.csv'))
    except FileNotFoundError as e:
        logging.error(f"Validation failed: Could not load raw reference files. {e}")
        raise e
    
    is_valid = True

    # ---------------------------------------------------------
    # CHECK 1: NO NULL PRIMARY KEYS
    # ---------------------------------------------------------
    pk_checks = [
        (df_campaigns, 'campaign_id', 'Campaigns'),
        (df_hcp, 'engagement_id', 'HCP Engagements'),
        (df_brands, 'brand_id', 'Brands')
    ]

    for df, pk_col, name in pk_checks:
        if df is not None and df[pk_col].isnull().any():
            logging.error(f"FAIL: Found NULL Primary Keys in {name}")
            is_valid = False
        else:
            logging.info(f"PASS: No NULL keys in {name}")

    # ---------------------------------------------------------
    # CHECK 2: VALID ENUM VALUES
    # ---------------------------------------------------------
    valid_statuses = ['planned', 'active', 'completed']
    invalid_status = df_campaigns[~df_campaigns['status'].isin(valid_statuses)]
    if not invalid_status.empty:
        # Warning only, don't stop pipeline
        logging.warning(f"FAIL: Found invalid statuses: {invalid_status['status'].unique()}")
    else:
        logging.info("PASS: All campaign statuses are valid.")

    valid_channel_types = ['digital', 'traditional']
    invalid_channels = df_channels[~df_channels['channel_type'].isin(valid_channel_types)]
    if not invalid_channels.empty:
        logging.warning(f"FAIL: Found invalid channel types: {invalid_channels['channel_type'].unique()}")
    else:
        logging.info("PASS: All channel types are valid.")

    # ---------------------------------------------------------
    # CHECK 3: FOREIGN KEY CONSISTENCY
    # ---------------------------------------------------------
    valid_brand_ids = df_brands['brand_id'].unique()
    orphaned_campaigns = df_campaigns[~df_campaigns['brand_id'].isin(valid_brand_ids)]
    
    if not orphaned_campaigns.empty:
        logging.error(f"FAIL: {len(orphaned_campaigns)} campaigns linked to non-existent Brand IDs")
        is_valid = False
    else:
        logging.info("PASS: All Foreign Keys (Brand IDs) match.")

    # ---------------------------------------------------------
    # CHECK 4: LOGICAL RANGES
    # ---------------------------------------------------------
    if (df_hcp['engagement_duration_sec'] <= 0).any():
        logging.warning("FAIL: Found interactions with 0 or negative duration")
        is_valid = False
    else:
        logging.info("PASS: All engagement durations are positive.")

    df_channel_agg = processed_dfs.get('agg_channel_effectiveness')
    if df_channel_agg is not None:
        invalid_ctr = df_channel_agg[
            (df_channel_agg['CTR'] < 0) | (df_channel_agg['CTR'] > 1)
        ]
        if not invalid_ctr.empty:
            logging.warning("FAIL: Found CTR values outside 0-1 range")
        else:
            logging.info("PASS: All CTR values are within range (0-1).")

    # ---------------------------------------------------------
    # FINAL VERDICT
    # ---------------------------------------------------------
    if is_valid:
        print("--- Validation Complete: SUCCESS ---")
        return processed_dfs
    else:
        raise ValueError("Data Quality Validation Failed. See logs above.")

if __name__ == "__main__":
    from transform import transform_data
    
    # 1. Find Project Root
    current_script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(current_script_path))
    staging_path = os.path.join(project_root, 'data', 'extractRawFiles')
    
    # 2. Run Transform
    data = transform_data(staging_path)
    
    # 3. Run Validation (Passing the root path)
    try:
        validate_data(data, project_root)
    except Exception as e:
        print(f"\nStopped due to error: {e}")