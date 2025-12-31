import pandas as pd
import numpy as np
import os
import logging
from typing import Dict, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Constants for file names to avoid 'magic strings' in the code
FILES = {
    'campaigns': 'campaigns.csv',
    'hcp': 'hcp_engagements.csv',
    'performance': 'channel_performance.csv',
    'brands': 'brands.csv',
    'channels': 'channels.csv'
}

def _load_data(staging_path: str) -> Optional[Dict[str, pd.DataFrame]]:
    """Loads all required CSV files from the staging directory."""
    loaded_data = {}
    try:
        for key, filename in FILES.items():
            file_path = os.path.join(staging_path, filename)
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Missing required file: {filename}")
            loaded_data[key] = pd.read_csv(file_path)
        
        logging.info("✔ Data Loaded Successfully.")
        return loaded_data

    except Exception as e:
        logging.error(f"❌ Critical Error Loading Data: {e}")
        return None

def _clean_campaigns(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans campaign data: removes zero budget and ensures valid dates."""
    df = df[df['planned_budget'] > 0].copy()
    
    # Convert dates
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    
    # Validation: Start date must be <= End date
    mask_valid_dates = df['start_date'] <= df['end_date']
    return df[mask_valid_dates].copy()

def _clean_hcp_engagements(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans HCP data: deduplicates and validates duration."""
    df = df.drop_duplicates()
    return df[df['engagement_duration_sec'] >= 0].copy()

def _clean_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans channel performance: logical checks on clicks and spend."""
    mask_logic = (df['clicks'] <= df['impressions']) & (df['spend'] >= 0)
    return df[mask_logic].copy()

def _enrich_campaign_features(df_campaigns: pd.DataFrame, df_perf: pd.DataFrame) -> pd.DataFrame:
    """Merges performance metrics into campaigns and calculates KPIs."""
    
    # Calculate Duration
    df_campaigns['campaign_duration_days'] = (
        df_campaigns['end_date'] - df_campaigns['start_date']
    ).dt.days

    # Aggregate performance by campaign
    perf_agg = df_perf.groupby('campaign_id')[['spend', 'clicks', 'conversions']].sum().reset_index()
    
    # Merge
    df_enriched = pd.merge(df_campaigns, perf_agg, on='campaign_id', how='left')

    # KPI Calculation (Safe Division)
    df_enriched['cost_per_click'] = np.where(
        df_enriched['clicks'] > 0, 
        df_enriched['spend'] / df_enriched['clicks'], 
        0
    )
    df_enriched['conversion_rate'] = np.where(
        df_enriched['clicks'] > 0, 
        df_enriched['conversions'] / df_enriched['clicks'], 
        0
    )
    
    return df_enriched

def _create_aggregations(
    df_campaigns_enriched: pd.DataFrame, 
    df_perf: pd.DataFrame, 
    df_hcp: pd.DataFrame, 
    df_brands: pd.DataFrame, 
    df_channels: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """Generates the summary tables for analysis."""
    
    # 1. Brand Performance
    brand_merge = pd.merge(df_campaigns_enriched, df_brands, on='brand_id', how='left')
    agg_brand = brand_merge.groupby('brand_name').agg(
        total_campaigns=('campaign_id', 'count'),
        total_spend=('spend', 'sum'),
        total_conversions=('conversions', 'sum'),
        avg_conversion_rate=('conversion_rate', 'mean')
    ).reset_index()

    # 2. Channel Effectiveness
    # Path: Performance -> Campaigns (for channel_id) -> Channels (for name)
    perf_camp = pd.merge(df_perf, df_campaigns_enriched[['campaign_id', 'channel_id']], on='campaign_id', how='left')
    perf_final = pd.merge(perf_camp, df_channels, on='channel_id', how='left')
    
    agg_channel = perf_final.groupby('channel_name').agg(
        total_impressions=('impressions', 'sum'),
        total_clicks=('clicks', 'sum'),
        total_spend=('spend', 'sum'),
        total_conversions=('conversions', 'sum')
    ).reset_index()
    
    # Channel KPIs
    agg_channel['CTR'] = agg_channel['total_clicks'] / agg_channel['total_impressions']
    agg_channel['avg_cost_per_click'] = agg_channel['total_spend'] / agg_channel['total_clicks']

    # 3. HCP Engagement
    hcp_camp = pd.merge(df_hcp, df_campaigns_enriched[['campaign_id', 'brand_id']], on='campaign_id', how='left')
    hcp_final = pd.merge(hcp_camp, df_brands[['brand_id', 'brand_name']], on='brand_id', how='left')
    
    agg_hcp = hcp_final.groupby(['hcp_id', 'brand_name']).agg(
        total_interactions=('engagement_id', 'count'),
        avg_engagement_time=('engagement_duration_sec', 'mean')
    ).reset_index()

    return {
        'agg_brand_performance': agg_brand,
        'agg_channel_effectiveness': agg_channel,
        'agg_hcp_summary': agg_hcp
    }

def transform_data(staging_folder_path: str) -> Dict[str, pd.DataFrame]:
    """
    Main orchestration function for the Transformation phase.
    """
    print("--- Starting Transformation Pipeline ---")

    # 1. Load
    data = _load_data(staging_folder_path)
    if not data:
        return {}

    # 2. Clean
    try:
        clean_campaigns = _clean_campaigns(data['campaigns'])
        clean_hcp = _clean_hcp_engagements(data['hcp'])
        clean_perf = _clean_performance(data['performance'])
        logging.info("✔ Data Cleaning Complete.")

        # 3. Feature Engineering
        df_campaigns_enriched = _enrich_campaign_features(clean_campaigns, clean_perf)

        # 4. Aggregations
        aggregations = _create_aggregations(
            df_campaigns_enriched, 
            clean_perf, 
            clean_hcp, 
            data['brands'], 
            data['channels']
        )
        logging.info("✔ Aggregations Created Successfully.")

        # 5. Prepare Output
        return {
            'fact_campaign_performance': df_campaigns_enriched,
            'fact_hcp_engagement': clean_hcp,
            **aggregations # Unpack the dictionary of aggregations
        }

    except Exception as e:
        logging.error(f"❌ Transformation Failed: {e}")
        return {}

# Self-test block
if __name__ == "__main__":
    current_script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(current_script_path))
    staging_path = os.path.join(project_root, 'data', 'extractRawFiles')
    
    results = transform_data(staging_path)
    
    if results:
        print("\n--- Sample Output: Channel Effectiveness ---")
        print(results['agg_channel_effectiveness'].head())