import os
import sys
import time
import logging
from pathlib import Path
from typing import Dict, Any

# Import pipeline modules
import extract
import transform
import validation
import load

# Setup Logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Configuration Constants
DB_CONFIG = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'database': 'pharma_marketing'
}

def get_project_paths() -> Dict[str, Path]:
    """Resolves and returns all necessary project paths using Pathlib."""
    # Resolve the script's directory and go up one level to project root
    project_root = Path(__file__).resolve().parent.parent
    
    return {
        'root': project_root,
        'raw': project_root / 'data' / 'raw',
        'staging': project_root / 'data' / 'extractRawFiles',
        'report': project_root / 'reports' / 'marketing_insights_report.txt'
    }

def generate_report(data: Dict[str, Any], report_path: Path) -> None:
    """
    Analyzes processed data and writes key business insights to a text file.
    """
    logging.info("Generating Business Insights Report...")
    
    try:
        # Unpack DataFrames
        df_brands = data.get('agg_brand_performance')
        df_channels = data.get('agg_channel_effectiveness')
        df_hcp = data.get('agg_hcp_summary')
        df_campaigns = data.get('fact_campaign_performance')

        if any(df is None for df in [df_brands, df_channels, df_hcp, df_campaigns]):
            raise ValueError("Missing required dataframes for reporting.")

        # 1. Top Performing Brand (Conversions)
        top_brand = df_brands.sort_values(by='total_conversions', ascending=False).iloc[0]
        
        # 2. Best Marketing Channel (ROI Proxy)
        # Calculate ROI proxy: Conversions per $1 Spend
        # FillNa(0) handles potential division by zero
        df_channels['roi_proxy'] = (
            df_channels['total_conversions'] / df_channels['total_spend']
        ).fillna(0)
        
        best_channel = df_channels.sort_values(by='roi_proxy', ascending=False).iloc[0]

        # 3. Highest HCP Engagement Brand
        # Aggregate HCP interactions by brand
        brand_hcp_agg = df_hcp.groupby('brand_name')['total_interactions'].sum().reset_index()
        top_hcp_brand = brand_hcp_agg.sort_values(by='total_interactions', ascending=False).iloc[0]

        # 4. Most Cost-Effective Campaign (Lowest CPC)
        # Filter > 0 to avoid zero-click campaigns skewing results
        valid_cpc = df_campaigns[df_campaigns['cost_per_click'] > 0]
        best_cpc_campaign = valid_cpc.sort_values(by='cost_per_click', ascending=True).iloc[0]

        # Write Report
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("========================================\n")
            f.write("PHARMA MARKETING INSIGHTS REPORT\n")
            f.write("========================================\n\n")
            
            f.write(f"1. TOP PERFORMING BRAND (Conversions)\n")
            f.write(f"   Name: {top_brand['brand_name']}\n")
            f.write(f"   Total Conversions: {int(top_brand['total_conversions'])}\n\n")
            
            f.write(f"2. BEST MARKETING CHANNEL (Efficiency)\n")
            f.write(f"   Channel: {best_channel['channel_name']}\n")
            f.write(f"   Efficiency: {best_channel['roi_proxy']:.4f} conversions/$1 spend\n\n")
            
            f.write(f"3. HIGHEST HCP ENGAGEMENT\n")
            f.write(f"   Brand: {top_hcp_brand['brand_name']}\n")
            f.write(f"   Total Interactions: {int(top_hcp_brand['total_interactions'])}\n\n")
            
            f.write(f"4. MOST COST-EFFECTIVE CAMPAIGN (Lowest CPC)\n")
            f.write(f"   Campaign: {best_cpc_campaign['campaign_name']}\n")
            f.write(f"   CPC: ${best_cpc_campaign['cost_per_click']:.2f}\n")
            
        logging.info(f"✔ Report generated successfully: {report_path}")

    except Exception as e:
        logging.error(f"❌ Failed to generate report: {e}")
        # We catch report errors specifically so they don't look like pipeline failures
        raise e 

def run_pipeline():
    """Main orchestration function."""
    start_time = time.time()
    paths = get_project_paths()

    print("\n=== STARTING ETL PIPELINE ===\n")

    try:
        # --- Step 1: Extraction ---
        # extract.extract_data returns a dict of raw dataframes
        extract.extract_data(str(paths['raw']))
        
        # --- Step 2: Transformation ---
        # transform.transform_data returns cleaned/aggregated dataframes
        clean_data = transform.transform_data(str(paths['staging']))
        
        if not clean_data:
            raise ValueError("Transformation step failed or returned no data.")
            
        # --- Step 3: Validation ---
        # Raises error if critical checks fail
        valid_data = validation.validate_data(clean_data, str(paths['root']))
        
        # --- Step 4: Loading ---
        # Loads to MySQL and saves processed CSVs
        load.load_data(valid_data, str(paths['root']), DB_CONFIG)
        
        # --- Step 5: Reporting ---
        generate_report(valid_data, paths['report'])

        duration = time.time() - start_time
        logging.info(f"✨ PIPELINE COMPLETED SUCCESSFULLY in {duration:.2f} seconds.")

    except Exception as e:
        logging.critical(f"🛑 Pipeline Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()