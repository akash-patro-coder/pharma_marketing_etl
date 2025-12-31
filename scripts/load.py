import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os
import logging
from typing import Dict, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Default configuration (simulating a config file or env vars)
DEFAULT_DB_CONFIG = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'database': 'pharma_marketing'
}

def _get_connection_string(config: Dict[str, str], db_name: Optional[str] = None) -> str:
    """
    Constructs the SQLAlchemy connection string.
    If db_name is None, connects to the server root (useful for creating DBs).
    """
    base = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}"
    if db_name:
        return f"{base}/{db_name}"
    return base

def _create_database_if_not_exists(config: Dict[str, str]) -> None:
    """Connects to MySQL server and creates the target database if missing."""
    db_name = config['database']
    conn_str = _get_connection_string(config, db_name=None) # Connect to server, not specific DB
    
    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
            logging.info(f"✔ Database check passed: '{db_name}' exists or was created.")
    except Exception as e:
        logging.error(f"❌ Failed to create database '{db_name}'. Error: {e}")
        raise # Critical error, stop pipeline

def _get_db_engine(config: Dict[str, str]) -> Engine:
    """Returns a SQLAlchemy engine connected to the specific database."""
    conn_str = _get_connection_string(config, config['database'])
    try:
        engine = create_engine(conn_str)
        # Test connection
        with engine.connect() as conn:
            pass
        return engine
    except Exception as e:
        logging.error(f"❌ Failed to connect to database. Error: {e}")
        raise

def _load_raw_dimensions(raw_path: str) -> Dict[str, pd.DataFrame]:
    """Loads raw dimension files (Brands, Channels) that weren't transformed."""
    dims = {}
    try:
        dims['dim_brands'] = pd.read_csv(os.path.join(raw_path, 'brands.csv'))
        dims['dim_channels'] = pd.read_csv(os.path.join(raw_path, 'channels.csv'))
    except FileNotFoundError:
        logging.warning("⚠ Could not find raw brands/channels. Loading empty DataFrames.")
        dims['dim_brands'] = pd.DataFrame()
        dims['dim_channels'] = pd.DataFrame()
    return dims

def load_data(processed_dfs: Dict[str, pd.DataFrame], project_root: str, db_config: Optional[Dict[str, str]] = None):
    """
    Main Orchestrator for Phase 4: Data Loading.
    1. Prepares Database.
    2. Maps DataFrames to Table Names.
    3. Performs Dual-Write (CSV + MySQL).
    """
    print("--- Starting Data Loading (Phase 4) ---")

    # 1. Setup Configuration & Paths
    config = db_config if db_config else DEFAULT_DB_CONFIG
    processed_path = os.path.join(project_root, 'data', 'processed')
    raw_path = os.path.join(project_root, 'data', 'extractRawFiles')
    os.makedirs(processed_path, exist_ok=True)

    # 2. Database Setup
    _create_database_if_not_exists(config)
    engine = _get_db_engine(config)

    # 3. Prepare Data Mappings
    # Load raw dimensions first
    raw_dims = _load_raw_dimensions(raw_path)
    
    # Define the map: { 'Table Name' : DataFrame }
    tables_map = {
        'dim_brands': raw_dims['dim_brands'],
        'dim_channels': raw_dims['dim_channels'],
        'dim_campaigns': processed_dfs.get('fact_campaign_performance')[
            ['campaign_id', 'brand_id', 'channel_id', 'campaign_name', 
             'start_date', 'end_date', 'status', 'planned_budget']
        ],
        'fact_hcp_engagement': processed_dfs.get('fact_hcp_engagement'),
        'fact_channel_performance': processed_dfs.get('fact_campaign_performance')[
            ['campaign_id', 'spend', 'clicks', 'conversions', 'cost_per_click', 'conversion_rate']
        ],
        'summary_brand_metrics': processed_dfs.get('agg_brand_performance')
    }

    # 4. Perform Load
    with engine.begin() as conn: # Transactional scope
        for table_name, df in tables_map.items():
            if df is None or df.empty:
                logging.warning(f"Skipping {table_name}: Data is empty.")
                continue

            try:
                # A. Write to CSV (Backup/Audit)
                csv_path = os.path.join(processed_path, f"{table_name}.csv")
                df.to_csv(csv_path, index=False)

                # B. Write to MySQL
                df.to_sql(table_name, con=conn, if_exists='replace', index=False)
                
                logging.info(f"✔ LOADED: {table_name:<25} | Rows: {len(df)}")

            except Exception as e:
                logging.error(f"❌ Failed to load {table_name}: {e}")
                # We do NOT raise here if we want partial success, 
                # but in production, you might want to rollback using transactions.

    print("--- Loading Complete ---\n")

if __name__ == "__main__":
    from transform import transform_data
    from validation import validate_data
    
    # Robust Path Finding
    current_script = os.path.abspath(__file__)
    root_dir = os.path.dirname(os.path.dirname(current_script))
    staging_dir = os.path.join(root_dir, 'data', 'extractRawFiles')

    # Simulation
    print("Running Load Simulation...")
    try:
        data = transform_data(staging_dir)
        valid_data = validate_data(data, root_dir)
        
        # In a real app, this might come from os.environ
        my_config = DEFAULT_DB_CONFIG.copy()
        
        load_data(valid_data, root_dir, my_config)
    except Exception as e:
        print(f"Simulation failed: {e}")