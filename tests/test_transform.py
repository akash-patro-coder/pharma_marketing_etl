import unittest
import pandas as pd
import numpy as np
import os
import shutil
import sys

# 1. Setup Path: Allow importing from the 'scripts' folder
# (Assumes folder structure: project/tests/test_transform.py)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

from transform import transform_data

class TestTransformation(unittest.TestCase):

    def setUp(self):
        """
        Runs BEFORE every test.
        Creates a temporary folder and fills it with dummy CSV data.
        """
        self.test_dir = 'tests/temp_data'
        os.makedirs(self.test_dir, exist_ok=True)

        # --- A. Create Dummy Campaigns ---
        # Case 1: Valid Active Campaign
        # Case 2: Zero Budget (Should be removed)
        # Case 3: Invalid Dates (Start > End) (Should be removed)
        df_campaigns = pd.DataFrame({
            'campaign_id': [1, 2, 3],
            'brand_id': [101, 101, 102],
            'channel_id': [10, 20, 10],
            'campaign_name': ['Valid Camp', 'Zero Budget Camp', 'Bad Dates Camp'],
            'start_date': ['2023-01-01', '2023-01-01', '2023-02-01'],
            'end_date':   ['2023-01-10', '2023-01-10', '2023-01-01'], # Camp 3 ends before start
            'status': ['active', 'planned', 'active'],
            'planned_budget': [1000, 0, 5000] # Camp 2 has 0 budget
        })
        df_campaigns.to_csv(os.path.join(self.test_dir, 'campaigns.csv'), index=False)

        # --- B. Create Dummy Performance ---
        # Camp 1: Standard performance
        # Camp 4 (Non-existent in campaigns): Should not break code, but won't merge
        # Edge Case: 0 Clicks (Check Division by Zero protection)
        df_perf = pd.DataFrame({
            'performance_id': [1, 2, 3],
            'campaign_id': [1, 1, 99], # Two entries for Camp 1 to test grouping
            'impressions': [1000, 1000, 500],
            'clicks': [10, 0, 5],      # Row 2 has 0 clicks
            'conversions': [2, 0, 1],
            'spend': [100, 50, 20]     # Row 2 spend 50 with 0 clicks -> CPC should handle this
        })
        df_perf.to_csv(os.path.join(self.test_dir, 'channel_performance.csv'), index=False)

        # --- C. Create Dummy HCP Data ---
        # Include duplicates and negative duration
        df_hcp = pd.DataFrame({
            'engagement_id': [1, 1, 2], # ID 1 is duplicated
            'campaign_id': [1, 1, 1],
            'hcp_id': [50, 50, 51],
            'engagement_duration_sec': [120, 120, -10] # Duplicate & Negative
        })
        df_hcp.to_csv(os.path.join(self.test_dir, 'hcp_engagements.csv'), index=False)

        # --- D. Create Dummy Dimensions ---
        pd.DataFrame({'brand_id': [101, 102], 'brand_name': ['Brand A', 'Brand B']}).to_csv(os.path.join(self.test_dir, 'brands.csv'), index=False)
        pd.DataFrame({'channel_id': [10, 20], 'channel_name': ['Email', 'Web']}).to_csv(os.path.join(self.test_dir, 'channels.csv'), index=False)

    def tearDown(self):
        """Runs AFTER every test. Cleans up."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_cleaning_logic(self):
        """Test if bad data (0 budget, bad dates, negatives) is removed."""
        results = transform_data(self.test_dir)
        df_camp = results['fact_campaign_performance']
        df_hcp = results['fact_hcp_engagement']

        # 1. Campaign Cleaning
        # Should only have Campaign 1. 
        # Camp 2 (Budget 0) and Camp 3 (Bad Dates) should be gone.
        self.assertEqual(len(df_camp), 1)
        self.assertEqual(df_camp.iloc[0]['campaign_id'], 1)

        # 2. HCP Cleaning
        # Should drop duplicate ID 1 and drop negative duration ID 2.
        # Original: 3 rows. Unique ID 1 (valid) = 1 row. ID 2 (negative) = removed.
        # Result should be exactly 1 row (The valid, unique interaction).
        self.assertEqual(len(df_hcp), 1)
        self.assertEqual(df_hcp.iloc[0]['engagement_duration_sec'], 120)

    def test_feature_engineering_math(self):
        """Test grouping, CPC calculation, and Division by Zero."""
        results = transform_data(self.test_dir)
        df_camp = results['fact_campaign_performance']
        
        # Campaign 1 Stats from `df_perf`:
        # Row 1: Spend 100, Clicks 10, Conv 2
        # Row 2: Spend 50,  Clicks 0,  Conv 0
        # Total: Spend 150, Clicks 10, Conv 2
        
        row = df_camp[df_camp['campaign_id'] == 1].iloc[0]
        
        # Check Totals
        self.assertEqual(row['spend'], 150)
        self.assertEqual(row['clicks'], 10)
        
        # Check KPI Math
        # CPC = 150 / 10 = 15.0
        self.assertEqual(row['cost_per_click'], 15.0)
        # Conv Rate = 2 / 10 = 0.2
        self.assertEqual(row['conversion_rate'], 0.2)
        
        # Check Duration (Jan 1 to Jan 10 = 9 days)
        self.assertEqual(row['campaign_duration_days'], 9)

    def test_channel_aggregation(self):
        """Test if Channel Effectiveness aggregates correctly (including conversions)."""
        results = transform_data(self.test_dir)
        df_channel = results['agg_channel_effectiveness']
        
        # Campaign 1 is on Channel 10 (Email).
        # Totals for Campaign 1 were: Imp 2000 (1000+1000), Clicks 10, Spend 150, Conv 2.
        
        email_stats = df_channel[df_channel['channel_name'] == 'Email'].iloc[0]
        
        self.assertEqual(email_stats['total_impressions'], 2000)
        self.assertEqual(email_stats['total_clicks'], 10)
        self.assertEqual(email_stats['total_spend'], 150)
        # This confirms our fix for the report error:
        self.assertEqual(email_stats['total_conversions'], 2)

if __name__ == '__main__':
    unittest.main()