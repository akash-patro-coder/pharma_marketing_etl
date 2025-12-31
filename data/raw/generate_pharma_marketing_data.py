import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

# ------------------------------
# Configuration
# ------------------------------
BASE_DIR = "data/raw"
os.makedirs(BASE_DIR, exist_ok=True)

np.random.seed(42)
random.seed(42)

TODAY = datetime.today()

# ------------------------------
# 1. Brands
# ------------------------------
brands = [
    (1, "Xtandi", "Oncology", 2012, "Astellas"),
    (2, "Padcev", "Oncology", 2019, "Astellas"),
    (3, "Xospata", "Oncology", 2018, "Astellas"),
    (4, "Vyloy", "Oncology", 2023, "Astellas")
]

brands_df = pd.DataFrame(
    brands,
    columns=["brand_id", "brand_name", "therapeutic_area", "launch_year", "manufacturer"]
)

brands_df.to_csv(f"{BASE_DIR}/brands.csv", index=False)

# ------------------------------
# 2. Channels
# ------------------------------
channels = [
    (1, "eDetails", "digital"),
    (2, "Email", "digital"),
    (3, "Website", "digital"),
    (4, "Video", "digital"),
    (5, "Print", "traditional")
]

channels_df = pd.DataFrame(
    channels,
    columns=["channel_id", "channel_name", "channel_type"]
)

channels_df.to_csv(f"{BASE_DIR}/channels.csv", index=False)

# ------------------------------
# 3. Campaigns
# ------------------------------
campaign_rows = []
campaign_id = 1

for _ in range(200):
    brand_id = random.randint(1, 4)
    channel_id = random.randint(1, 5)

    start_date = TODAY - timedelta(days=random.randint(30, 365))
    end_date = start_date + timedelta(days=random.randint(15, 120))

    if random.random() < 0.15:  # ongoing campaigns
        end_date = None

    budget = random.randint(50_000, 500_000)

    campaign_rows.append((
        campaign_id,
        brand_id,
        channel_id,
        f"Campaign_{campaign_id}",
        start_date.date(),
        end_date.date() if end_date else None,
        budget,
        random.choice(["planned", "active", "completed"])
    ))

    campaign_id += 1

campaigns_df = pd.DataFrame(
    campaign_rows,
    columns=[
        "campaign_id",
        "brand_id",
        "channel_id",
        "campaign_name",
        "start_date",
        "end_date",
        "planned_budget",
        "status"
    ]
)

campaigns_df.to_csv(f"{BASE_DIR}/campaigns.csv", index=False)

# ------------------------------
# 4. Channel Performance
# ------------------------------
performance_rows = []

for _, row in campaigns_df.iterrows():
    impressions = random.randint(10_000, 500_000)
    clicks = random.randint(100, impressions // 10)
    conversions = random.randint(10, clicks // 2)

    performance_rows.append((
        len(performance_rows) + 1,
        row["campaign_id"],
        impressions,
        clicks,
        conversions,
        random.randint(20_000, row["planned_budget"])
    ))

channel_perf_df = pd.DataFrame(
    performance_rows,
    columns=[
        "performance_id",
        "campaign_id",
        "impressions",
        "clicks",
        "conversions",
        "spend"
    ]
)

channel_perf_df.to_csv(f"{BASE_DIR}/channel_performance.csv", index=False)

# ------------------------------
# 5. HCP Engagements
# ------------------------------
hcp_rows = []
hcp_ids = range(1000, 2000)

for _ in range(20_000):
    campaign = campaigns_df.sample(1).iloc[0]
    engagement_date = pd.to_datetime(campaign["start_date"]) + timedelta(
        days=random.randint(0, 60)
    )

    hcp_rows.append((
        len(hcp_rows) + 1,
        campaign["campaign_id"],
        random.choice(hcp_ids),
        random.choice(["open", "click", "view", "download"]),
        engagement_date.date(),
        random.randint(10, 900)
    ))

hcp_df = pd.DataFrame(
    hcp_rows,
    columns=[
        "engagement_id",
        "campaign_id",
        "hcp_id",
        "interaction_type",
        "interaction_date",
        "engagement_duration_sec"
    ]
)

hcp_df.to_csv(f"{BASE_DIR}/hcp_engagements.csv", index=False)

# ------------------------------
# 6. Website Metrics
# ------------------------------
website_rows = []

for _ in range(5000):
    website_rows.append((
        len(website_rows) + 1,
        random.randint(1, 4),
        (TODAY - timedelta(days=random.randint(1, 365))).date(),
        random.randint(500, 10_000),
        random.randint(200, 5_000),
        round(random.uniform(0.2, 0.8), 2)
    ))

website_df = pd.DataFrame(
    website_rows,
    columns=[
        "metric_id",
        "brand_id",
        "visit_date",
        "page_views",
        "unique_visitors",
        "bounce_rate"
    ]
)

website_df.to_csv(f"{BASE_DIR}/website_metrics.csv", index=False)

print("✅ Pharma marketing CSV files generated successfully!")
