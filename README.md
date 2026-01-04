<div align="center">

  <img src="https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/MySQL-Database-4479A1?style=for-the-badge&logo=mysql&logoColor=white" />
  <img src="https://img.shields.io/badge/ETL-Pipeline-FFD43B?style=for-the-badge&logo=data-analysis&logoColor=black" />

  <h1>🚀 End-to-End Data Pipeline (ETL)</h1>
  
  <p>
    <b>Automated data extraction, Star Schema transformation, and dual-destination loading.</b>
  </p>

  <p>
    <a href="#-architecture">Architecture</a> •
    <a href="#-key-features">Key Features</a> •
    <a href="#-setup--usage">How to Run</a>
  </p>

</div>

---

### 📖 Project Overview

This project is a robust **ETL (Extract, Transform, Load)** system built with Python. It automates the lifecycle of data processing—moving raw data from external sources, cleaning and structuring it into a **Star Schema** for analytics, and simultaneously loading it into both a relational database and flat file storage for redundancy.

---

https://github.com/akash-patro-coder/pharma_marketing_etl/tree/main ---> complete project

https://github.com/akash-patro-coder/pharma_marketing_etl/blob/main/data/raw/generate_pharma_marketing_data.py ----> this python file can generating my csv file (10,000, 20,000) raw file creating 

**Data Structure**</br>
**data/raw**: The original source. We keep these untouched so we always have a backup of the raw truth.

**data/extractRawFiles(staging)**: The staging area. A working copy of the data where the actual processing happens, keeping the originals safe.

**data/processed**: The final output. Clean, ready-to-use data saved as CSVs for auditing or backup.

**The Scripts (Logic)**</br>
**extract.py**: Handles ingestion. Simply reads the source files and moves them to our staging area.

**transform.py**: The cleaning engine. Removes duplicates, calculates KPIs (like ROI), and organizes data into our Star Schema.

**validation.py**: The gatekeeper. Checks for bad data (like negative numbers) and stops the pipeline if quality standards aren't met.

**load.py**: The loader. Saves valid data into the MySQL database for analysis and creates CSV backups(processed).

**main.py**: The orchestrator. Runs the full pipeline (Extract → Transform → Validate → Load) automatically in one go.

**Outputs & Docs**

**reports/**: The results. Stores the final business insights (e.g., "Top Performing Brand") in text format.

**tests/**: Quality assurance. Uses fake data to test our logic and ensure the math is correct before touching real data.

---

### 🏗 Architecture

The pipeline follows a strict modular design to ensure scalability and ease of debugging.

```mermaid
flowchart LR
    A[Raw] -->|extract.py| B(data/extractRawFiles)
    B -->|transform.py| C{Data Cleaning & Logic}
    C -->|Star Schema| D[Processed CSVs]
    C -->|Star Schema| E[(MySQL Database)]
