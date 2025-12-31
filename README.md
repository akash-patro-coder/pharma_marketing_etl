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

### 🏗 Architecture

The pipeline follows a strict modular design to ensure scalability and ease of debugging.

```mermaid
flowchart LR
    A[Raw Data Source] -->|extract.py| B(data/extractRawFiles)
    B -->|transform.py| C{Data Cleaning & Logic}
    C -->|Star Schema| D[Processed CSVs]
    C -->|Star Schema| E[(MySQL Database)]
