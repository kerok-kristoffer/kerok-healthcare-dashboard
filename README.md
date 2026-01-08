# Healthcare Staffing & Quality Analytics — AWS Lakehouse + Streamlit

A production-style **serverless data pipeline and dashboard** for analyzing U.S. nursing home staffing and quality metrics.  
Built end-to-end on AWS using **Athena + Iceberg**, **Step Functions**, and **Streamlit** following the **Medallion (Bronze → Silver → Gold)** architecture pattern.

---

## Project Overview
This project demonstrates how to design a **modern data lakehouse** that ingests raw CMS (Centers for Medicare & Medicaid Services) CSV files, performs incremental transformations, and powers a real-time analytics dashboard — all without traditional ETL servers.

### Key Outcomes
-  Automated ingestion from Google Drive → AWS S3  
-  EventBridge + Step Functions orchestration of Athena SQL MERGEs  
-  Iceberg-backed Silver and Gold tables with ACID merges  
-  Streamlit dashboard powered by PyAthena for live metrics  
-  Full CI-friendly structure with documentation and diagrams  

---

## Architecture at a Glance

![Pipeline Architecture](docs/kerok_healthcare_aws_drawio.png)

The system follows a **3-layered Medallion architecture**:

| Layer | Purpose | AWS Components |
|-------|----------|----------------|
| 🥉 **Bronze** | Raw data ingestion | S3 + Lambda + EventBridge |
| 🥈 **Silver** | Data cleaning, typing, normalization | Athena (MERGE) + Iceberg |
| 🥇 **Gold** | Analytical facts and dimensions | Athena + Glue + Streamlit |

Each new `.csv` file added to S3 automatically triggers the pipeline and updates downstream Iceberg tables.

---

## Quickstart

### Prerequisites
- AWS account with permissions for **S3**, **Athena**, **Glue**, and **Step Functions**
- Python 3.9+
- Streamlit + PyAthena libraries

### Setup
```bash
# Clone repository
git clone https://github.com/<your-handle>/healthcare-lakehouse.git
cd healthcare-lakehouse

# Create virtual environment
python -m venv .venv && source .venv/bin/activate

# Install dependencies
pip install -r dashboard/requirements.txt
