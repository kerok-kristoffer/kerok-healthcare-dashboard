# Healthcare Staffing Analytics (Streamlit + Athena)

Single-file Streamlit dashboard reading Athena/Iceberg **Gold** views.

## Quickstart

```bash
cp .env.example .env   # fill values or rely on AWS_PROFILE
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
./scripts/run_local.sh


# Healthcare Staffing & Quality Data Pipeline (AWS Native Lakehouse)

## Overview
This project implements a fully automated **data lakehouse pipeline** on AWS to ingest, transform, and analyze healthcare staffing and provider quality data.  
The architecture follows a **Medallion (Bronze → Silver → Gold)** design and provides a **Streamlit dashboard** for visualization and exploration.

---

## 🏗️ Architecture

### Data Flow
1. **Google Drive → S3 (Bronze)**
   - Lambda fetches CSVs from shared Google Drive folders.
   - Appends them to appropriate S3 prefixes (`bronze/pbj/`, `bronze/providerinfo/`).
   - Triggered automatically via EventBridge.

2. **Step Functions (ETL Orchestration)**
   - EventBridge triggers a **Step Function** pipeline when a new CSV arrives.
   - Executes multiple Athena SQL steps:
     - Cleans & merges Bronze → Silver.
     - Normalizes and upserts Silver → Gold.
     - Refreshes Gold dimensional and fact tables.

3. **Athena + Glue Data Catalog**
   - Metadata and schema tracking via AWS Glue.
   - Transformations executed in **Athena Engine v3** with **Iceberg** for schema evolution and ACID compliance.

4. **Streamlit Dashboard**
   - Connects to Athena using **PyAthena**.
   - Provides dynamic filters and visualizations for staffing metrics, bed utilization, and quality ratings.

---

## 🧩 AWS Components

| Layer | AWS Service | Purpose |
|-------|--------------|----------|
| Ingestion | Lambda + EventBridge | Automated ingestion from Google Drive |
| Storage | S3 | Raw & transformed data lake (Bronze/Silver/Gold) |
| Metadata | Glue Data Catalog | Schema definitions, type management |
| Query Engine | Athena (Iceberg tables) | SQL transformations & analytics |
| Orchestration | Step Functions | Manage ETL jobs and dependencies |
| Visualization | Streamlit | Interactive dashboard front end |

---

## 📂 Directory Structure




---

## 🧠 Pipeline Design deicisions

### AWS Native Stack
- **S3** → cost-effective, infinitely scalable, schema-flexible foundation for data lakes.  
- **Glue Catalog** → centralizes schema definitions; integrates natively with Athena and Iceberg.  
- **Athena + Iceberg** → no infrastructure management, supports ACID MERGE, schema evolution, time travel.  
- **Step Functions** → declarative orchestration, easy monitoring, parallel tracks for PBJ and ProviderInfo datasets.  
- **EventBridge** → serverless event routing; ensures data-driven workflow triggers.  
- **Lambda (Ingestion)** → isolates external ingestion logic from pipeline logic, ensures modularity.  
- **Streamlit** → Python-native, lightweight front-end for rapid dashboard deployment without frontend frameworks.

### Medallion Architecture
- **Bronze:** raw ingestion (schema inferred only).  
- **Silver:** cleaned & typed, standard schema, deduplicated rows.  
- **Gold:** analytical aggregates and dimension tables for visualization.  
This pattern isolates transformation complexity and promotes lineage clarity.

---

## 📊 Dashboard Features
- **Facility-level HPRD** (Hours per Resident per Day)
- **State comparisons** with ranking and filtering
- **Staffing mix** (permanent vs contract)
- **Bed utilization** (occupancy metrics)
- **Staffing vs occupancy scatter plot**
- **CSV export** for any view

Each visualization runs parameterized Athena SQL through PyAthena with caching to optimize costs.

---

## 🧪 Testing & Validation
- Test CSV append triggers verified via EventBridge and Step Function logs.
- Silver and Gold table merges validated by querying Athena with unique test CCNs (e.g., `99999`).
- Cross-validation between Bronze and Silver to ensure no column loss.
- Dashboard validated against expected aggregations.

---

## 🚀 Deployment

### Local Run
```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
