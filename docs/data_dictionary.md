# Data Dictionary — Healthcare Staffing & Provider Quality Pipeline

This document details the schema and lineage across the Bronze, Silver, and Gold layers of the Kerok Healthcare Data Pipeline.

---

## 🥉 Bronze Layer — Raw Zone

| Dataset | Table Name | Description | Example S3 Prefix |
|----------|-------------|--------------|------------------|
| PBJ Staffing | `bronze_pbj_daily_nurse_staffing_q2_2024_csv` | Daily staffing data from CMS PBJ datasets. | `s3://kerok-healthcare-landing/bronze/pbj/` |
| Provider Info | `bronze_nh_providerinfo_oct2024_csv` | Provider ownership, quality ratings, and inspection details. | `s3://kerok-healthcare-landing/bronze/providerinfo/` |

All fields are strings at this stage. CSVs are stored as **external tables** with `OpenCSVSerde`.

---

## 🥈 Silver Layer — Cleaned Zone

### `silver_pbj_daily`
| Column | Type | Description |
|---------|------|-------------|
| ccn | string | CMS certification number (zero-padded 6-digit key). |
| provider_name | string | Nursing home name. |
| city | string | City. |
| county | string | County. |
| county_fips | int | County FIPS code. |
| state | string | Two-letter state code. |
| cy_quarter | string | Calendar quarter (e.g., "2024Q2"). |
| workdate | date | Date of staffing record. |
| mds_census_resident_count | int | Number of residents. |
| hrs_rn, hrs_lpn, hrs_cna, hrs_rndon, hrs_natrn, hrs_medaide, hrs_admin | decimal(9,2) | Staffed hours per role. |
| hrs_*_emp / hrs_*_ctr | decimal(9,2) | Split between employee and contractor hours. |

**Partitioned by:** `month(workdate)` and `state`

---

### `silver_providerinfo`
| Column | Type | Description |
|---------|------|-------------|
| ccn | string | CMS certification number (primary key). |
| provider_name | string | Facility name. |
| provider_address | string | Street address. |
| city | string | City. |
| state | string | Two-letter code. |
| zip_code | string | ZIP code. |
| telephone_number | string | Phone number. |
| ownership_type | string | Ownership category. |
| county | string | County name. |
| provider_ssa_county_code | string | SSA county code. |
| latitude | decimal(9,4) | Latitude. |
| longitude | decimal(9,4) | Longitude. |
| number_of_certified_beds | int | Total certified beds. |
| average_number_of_residents_per_day | double | Average census. |
| multiple *_rating fields | double/string | CMS quality ratings. |
| processing_date | string | Date of processing. |

**Partitioned by:** `state`

---

## 🥇 Gold Layer — Analytics Zone

### `gold_facility_dim`
| Column | Type | Description |
|---------|------|-------------|
| ccn | string | Facility key. |
| provider_name | string | Facility name. |
| state | string | State. |
| city | string | City. |
| county | string | County. |
| ownership_type | string | Ownership category. |
| latitude | decimal(9,4) | Latitude. |
| longitude | decimal(9,4) | Longitude. |

---

### `gold_daily_staffing_fact`
| Column | Type | Description |
|---------|------|-------------|
| ccn | string | Facility key. |
| workdate | date | Observation date. |
| hrs_rn / hrs_lpn / hrs_cna | decimal(9,2) | Staffing hours by nurse type. |
| hrs_*_emp / hrs_*_ctr | decimal(9,2) | Employee vs contract hours. |
| residents | int | Resident count. |
| hrs_total_direct | decimal(9,2) | Total direct-care hours. |
| state | string | Partition key. |

---

### `gold_quarterly_provider_fact`
| Column | Type | Description |
|---------|------|-------------|
| ccn | string | Facility key. |
| reporting_period_quarter | string | Reporting quarter (e.g., `2024Q2`). |
| state | string | Partition key. |
| number_of_certified_beds | int | Certified beds. |
| average_number_of_residents_per_day | double | Average residents. |
| overall_rating | double | CMS overall quality rating. |
| number_of_fines | int | Number of fines during quarter. |
| total_amount_of_fines_in_dollars | decimal(18,2) | Fine amounts. |
| total_number_of_penalties | int | Total penalties. |
| processing_date | date | Processing date. |

---

## 💡 Analytical Views

| View | Description |
|------|--------------|
| `vw_state_hprd` | Aggregates nurse hours per resident per day (HPRD) by state. |
| `vw_facility_hprd` | Facility-level HPRD metrics. |
| `vw_staffing_mix` | Employee vs contract staffing ratios. |
| `vw_bed_utilization` | Bed utilization = resident-days / (beds * observed days). |
| `vw_staffing_vs_occupancy` | Correlation between staffing intensity and occupancy rate. |

---

## 🧠 Data Casting and Validation
- String → Numeric conversion via `try_cast()` ensures robust ingestion.
- Null-safe coalescing avoids overwrite of valid existing data.
- `date_parse()` standardizes date formats across providers.
- Athena Engine v3 + Iceberg enforces ACID compliance.

---

## 🔗 Relationships

| Source | Target | Relationship |
|---------|---------|---------------|
| `gold_facility_dim.ccn` | `gold_daily_staffing_fact.ccn` | 1-to-many |
| `gold_facility_dim.ccn` | `gold_quarterly_provider_fact.ccn` | 1-to-1 per quarter |

These relationships enable dimensional joins for the Streamlit dashboard.

---

## 🔄 Data Lineage Summary
