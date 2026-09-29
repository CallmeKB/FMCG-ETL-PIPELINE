# Consolidated ETL Pipeline

This repository contains an end-to-end ETL (Extract, Transform, Load) pipeline built using PySpark on Databricks. The pipeline processes raw FMCG (Fast-Moving Consumer Goods) data into a structured data warehouse following the medallion architecture (Bronze, Silver, Gold layers) with Delta Lake.

## Overview

The pipeline performs the following steps:
1. **Setup**: Creates the necessary catalog (`fmcg`) and schemas (`bronze`, `silver`, `gold`) in Databricks Unity Catalog.
2. **Dimension Processing**: Cleanses and enriches dimension data (customers, products, pricing) and loads them into the Silver and Gold layers.
3. **Fact Processing**: Loads fact data (sales transactions) into the Gold layer, supporting both full and incremental loads.

## Folder Structure

```
consolidated_pipeline/
│
├── 1_setup_folder/
│   ├── setup_catalogs.py          # Creates catalog and schemas
│   ├── dim_date_table_creation.py # Creates date dimension table
│   └── utilities.py               # Shared variables (schema names)
│
├── 2_dimension_data_processing/
│   ├── 1_customer_data_processing.py
│   ├── 2_products_data_processing.py
│   └── 3_pricing_data_processing.py
│
└── 3_fact_data_processing/
    ├── 1_full_load_fact.py
    └── 2_incremental_load_fact.py
```

## Components

### 1. Setup
- **setup_catalogs.py**: Initializes the `fmcg` catalog and creates `bronze`, `silver`, and `gold` schemas.
- **dim_date_table_creation.py**: Generates a date dimension table commonly used in data warehousing.
- **utilities.py**: Defines schema names (`bronze_schema`, `silver_schema`, `gold_schema`) for reuse across notebooks.

### 2. Dimension Processing
Each notebook follows a similar pattern:
- Reads raw CSV files from S3 (`s3://sportsbar-dp-kaushal/<data_source>/*.csv`).
- Writes raw data to the Bronze layer as Delta tables.
- Cleanses and transforms data (deduplication, trimming, standardization, business rule applications).
- Writes cleansed data to the Silver layer.
- Creates final dimension tables in the Gold layer with surrogate keys and slowly changing dimension (SCD) Type 2 logic (where applicable).

**Examples**:
- Customer data: Standardizes names, corrects city values, creates a composite `customer` key (`CustomerName-City`), and adds static attributes (`market`, `platform`, `channel`).
- Product and pricing data: Similar cleansing and enrichment steps tailored to each domain.

### 3. Fact Processing
- **1_full_load_fact.py**: Performs a full load of fact data into the Gold layer.
- **2_incremental_load_fact.py**: Processes only new or changed fact data (using Delta Lake's Change Data Feed) for incremental updates.

## How to Run

1. **Prerequisites**:
   - Access to a Databricks workspace with Unity Catalog enabled.
   - The `sportsbar-dp-kaushal` S3 bucket containing raw CSV files for customers, products, and pricing.
   - Appropriate permissions to create catalogs, schemas, and tables.

2. **Execution Order**:
   - Run notebooks in the following sequence:
     1. `1_setup_folder/setup_catalogs.py`
     2. `1_setup_folder/dim_date_table_creation.py`
     3. `2_dimension_data_processing/1_customer_data_processing.py`
     4. `2_dimension_data_processing/2_products_data_processing.py`
     5. `2_dimension_data_processing/3_pricing_data_processing.py`
     6. `3_fact_data_processing/1_full_load_fact.py` (for initial load)
     7. `3_fact_data_processing/2_incremental_load_fact.py` (for subsequent incremental runs)

3. **Execution Method**:
   - **Interactive**: Open each notebook in Databricks and run all cells.
   - **Scheduled Job**: Create a Databricks job that runs the notebooks in the above order as separate tasks.

## Technology Stack

- **Language**: PySpark (Spark SQL)
- **Storage**: Delta Lake on S3 (via Unity Catalog external locations or managed storage)
- **Orchestration**: Databricks Notebooks & Jobs
- **Data Formats**: CSV (raw), Delta Lake (processed)

## Key Features

- **Medallion Architecture**: Clear separation of concerns (Bronze = raw, Silver = cleansed, Gold = business-ready).
- **Delta Lake**: ACID transactions, time travel, and Change Data Feed for incremental processing.
- **Data Quality**: Includes deduplication, null handling, standardization, and business rule validation.
- **Reusability**: Shared utilities and parameterized notebooks (via widgets) for different data sources.

## Notes

- The pipeline assumes the raw data files are in CSV format with headers.
- Schema evolution is enabled (`mergeSchema: true`) to accommodate changes in source data.
- Change Data Feed is enabled on all Delta tables to support incremental processing.

## Future Enhancements

- Add data validation and testing frameworks (e.g., Great Expectations).
- Implement logging and monitoring (e.g., Databricks job alerts, custom metrics).
- Parameterize S3 paths and other configurations via Databricks Jobs or a config file.
- Containerize the pipeline for portability (if moving outside Databricks).

--- 

*Pipeline Author: [Your Name or Team]*  
*Last Updated: September 2026*