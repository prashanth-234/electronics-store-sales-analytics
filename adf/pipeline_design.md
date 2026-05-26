# Azure Data Factory Pipeline Design

## Pipeline Name
PL_Electronics_Store_Sales_Ingestion

## Objective
This pipeline ingests electronics store sales data from On-Prem SQL Server into ADLS Gen2 Raw Layer and triggers Azure Databricks for Delta Lake processing.

## Architecture

On-Prem SQL Server  
↓  
Self-Hosted Integration Runtime  
↓  
Azure Data Factory  
↓  
ADLS Gen2 Raw Layer  
↓  
Azure Databricks Notebook  
↓  
Delta Lake Bronze / Silver / Gold  

## Pipeline Activities

### 1. Lookup Activity
**Name:** LK_Get_Source_Config

Purpose:
- Reads metadata/config table.
- Gets source table name, target folder path, and load type.

Example config table:

| source_table | target_path | load_type | is_active |
|---|---|---|---|
| electronics_sales | raw/electronics_sales/ | Full | 1 |

---

### 2. Copy Activity
**Name:** CP_SQLServer_To_ADLS

Source:
- On-Prem SQL Server
- Table: electronics_sales

Sink:
- ADLS Gen2
- Folder: raw/electronics_sales/
- Format: CSV or Parquet

Purpose:
- Copies sales data from SQL Server to ADLS Raw Layer.

---

### 3. Databricks Notebook Activity
**Name:** NB_Electronics_Delta_Processing

Notebook:
```text
/databricks/electronics_sales_delta_pipeline.py
