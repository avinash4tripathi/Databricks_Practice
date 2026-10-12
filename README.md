# Databricks Practice

A collection of hands-on Databricks labs and projects covering Spark Declarative Pipelines (SDP), Medallion architecture, Auto Loader, and Lakeflow Jobs.

## Repository Structure

```
Databricks_Practice/
├── SDP_Tutorial/
│   ├── source_prep.ipynb
│   └── first_sdp_pipeline (2)/
│       ├── Visual data prep 2026-10-07 18:41:02.designer.ipynb
│       ├── explorations/
│       │   └── sample_exploration.py
│       ├── transformations/
│       │   ├── first_dag.py
│       │   ├── second_dag.py
│       │   └── sample_aggregation_first_sdp_pipeline.py
│       └── utilities/
│           └── utils.py
├── Medallion_Lab/
│   └── Mini_Medallion_Lab.ipynb
├── AutoLoader_Lab/
│   └── AutoLoader_Lab.ipynb
└── Databricks_Jobs/
    ├── Basics/
    │   ├── Notebook-A.ipynb
    │   ├── Notebook-B.ipynb
    │   └── Notebook-C.ipynb
    └── job-1-definition.json
```

## Projects

### SDP_Tutorial
Spark Declarative Pipelines (SDP) tutorial building a sales data pipeline with materialized views.

- **source_prep.ipynb** — Source data preparation notebook that creates `sdp_catalog.source.sales`
- **transformations/first_dag.py** — First DAG defining three materialized views:
  - `src_sales` — reads source sales data, converts date column
  - `enr_sales` — enriches with 5% revenue uplift
  - `cur_sales` — aggregates by date
- **transformations/second_dag.py** — Additional SDP transformations
- **transformations/sample_aggregation_first_sdp_pipeline.py** — Sample aggregation logic
- **utilities/utils.py** — Shared utility functions
- **explorations/sample_exploration.py** — Data exploration notebook
- **Visual data prep** — Visual data prep designer file

### Medallion_Lab
Mini Medallion architecture lab demonstrating the bronze-silver-gold pattern:
- Bronze layer — raw data ingestion
- Silver layer — filtering and MERGE operations
- Gold layer — business-level aggregations

### AutoLoader_Lab
Auto Loader lab demonstrating cloudFiles streaming ingestion:
- Incremental data loading from cloud storage
- Schema inference and evolution
- Bronze Delta table writes using streaming queries

### Databricks_Jobs
Lakeflow Jobs example with a 3-task DAG:
- **Task-A (Notebook-A)** → **Task-B (Notebook-B)**
- **Task-A (Notebook-A)** → **Task-C (Notebook-C)**
- **job-1-definition.json** — Exported job configuration

## Technologies

- Databricks Spark (PySpark)
- Spark Declarative Pipelines (SDP)
- Delta Lake
- Auto Loader (cloudFiles)
- Medallion Architecture
- Lakeflow Jobs
- Unity Catalog

## Author

**avinash4tripathi**
