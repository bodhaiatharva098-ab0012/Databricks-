
# Databricks Avocado Sales ETL Pipeline

An end-to-end data engineering project using **Databricks, PySpark, Delta Lake, Unity Catalog, and Power BI** to process avocado sales data through the Bronze, Silver, and Gold layers of the Medallion Architecture.

## Project Overview

This project demonstrates how raw CSV data can be ingested, cleaned, transformed, and aggregated into business-ready datasets using Databricks.

The source dataset, `avocado.csv`, is stored in a Unity Catalog Volume. PySpark processes the data, Delta tables store the results, and the Gold layer provides summarized metrics for business analysis and Power BI reporting.

## Architecture

**CSV → Bronze → Silver → Gold → Power BI**

1. **Bronze Layer:** Ingest raw avocado CSV data and preserve source information.
2. **Silver Layer:** Clean and standardize columns, convert data types, and validate records.
3. **Gold Layer:** Aggregate sales volume, prices, and bag sales by year, avocado type, and region.
4. **Power BI:** Use the Gold dataset to build interactive dashboards and reports.

## 1. Bronze Layer – Raw Data Ingestion

**Table:** `workspace.ab.bronze_avocado`

The Bronze layer is the first stage of the pipeline. It reads the raw avocado CSV file from Unity Catalog and stores the ingested records in Delta format.

### Key Operations

- Read the CSV file using PySpark.
- Preserve raw data values and column names.
- Add source file and ingestion timestamp metadata.
- Save the data as a Delta table.
- Support data lineage and downstream processing.

### Bronze Layer Infographic

![Databricks Bronze Layer – Avocado CSV Ingestion](https://raw.githubusercontent.com/bodhaiatharva098-ab0012/Databricks-/5828b6658be4a4d4bff50a4a8619588a85c9c8cc/Databricks%20Bronze%20Layer%20Infographic.jpg)

## 2. Silver Layer – Data Cleaning and Validation

**Table:** `workspace.ab.silver_avocado`

The Silver layer prepares the avocado dataset for analysis by standardizing column names, converting data types, handling missing values, and removing invalid or duplicate records where implemented.

### Key Operations

- Remove unnecessary index columns.
- Standardize column names.
- Trim text and handle blank values.
- Convert dates and numeric fields to appropriate data types.
- Validate records and remove duplicates.
- Save cleaned data in Delta format.

### Silver Layer Infographic

![Databricks Silver Layer – Avocado Data Processing](https://raw.githubusercontent.com/bodhaiatharva098-ab0012/Databricks-/5828b6658be4a4d4bff50a4a8619588a85c9c8cc/Databricks%20Silver%20Layer%20Avocado%20Pipeline.jpg)

## 3. Gold Layer – Business Aggregation and Analytics

**Table:** `workspace.ab.gold_avocado`

The Gold layer converts cleaned Silver data into business-level summaries. It groups avocado sales data by year, type, and region to support reporting and decision-making.

### Key Operations

- Group data by `year`, `type`, and `region`.
- Calculate total sales volume.
- Calculate average, minimum, and maximum prices.
- Summarize total, small, large, and extra-large bag volumes.
- Count records for each group.
- Round numeric values for reporting.
- Save the aggregated output as a Delta table.

### Gold Layer Infographic

![Databricks Gold Layer – Business Aggregation and Analytics](images/gold_layer.png)

> Upload your Gold Layer infographic to the repository's `images` folder as `gold_layer.png` for this image to display.

## Source Dataset

**CSV file:** `avocado.csv`

**Unity Catalog Volume path:**

`/Volumes/workspace/ab/sql/avocado.csv`

The dataset contains avocado pricing, sales volume, bag sales, date, year, avocado type, and region information.

## Tables Created

| Layer | Table Name | Purpose |
|---|---|---|
| Bronze | `workspace.ab.bronze_avocado` | Raw data ingestion |
| Silver | `workspace.ab.silver_avocado` | Cleaned and standardized data |
| Gold | `workspace.ab.gold_avocado` | Aggregated business metrics |

## Technologies Used

- **Databricks:** Data engineering and notebook execution.
- **PySpark:** Data processing, cleaning, and aggregation.
- **Delta Lake:** Reliable storage of processed datasets.
- **Unity Catalog:** Data organization and access management.
- **Power BI:** Business intelligence and visualization.

## Business Insights

The Gold dataset can be used to analyze:

- Avocado sales volume by region.
- Conventional versus organic avocado performance.
- Average prices across different years.
- Regional price variations.
- Small, large, and extra-large bag sales.
- Minimum and maximum avocado prices.

## Project Structure

```text
Databricks-Avocado-ETL/
│
├── README.md
├── avocado.csv
├── images/
│   ├── bronze_layer.png
│   ├── silver_layer.png
│   └── gold_layer.png
└── notebooks/
    ├── bronze_layer.py
    ├── silver_layer.py
    └── gold_layer.py
```

*The folder structure above is a suggested organization for the repository.*

## How to Run

1. Upload `avocado.csv` to the Unity Catalog Volume.
2. Open your Databricks workspace.
3. Run the Bronze ingestion notebook.
4. Run the Silver cleaning and validation notebook.
5. Run the Gold aggregation notebook.
6. Verify the output tables in Unity Catalog.
7. Connect Power BI to Databricks SQL Warehouse to create dashboards.

## Expected Outcome

The pipeline transforms raw avocado sales data into structured Delta tables across three layers. The Gold layer provides business-ready summaries for reporting, visualization, and further analysis.

## Future Enhancements

- Automate execution using Databricks Workflows.
- Add data quality checks and validation reports.
- Implement incremental data processing.
- Schedule regular data refreshes.
- Build interactive Power BI dashboards.

## Author

**Atharva Bodhai**

GitHub: [bodhaiatharva098-ab0012](https://github.com/bodhaiatharva098-ab0012)
