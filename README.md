
# Databricks Avocado Sales ETL Pipeline

An end-to-end data engineering project using **Databricks, PySpark, Delta Lake, Unity Catalog, and Power BI** to process avocado sales data through the Bronze, Silver, and Gold layers of the Medallion Architecture.

## Project Overview

This project demonstrates how raw avocado sales data is ingested from a CSV file, cleaned and validated using PySpark, and transformed into business-ready summary tables for analytics and reporting.

### Technologies Used

- **Databricks:** Cloud data processing and notebook environment
- **PySpark:** Data ingestion, cleaning, and aggregation
- **Unity Catalog:** Data governance and table management
- **Delta Lake:** Reliable storage for Bronze, Silver, and Gold tables
- **CSV:** Source dataset format
- **Power BI:** Business intelligence and visualization

## ETL Workflow

**CSV File → Bronze Layer → Silver Layer → Gold Layer → Power BI**

1. Extract avocado sales data from the Unity Catalog Volume.
2. Load raw data into the Bronze Delta table.
3. Clean and standardize data in the Silver layer.
4. Aggregate business metrics in the Gold layer.
5. Connect the Gold data to Power BI for reporting.

## 1. Bronze Layer – Raw Data Ingestion

**Table:** `workspace.ab.bronze_avocado`

The Bronze layer reads the raw `avocado.csv` file from a Unity Catalog Volume and stores the data in Delta format.

### Key Operations

- Read CSV data using PySpark.
- Preserve the original data structure.
- Add source file path and ingestion timestamp metadata.
- Store raw data in a Bronze Delta table.
- Maintain source traceability for downstream processing.

### Bronze Layer Architecture

![Databricks Bronze Layer - Avocado CSV Ingestion](https://raw.githubusercontent.com/bodhaiatharva098-ab0012/Databricks-/5828b6658be4a4d4bff50a4a8619588a85c9c8cc/Databricks%20Bronze%20Layer%20Infographic.jpg)

### Bronze Table Screenshot

![Bronze Table](https://github.com/bodhaiatharva098-ab0012/Databricks-/blob/main/bronze%20table.jpg?raw=true)

## 2. Silver Layer – Data Cleaning and Validation

**Table:** `workspace.ab.silver_avocado`

The Silver layer processes raw data into a cleaner, standardized dataset suitable for analysis. It removes unnecessary columns, standardizes column names, converts data types, and applies data quality checks.

### Key Operations

- Remove unnecessary CSV index columns.
- Standardize column names.
- Convert the date column to a proper date type.
- Clean text values and handle blank values where implemented.
- Remove invalid records and duplicates where implemented.
- Validate numeric values before downstream analysis.
- Save cleaned data as a Delta table.

### Silver Layer Architecture

![Databricks Silver Layer - Avocado Data Processing](https://raw.githubusercontent.com/bodhaiatharva098-ab0012/Databricks-/5828b6658be4a4d4bff50a4a8619588a85c9c8cc/Databricks%20Silver%20Layer%20Avocado%20Pipeline.jpg)

### Silver Table Screenshot

![Silver Table](https://github.com/bodhaiatharva098-ab0012/Databricks-/blob/main/silver.jpg?raw=true)

## 3. Gold Layer – Business Aggregation and Analytics

**Table:** `workspace.ab.gold_avocado`

The Gold layer transforms cleaned Silver data into aggregated business metrics. It groups avocado sales by year, avocado type, and region to support business analysis and reporting.

### Key Operations

- Group data by year, type, and region.
- Calculate total sales volume.
- Calculate average, minimum, and maximum avocado prices.
- Aggregate total, small, large, and extra-large bag volumes.
- Count records for each group.
- Round numeric values for clear reporting.
- Save the results as a Gold Delta table.

### Gold Layer Architecture

![Databricks Gold Layer - Avocado Business Analytics](https://github.com/bodhaiatharva098-ab0012/Databricks-/blob/main/Databricks%20Gold%20Layer%20Pipeline%20Infographic.jpg?raw=true)

### Gold Table Screenshot

![Gold Table](https://github.com/bodhaiatharva098-ab0012/Databricks-/blob/main/gold.jpg?raw=true)

## Data Source

The source dataset is stored in a Databricks Unity Catalog Volume.

```text
/Volumes/workspace/ab/sql/avocado.csv
```

The dataset contains avocado sales information, including prices, sales volume, bag volumes, avocado type, year, and region.

## Tables Created

| Layer | Table Name | Purpose |
|---|---|---|
| Bronze | `workspace.ab.bronze_avocado` | Raw ingested data |
| Silver | `workspace.ab.silver_avocado` | Cleaned and standardized data |
| Gold | `workspace.ab.gold_avocado` | Aggregated business metrics |

## Project Structure

```text
Databricks-Avocado-ETL/
│
├── README.md
└── images/
    ├── bronze_layer.jpg
    ├── silver_layer.jpg
    └── gold_layer.jpg
```

The diagrams may be stored in the repository or referenced directly through their GitHub URLs. The source CSV can remain in the Databricks Volume instead of being uploaded to GitHub.

## How to Run the Project

1. Open your Databricks workspace.
2. Verify that the Unity Catalog Volume contains `avocado.csv`.
3. Run the Bronze-layer notebook to ingest raw data.
4. Run the Silver-layer notebook to clean and validate the data.
5. Run the Gold-layer notebook to aggregate business metrics.
6. Verify the three Delta tables using Databricks SQL.
7. Connect Power BI to Databricks if you want to create interactive dashboards.

## Expected Results

- Raw avocado data stored in the Bronze layer.
- Cleaned and standardized records in the Silver layer.
- Aggregated sales and price metrics in the Gold layer.
- Data prepared for business intelligence and visualization.

## Future Enhancements

- Automate the pipeline using Databricks Workflows.
- Add incremental data processing.
- Implement detailed data quality checks.
- Create interactive Power BI dashboards.
- Schedule regular data refreshes.
- Add monitoring and error handling.

## Author

**Atharva Bodhai**

GitHub: [bodhaiatharva098-ab0012](https://github.com/bodhaiatharva098-ab0012)

## License

Add an appropriate open-source license if you intend to distribute or reuse this project publicly.
