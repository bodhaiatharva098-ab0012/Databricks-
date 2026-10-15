# Databricks notebook source

from pyspark.sql import functions as F

# Source CSV file
source_path = "/Volumes/workspace/ab/sql/avocado.csv"

# Bronze table
bronze_table = "workspace.ab.bronze_avocado"

# Read raw CSV data
df_bronze = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .option("escape", '"')
    .load(source_path)
)

# Add ETL metadata
df_bronze = (
    df_bronze
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingestion_timestamp", F.current_timestamp())
)

# Display raw data
display(df_bronze)

# Save to Bronze Delta table
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .saveAsTable(bronze_table)
)

print("Bronze layer loaded successfully!")
print("Bronze table:", bronze_table)
print("Total rows:", df_bronze.count())
