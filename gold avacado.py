# Databricks notebook source

from pyspark.sql import functions as F

# ============================================================
# 1. DEFINE PATHS AND TABLE NAMES
# ============================================================

csv_path = "/Volumes/workspace/ab/sql/avocado.csv"

silver_table = "workspace.ab.silver_avocado"
gold_table = "workspace.ab.gold_avocado"


# ============================================================
# 2. READ CSV DATA (BRONZE / RAW LAYER)
# ============================================================

df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csv_path)
)

print("CSV data loaded successfully!")
print("Total rows:", df.count())

display(df)


# ============================================================
# 3. CLEAN AND CREATE SILVER LAYER
# ============================================================

# Remove the unnecessary index column if it exists
if "Unnamed: 0" in df.columns:
    df = df.drop("Unnamed: 0")

# Rename columns to simple, consistent names
df = (
    df
    .withColumnRenamed("Date", "date")
    .withColumnRenamed("AveragePrice", "average_price")
    .withColumnRenamed("Total Volume", "total_volume")
    .withColumnRenamed("Total Bags", "total_bags")
    .withColumnRenamed("Small Bags", "small_bags")
    .withColumnRenamed("Large Bags", "large_bags")
    .withColumnRenamed("XLarge Bags", "xlarge_bags")
)

# Convert date column to date type
df = df.withColumn(
    "date",
    F.to_date("date", "yyyy-MM-dd")
)

# Save cleaned data as a Silver Delta table
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print("Silver layer created successfully!")
print("Silver table:", silver_table)


# ============================================================
# 4. READ DATA FROM SILVER LAYER
# ============================================================

silver_df = spark.table(silver_table)


# ============================================================
# 5. CREATE GOLD LAYER (BUSINESS SUMMARY)
# ============================================================

gold_df = (
    silver_df
    .groupBy(
        "year",
        "type",
        "region"
    )
    .agg(
        F.sum("total_volume").alias("total_sales_volume"),
        F.avg("average_price").alias("average_price"),
        F.sum("total_bags").alias("total_bags_sold"),
        F.sum("small_bags").alias("total_small_bags"),
        F.sum("large_bags").alias("total_large_bags"),
        F.sum("xlarge_bags").alias("total_xlarge_bags"),
        F.count("*").alias("total_records"),
        F.min("average_price").alias("minimum_price"),
        F.max("average_price").alias("maximum_price")
    )
)


# ============================================================
# 6. ROUND NUMERIC VALUES
# ============================================================

gold_df = (
    gold_df
    .withColumn(
        "total_sales_volume",
        F.round("total_sales_volume", 2)
    )
    .withColumn(
        "average_price",
        F.round("average_price", 2)
    )
    .withColumn(
        "total_bags_sold",
        F.round("total_bags_sold", 2)
    )
    .withColumn(
        "total_small_bags",
        F.round("total_small_bags", 2)
    )
    .withColumn(
        "total_large_bags",
        F.round("total_large_bags", 2)
    )
    .withColumn(
        "total_xlarge_bags",
        F.round("total_xlarge_bags", 2)
    )
    .withColumn(
        "minimum_price",
        F.round("minimum_price", 2)
    )
    .withColumn(
        "maximum_price",
        F.round("maximum_price", 2)
    )
)


# ============================================================
# 7. SAVE GOLD DATA AS A DELTA TABLE
# ============================================================

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print("Gold layer created successfully!")
print("Gold table:", gold_table)


# ============================================================
# 8. DISPLAY GOLD DATA
# ============================================================

display(
    spark.table(gold_table)
    .orderBy(
        F.desc("total_sales_volume")
    )
)
