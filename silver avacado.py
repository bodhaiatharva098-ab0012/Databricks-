# Databricks notebook source
from pyspark.sql import functions as F

# ============================================================
# 1. FILE AND TABLE PATHS
# ============================================================

csv_file = "/Volumes/workspace/ab/sql/avocado.csv"

bronze_table = "workspace.ab.bronze_avocado"
silver_table = "workspace.ab.silver_avocado"


# ============================================================
# 2. READ CSV FILE
# ============================================================

df = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .option("mode", "PERMISSIVE")
         .load(csv_file)
)

print("CSV file loaded successfully!")
print("Total columns:", len(df.columns))

display(df)


# ============================================================
# 3. CLEAN COLUMN NAMES
# ============================================================

# Remove leading/trailing spaces and replace spaces with underscores
for old_name in df.columns:
    new_name = (
        old_name.strip()
                .replace(" ", "_")
                .replace(".", "_")
                .lower()
    )

    if old_name != new_name:
        df = df.withColumnRenamed(old_name, new_name)

# Remove the unnecessary CSV index column
if "unnamed:_0" in df.columns:
    df = df.drop("unnamed:_0")

# Handle the index column after column-name cleaning
if "unnamed_0" in df.columns:
    df = df.drop("unnamed_0")


# ============================================================
# 4. TRIM TEXT AND CONVERT BLANK VALUES TO NULL
# ============================================================

for name, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(
            name,
            F.when(
                F.trim(F.col(name)) == "",
                F.lit(None)
            ).otherwise(F.trim(F.col(name)))
        )


# ============================================================
# 5. CONVERT DATA TYPES
# ============================================================

df = (
    df.withColumn(
        "date",
        F.to_date(F.col("date"), "yyyy-MM-dd")
    )
    .withColumn("averageprice", F.col("averageprice").cast("double"))
    .withColumn("total_volume", F.col("total_volume").cast("double"))
    .withColumn("4046", F.col("`4046`").cast("double"))
    .withColumn("4225", F.col("`4225`").cast("double"))
    .withColumn("4770", F.col("`4770`").cast("double"))
    .withColumn("total_bags", F.col("total_bags").cast("double"))
    .withColumn("small_bags", F.col("small_bags").cast("double"))
    .withColumn("large_bags", F.col("large_bags").cast("double"))
    .withColumn("xlarge_bags", F.col("xlarge_bags").cast("double"))
    .withColumn("year", F.col("year").cast("int"))
)


# ============================================================
# 6. REMOVE INVALID ROWS AND DUPLICATES
# ============================================================

df = df.filter(
    F.col("date").isNotNull()
    & F.col("averageprice").isNotNull()
    & F.col("region").isNotNull()
    & F.col("type").isNotNull()
)

df = df.dropDuplicates()


# ============================================================
# 7. SAVE BRONZE TABLE
# ============================================================

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(bronze_table)
)

print("Bronze table created successfully!")
print("Bronze table:", bronze_table)


# ============================================================
# 8. CREATE SILVER TABLE
# ============================================================

# Read data from Bronze layer
silver_df = spark.table(bronze_table)

# Apply final Silver-layer quality checks
silver_df = silver_df.filter(
    (F.col("averageprice") >= 0)
    & (F.col("total_volume") >= 0)
    & (F.col("total_bags") >= 0)
)

(
    silver_df.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable(silver_table)
)

print("Silver layer created successfully!")
print("Silver table:", silver_table)


# ============================================================
# 9. DISPLAY SILVER DATA
# ============================================================

display(spark.table(silver_table))


# ============================================================
# 10. VERIFY TABLE
# ============================================================

print("Silver row count:", spark.table(silver_table).count())

spark.table(silver_table).printSchema()