# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9ebea46e-2a32-4840-ac5d-6c51562477cb",
# META       "default_lakehouse_name": "PysparkTest03July2026",
# META       "default_lakehouse_workspace_id": "cb3a2f29-6cfd-41ff-a015-d681399cdf4f",
# META       "known_lakehouses": [
# META         {
# META           "id": "9ebea46e-2a32-4840-ac5d-6c51562477cb"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # OneLake CSV Ingestion Pipeline
# This notebook reads a source CSV file from OneLake, removes bad records,
# and writes the clean dataset back to a target folder in OneLake.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.appName("OneLake_CSV_Pipeline").getOrCreate()

print("Spark session initialized")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace these paths with the actual OneLake Files paths for your workspace.
source_file_path = "Files/raw/customer_orders.csv"
target_clean_path = "Files/processed/clean_orders"
target_bad_path = "Files/processed/bad_orders"

# Read the source CSV from OneLake
raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("timestampFormat", "yyyy-MM-dd") \
    .load(source_file_path)

print(f"Source rows read: {raw_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardize and validate key columns.
normalized_df = raw_df \
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd")) \
    .withColumn("quantity", F.col("quantity").cast("int")) \
    .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)")) \
    .withColumn("total_amount", F.col("total_amount").cast("decimal(10,2)"))

required_columns = [
    "order_id",
    "customer_id",
    "order_date",
    "product_name",
    "quantity",
    "unit_price",
    "total_amount"
]

bad_condition = (
    F.col("order_id").isNull() |
    F.col("customer_id").isNull() |
    F.col("order_date").isNull() |
    F.col("product_name").isNull() |
    F.col("quantity").isNull() |
    F.col("unit_price").isNull() |
    F.col("total_amount").isNull() |
    (F.col("quantity") <= 0) |
    (F.col("unit_price") < 0) |
    (F.col("total_amount") < 0) |
    (F.col("total_amount") =!= (F.col("quantity") * F.col("unit_price")))
)

bad_df = normalized_df.filter(bad_condition)
good_df = normalized_df.filter(~bad_condition)

print(f"Bad records identified: {bad_df.count()}")
print(f"Clean records available: {good_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Remove duplicates and add ingestion metadata
clean_df = good_df \
    .dropDuplicates([
        "order_id",
        "customer_id",
        "order_date",
        "product_name"
    ]) \
    .withColumn("ingestion_timestamp", F.current_timestamp())

bad_df = bad_df.withColumn("bad_record_timestamp", F.current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

clean_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(target_clean_path)

bad_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(target_bad_path)

print(f"Clean data written to: {target_clean_path}")
print(f"Bad data written to: {target_bad_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final validation counts
clean_count = spark.read.format("delta").load(target_clean_path).count()
bad_count = spark.read.format("delta").load(target_bad_path).count()

print(f"Final clean dataset count: {clean_count}")
print(f"Final bad dataset count: {bad_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
