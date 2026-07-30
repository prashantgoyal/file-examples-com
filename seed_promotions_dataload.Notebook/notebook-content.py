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

# seed_promotions_dataload SCD2 pipeline
#
# Load source promotion data from `/lakehouse/default/Files/seed_promotions.csv`,
# apply Slowly Changing Dimension Type 2 logic, and write the promotion history into a Delta path.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.appName("seed_promotions_dataload").getOrCreate()
print("Spark session initialized")

# CELL ********************

source_file = "/lakehouse/default/Files/seed_promotions.csv"
target_path = "Files/silver/scd2_seed_promotions"

raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("timestampFormat", "yyyy-MM-dd") \
    .load(source_file)

print(f"Source rows loaded: {raw_df.count()}")

# CELL ********************

staging_df = raw_df \
    .select(
        "promotion_id",
        "promotion_name",
        "promotion_type",
        "discount_pct",
        "start_date",
        "end_date"
    ) \
    .withColumn("start_date", F.to_date("start_date", "yyyy-MM-dd")) \
    .withColumn("end_date", F.to_date("end_date", "yyyy-MM-dd")) \
    .withColumn("discount_pct", F.col("discount_pct").cast("double")) \
    .withColumn("effective_start_date", F.current_date()) \
    .withColumn("effective_end_date", F.lit(None).cast("date")) \
    .withColumn("current_flag", F.lit(True)) \
    .withColumn("record_hash", F.md5(F.concat_ws("||",
        F.col("promotion_id"),
        F.col("promotion_name"),
        F.col("promotion_type"),
        F.col("discount_pct"),
        F.col("start_date"),
        F.col("end_date")
    )))

# CELL ********************

if DeltaTable.isDeltaTable(spark, target_path):
    target_table = DeltaTable.forPath(spark, target_path)
    current_df = spark.read.format("delta").load(target_path).filter("current_flag = true")

    changed_df = staging_df.alias("src").join(
        current_df.alias("t"),
        on=["promotion_id"],
        how="left_outer"
    ).filter(
        "t.promotion_id IS NULL OR src.record_hash <> t.record_hash"
    ).select("src.*")

    changed_count = changed_df.count()
    if changed_count > 0:
        changed_keys = changed_df.select("promotion_id").distinct()

        target_table.alias("t").merge(
            changed_keys.alias("c"),
            "t.promotion_id = c.promotion_id AND t.current_flag = true"
        ).whenMatchedUpdate(
            set={
                "current_flag": "false",
                "effective_end_date": "current_date()"
            }
        ).execute()

        changed_df.write.format("delta").mode("append").save(target_path)
        print(f"Applied {changed_count} SCD2 insert/update rows.")
    else:
        print("No changed records found; SCD2 history is up to date.")
else:
    staging_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
    print("Created initial SCD2 history table.")

# CELL ********************

current_count = spark.read.format("delta").load(target_path).filter("current_flag = true").count()
total_count = spark.read.format("delta").load(target_path).count()
print(f"Current active promotions: {current_count}")
print(f"Total history promotions: {total_count}")
