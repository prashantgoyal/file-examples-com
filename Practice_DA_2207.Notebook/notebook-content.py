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

# # Script for Data Quality checks

# CELL ********************

#importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark Session
spark = SparkSession.builder.appName("FabricSchemaMigration").getOrCreate()

print("sesion intialized")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS sales_raw.bronze_orders (
# MAGIC   order_id          STRING,
# MAGIC   customer_id       STRING,
# MAGIC   customer_name     STRING,
# MAGIC   email             STRING,
# MAGIC   region            STRING,          -- North America | Europe | APAC | LATAM
# MAGIC   order_date        DATE,
# MAGIC   product_category  STRING,          -- Apparel | Footwear | Electronics | Home & Kitchen |
# MAGIC                                       -- Office Supplies | Furniture | Beauty | Sports & Outdoors
# MAGIC   product_name      STRING,
# MAGIC   quantity          INT,
# MAGIC   unit_price        DECIMAL(10,2),
# MAGIC   discount_pct      INT,             -- 0, 5, 10, 15, 20
# MAGIC   total_amount      DECIMAL(10,2),   -- (quantity * unit_price) - discount
# MAGIC   sales_channel     STRING,          -- Online | Retail Store | Partner Reseller | Mobile App
# MAGIC   payment_method    STRING,          -- Credit Card | Debit Card | PayPal | Gift Card | Apple Pay
# MAGIC   order_status      STRING,          -- fulfilled | processing | shipped | cancelled | returned
# MAGIC   ship_date         DATE,
# MAGIC   shipping_zip      STRING
# MAGIC )
# MAGIC USING DELTA ;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_schema = "sales_raw"
target_table = "bronze_orders"
df = spark.read.csv("Files/customer_orders_1000.csv", header=True, inferSchema=True)

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_schema}.{target_table}")


print("Files data is inserted in to delta table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Apply transformations if needed

from pyspark.sql import functions as F

# Example: Rename columns, cast datatypes, drop duplicates
df_transformed = df.withColumnRenamed("order_id", "orderid") \
                  .withColumnRenamed("customer_id", "customerid") \
                  .dropDuplicates()\
                  .dropna()\
                  .withColumn("Load_Datetime",F.current_timestamp())

display(df_transformed)
print("applied trnasformations on it")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

clean_schema = "sales_clean"
clean_table = "orders_clean"

df_transformed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{clean_schema}.{clean_table}")

print("written data into target tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM PysparkTest03July2026.sales_clean.orders_clean LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_schema = "sales"
silver_table = "orders"
silver_lakehouse = "silver_DA_23_07_2026"
df_transformed1 = df_transformed.drop("Load_Datetime")
df_transformed1.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_lakehouse}.{silver_schema}.silver_{silver_table}")

print("written data into silver schema and silver tables")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
