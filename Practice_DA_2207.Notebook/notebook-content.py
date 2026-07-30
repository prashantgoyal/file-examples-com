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
# META         },
# META         {
# META           "id": "f52293ba-5858-4845-9b6a-ebc21ef00830"
# META         },
# META         {
# META           "id": "178dbfd6-acea-4c12-8ff6-97ff176b8a27"
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

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS sales_clean.ETLControl_01 (
# MAGIC   Table_name        STRING,
# MAGIC   Table_count           decimal(18,4),
# MAGIC   count_timestamp   TIMESTAMP 
# MAGIC    
# MAGIC )
# MAGIC USING DELTA ;
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_schema = "sales_raw"
target_table = "bronze_orders"
df = spark.read.csv("Files/customer_orders_1000.csv", header=True, inferSchema=True)

print("Count of dataframe", df.count())
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_schema}.{target_table}")


print("Files data is inserted in to delta table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Doing Data checks based on Pyspark
duplicate_rows = df.count() - df.dropDuplicates().count()
print(f"Number of duplicate rows: {duplicate_rows}")


#Checking for unique values 

#for column in df.columns:
#    print(f"{column}: {df.select(column).distinct().count()} distinct values")

#Taking Categorical product_category  count. 
#df.groupBy("product_category").count().show()

# Match 'YYYY-MM-DD' strict numerical pattern
regex_pattern = r"^\d{4}-\d{2}-\d{2}$"

df_regex = df.withColumn(
    "matches_regex",F.col("order_date").rlike(regex_pattern)
)
df_regex.show()

# taking count based on active and inactive records
#active_count = df.filter(df.order_status == "shipped").count()

#print(f"Number of true records: {active_count}") 

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

# CELL ********************

import pandas as pd

wrangler_sample_df = pd.read_csv("https://aka.ms/wrangler/titanic.csv")
display(wrangler_sample_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "editable": true
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp

table_name  = "silver_DA_23_07_2026.sales.silver_orders"
def get_table_count(table_name: str) -> int:
    """
    Function to return the count of rows in a given table.
    
    Parameters:
        table_name (str): Name of the table to count rows from.
    
    Returns:
        int: Number of rows in the table.
    """
    df = spark.table(table_name)  # Load table into DataFrame
    table_count = df.count()
    # Create DataFrame with multiple columns
    df = spark.createDataFrame([(table_name, table_count)], 
                            ["table_name", "table_count",])\
    .withColumn("count_timestamp", current_timestamp())
            
    df.show()
    df.write.mode("append").insertInto("sales_clean.ETLControl_01")

get_table_count(table_name)


#df.write.mode("append").insertInto("sales_clean.ETLControl")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from sales_clean.ETLControl_01 ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define the function
def clean_text(text):
    if text is None or str(text).strip() == "" or str(text).strip().upper() == "NULL":
        return ""
    text_str = str(text)
    cleaned = re.sub(r'[^a-zA-Z0-9\s,.-]', '', text_str).replace('_', ' ').strip()   
    return cleaned

# Register as UDF
clean_text_udf = udf(clean_text, StringType())


# Example usage
data = [("Hello_World!",), ("NULL",), ("Test@123",), (None,)]
df = spark.createDataFrame(data, ["raw_text"])


df_cleaned = df.withColumn("cleaned_text", clean_text_udf(df["raw_text"]))
df_cleaned.show(truncate=False)

clean_schema = "sales_clean"
clean_table = "orders_clean"
df_transformed.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(f"{clean_schema}.clean_{clean_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
