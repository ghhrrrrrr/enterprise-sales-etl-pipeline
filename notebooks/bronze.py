from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, lit, col
import pandas as pd
import re

def normalize_column_name(name):
    name = name.replace(' ', '_')   
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    name = re.sub(r'_+', '_', name)
    return name.lower()

schema = StructType([
    StructField("Invoice", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Customer_ID", DoubleType(), True),
    StructField("Country", StringType(), True)
]) 

file_path = "/Volumes/workspace/retail/sales/online_retail_II.csv"

df = spark.read \
      .format("csv") \
      .option("header", "true") \
      .schema(schema) \
      .load(file_path) \
      .withColumn("ingestion_time", current_timestamp()) \
      .withColumn("file_path", col("_metadata.file_path")) \
      .withColumn("env", lit("dev"))

final_columns = [normalize_column_name(c) for c in df.columns]
bronze_df = df.toDF(*final_columns)

table_name = 'workspace.retail.bronze_sales'

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)
