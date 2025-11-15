
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PartitionPruning_PredicatePushdown") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# ============================================================
# STEP 1: Read raw data from S3
# ============================================================
raw_path = '/Users/pavanhalde/Downloads/refined/customer/date_partition=1999-12-23/'

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .parquet(raw_path)

print("Raw Data Schema:")
df_raw.printSchema()
df_raw.show(10)

raw_path = '/Users/pavanhalde/Downloads/refined/customer/'

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .parquet(raw_path)


df_filtered_partition = df_raw.filter(col("date_partition") == "1999-12-23")

print("Raw Data Schema:")
df_filtered_partition.printSchema()
df_filtered_partition.show(10)

df_filtered_partition.explain(True)





