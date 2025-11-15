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
raw_path = "/Users/pavanhalde/Downloads/customers_11111.csv"

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(raw_path)

print("Raw Data Schema:")
df_raw.printSchema()
df_raw.show(5)

# Sample data structure:
# +--------+------------+----------+----------+
# | OrderID| OrderName  | Customer | Date     |
# +--------+------------+----------+----------+
# | 1      | Order_A    | John     |21-12-1999|
# | 2      | Order_B    | Jane     |22-12-1999|
# +--------+------------+----------+----------+

# ============================================================
# STEP 2: Write data partitioned by DATE to refined layer
# ============================================================
refined_path = "/Users/pavanhalde/Downloads/refined/customer/"

refined_path_csv = "/Users/pavanhalde/Downloads/refined_csv/customer/"

# Convert date string to proper date format for partitioning
df_partitioned = df_raw.withColumn("date_partition", to_date(col("Date"), "dd-MM-yyyy"))

# Write data partitioned by date
df_partitioned.write \
    .mode("overwrite") \
    .partitionBy("date_partition") \
    .parquet(refined_path)

# df_partitioned.write \
#     .mode("overwrite") \
#     .partitionBy("date_partition") \
#     .csv(refined_path)

print(f"\n✅ Data written to {refined_path} partitioned by date_partition")

# Resulting S3 structure:
# s3://my-bucket/refined/customer/date_partition=1999-12-21/part-00000.parquet
# s3://my-bucket/refined/customer/date_partition=1999-12-22/part-00000.parquet
# s3://my-bucket/refined/customer/date_partition=1999-12-23/part-00000.parquet
# s3://my-bucket/refined/customer/date_partition=1999-12-24/part-00000.parquet
# s3://my-bucket/refined/customer/date_partition=1999-12-25/part-00000.parquet

# ============================================================
# STEP 3: Read partitioned data with PARTITION PRUNING
# ============================================================
df_refined = spark.read.parquet(refined_path)

print("\n📊 Refined Data Schema (with partition column):")
df_refined.printSchema()

# ============================================================
# DEMONSTRATION: Partition Pruning
# ============================================================
print("\n" + "="*70)
print("🚀 PARTITION PRUNING - Filter on partition column (date_partition)")
print("="*70)

# Filter on PARTITION COLUMN - Spark will ONLY read specific partitions
df_filtered_partition = df_refined.filter(col("date_partition") == "1999-12-23")

print("\n✅ Query with Partition Pruning (only reads 1 partition):")
print("Filter: date_partition == '1999-12-23'")
df_filtered_partition.explain(True)
df_filtered_partition.show()

# When you filter on partition column, Spark:
# 1. Scans only s3://my-bucket/refined/customer/date_partition=1999-12-23/
# 2. Skips all other date partitions (21, 22, 24, 25)
# 3. This is PARTITION PRUNING - reduces data read significantly

# ============================================================
# DEMONSTRATION: Predicate Pushdown
# ============================================================
print("\n" + "="*70)
print("🚀 PREDICATE PUSHDOWN - Filter on data column (Customer)")
print("="*70)

# Filter on DATA COLUMN (not partition column) - Predicate Pushdown applies
df_filtered_data = df_refined.filter(col("Customer") == "John")

print("\n✅ Query with Predicate Pushdown (filter pushed to file format):")
print("Filter: Customer == 'John'")
df_filtered_data.explain(True)
df_filtered_data.show()

# When you filter on data column, Spark:
# 1. Pushes the filter condition to the Parquet file reader
# 2. Parquet reader applies filter while reading (skips row groups)
# 3. This is PREDICATE PUSHDOWN - reduces data loaded into memory

# ============================================================
# DEMONSTRATION: Combined Optimization
# ============================================================
print("\n" + "="*70)
print("🚀 COMBINED - Partition Pruning + Predicate Pushdown")
print("="*70)

# Filter on BOTH partition column AND data column
df_optimized = df_refined.filter(
    (col("date_partition") == "1999-12-23") &  # Partition Pruning
    (col("Customer") == "John")                 # Predicate Pushdown
)

print("\n✅ Optimized Query (both techniques applied):")
print("Filter: date_partition == '1999-12-23' AND Customer == 'John'")
df_optimized.explain(True)
df_optimized.show()

# This query benefits from BOTH optimizations:
# 1. Partition Pruning: Only reads date_partition=1999-12-23
# 2. Predicate Pushdown: Within that partition, filter Customer=='John' at file level

# ============================================================
# Performance Comparison
# ============================================================
print("\n" + "="*70)
print("📈 PERFORMANCE COMPARISON")
print("="*70)

# WITHOUT optimization (full table scan)
print("\n1️⃣  NO FILTER - Full table scan:")
df_refined.count()

# WITH Partition Pruning only
print("\n2️⃣  PARTITION PRUNING - Filter on partition column:")
df_filtered_partition.count()

# WITH Predicate Pushdown only
print("\n3️⃣  PREDICATE PUSHDOWN - Filter on data column:")
df_filtered_data.count()

# WITH Both optimizations
print("\n4️⃣  BOTH OPTIMIZATIONS - Filter on both:")
df_optimized.count()

# ============================================================
# Key Takeaways
# ============================================================
print("\n" + "="*70)
print("📚 KEY TAKEAWAYS")
print("="*70)
print("""
1. PARTITION PRUNING:
   - Applies when filtering on PARTITION COLUMNS
   - Skips reading entire partitions/folders
   - Reduces data scanned from S3
   - Example: date_partition == '1999-12-23'

2. PREDICATE PUSHDOWN:
   - Applies when filtering on DATA COLUMNS (non-partition)
   - Pushes filter to file format reader (Parquet, ORC)
   - Reduces data loaded into memory
   - Example: Customer == 'John'

3. BEST PRACTICE:
   - Partition by frequently filtered columns (date, region, category)
   - Use columnar formats (Parquet/ORC) for predicate pushdown
   - Combine both for maximum performance
   - Avoid over-partitioning (too many small files)
""")

spark.stop()