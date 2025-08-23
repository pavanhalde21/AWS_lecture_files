import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

	spark = SparkSession.builder.getOrCreate()

	input_path1 = sys.argv[1]
	input_path2 = sys.argv[2]
	output_path = sys.argv[3]

	sales_df = spark.read.csv(input_path1, header=True, inferSchema=True)

	customer_df = spark.read.csv(input_path2, header=True, inferSchema=True)

	# sales :: Data Product Catagory Price Quantity CustomerID
	# Customer :: CustomerID Name Age Gender

	# Transforming 2 dataframes to add segmentation column

	combined_df=sales_df.join(customer_df, "CustomerID")
	
	customer_sales=combined_df.withColumn("sales", col("Price")*col("Quantity")).groupBy("CustomerID","Name","Age","Gender").agg(sum("Total_sales").alias("Total_sales"))

	# Adding segmentation column :
	customer_segments=customer_sales.withColumn("Segmentation_column", when(col("Total_sales")>=28000, "High Value").when(col("Total_sales")>=20000, "Mid Value").otherwise("Low Value"))

	customer_segments.write.csv(output_path, mode = "overwrite")

	spark.stop()