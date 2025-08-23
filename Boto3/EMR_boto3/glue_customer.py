import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="pavan-glue-customer-boto3-database",
    table_name="customer",
    transformation_ctx="S3bucket_node1",
)

S3bucket_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="pavan-glue-customer-boto3-database",
    table_name="sales",
    transformation_ctx="S3bucket_node2",
)


#convert dynamic dataframe into spark dataframe
customer_df=S3bucket_node1.toDF()
sales_df=S3bucket_node2.toDF()


# Transforming 2 dataframes to add segmentation column
combined_df=sales_df.join(customer_df,on="CustomerID",how="inner")

customer_sales=combined_df.withColumn("Total_sales",col("Price")*col("Quantity")).groupBy("CustomerID","Name","Age","Gender").agg(sum("Total_sales").alias("Total_sales"))

# Adding Segmentation column:

customer_segments=customer_sales.withColumn("Segmentation_column",when(col("Total_sales")>=28000,"High Value").when(col("Total_sales")>=20000,"Medium Value")\
                                            .otherwise("Low Value"))

customer_segments=customer_segments.coalesce(1)
# Convert the data frame back to a dynamic frame
dynamic_frame = DynamicFrame.fromDF(customer_segments, glueContext, "dynamic_frame")


# Script generated for node S3 bucket
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="parquet",
    connection_options={"path": "s3://pavan-emr-boto3-customer/glue-output/", "compression": "snappy", "partitionKeys": []},
    transformation_ctx="S3bucket_node2",
)

job.commit()