# dags/brazil_ecom_pipeline.py
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Amazon provider operators
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

import json
import os
import boto3
import mysql.connector

# -----------------------
# Config (edit to your env)
# -----------------------
REGION = "us-east-1"
BUCKET = "golu-aws-project-bucket"
DB_NAME = "brazil_e_commerce"
RAW_TABLE = "raw2_customers"           # Glue catalog table for RAW
REFINED_TABLE = "refined_customer_refined"
GLUE_DB = "mydb_01"                    # Glue Data Catalog database
GLUE_JOB_NAME = "glue_job_07"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT = f"s3://{BUCKET}/athena-results/"
RAW_BASE_PREFIX = "raw/RDS/brazil_e_commerce/customers"
REFINED_BASE_PREFIX = "output/brazil_e_commerce/customer_refined"

# Optional: name of a Secrets Manager secret with your RDS creds
RDS_SECRET_NAME = os.getenv("RDS_SECRET_NAME", "brazil-ecom-rds-mysql-secret")

# Crawler names to REUSE (do NOT create per-day)
RAW_CRAWLER_NAME = "crawler_raw_customers"
REFINED_CRAWLER_NAME = "crawler_refined_customers"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# -----------------------
# Define DAG (no context manager)
# -----------------------
dag = DAG(
    dag_id="brazil_ecom_daily_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["glue", "athena", "rds", "s3"],
)

# -----------------------
# Task 1: Extract RDS → S3 (dt={{ ds }})
# -----------------------
def extract_rds_to_s3(**context):
    """
    Connects to RDS MySQL and writes customers to S3:
    s3://<bucket>/<RAW_BASE_PREFIX>/dt={{ ds }}/customers.csv
    Returns XCom dict with paths.
    """
    ds = context["ds"]  # YYYY-MM-DD
    s3_key = f"{RAW_BASE_PREFIX}/dt={ds}/customers.csv"

    # Fetch DB creds from Secrets Manager
    sm = boto3.client("secretsmanager", region_name=REGION)
    secret = json.loads(sm.get_secret_value(SecretId=RDS_SECRET_NAME)["SecretString"])
    conn = mysql.connector.connect(
        host=secret["host"],
        port=int(secret.get("port", 3306)),
        user=secret["username"],
        password=secret["password"],
        database=DB_NAME,
        connection_timeout=10,
    )
    try:
        query = "SELECT * FROM customers"
        cur = conn.cursor()
        cur.execute(query)

        # Stream to CSV in-memory
        import csv, io
        buf = io.StringIO()
        writer = csv.writer(buf)
        # headers
        cols = [d[0] for d in cur.description]
        writer.writerow(cols)
        # rows
        for row in cur:
            writer.writerow(row)

        # Upload to S3
        s3 = boto3.client("s3", region_name=REGION)
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=buf.getvalue().encode("utf-8"),
            StorageClass="STANDARD_IA",
            ServerSideEncryption="AES256",
            Metadata={"source": "rds-mysql", "table": "customers", "ds": ds},
        )

        # Return for XCom
        return {
            "ds": ds,
            "raw_s3_path": f"s3://{BUCKET}/{RAW_BASE_PREFIX}/dt={ds}/",
            "refined_s3_path": f"s3://{BUCKET}/{REFINED_BASE_PREFIX}/dt={ds}/",
        }
    finally:
        conn.close()

extract = PythonOperator(
    task_id="extract_rds_to_s3",
    python_callable=extract_rds_to_s3,
    dag=dag,
)

# -----------------------
# Task 2: Crawl RAW (reuse one crawler; point at dt path)
# -----------------------
raw_crawler = GlueCrawlerOperator(
    task_id="crawl_raw_dt",
    wait_for_completion=True,
    config={
        "Name": RAW_CRAWLER_NAME,
        "Role": "arn:aws:iam::180294202865:role/glue_role_to_give_full_access_to_s3",
        "DatabaseName": GLUE_DB,
        "Description": "Daily crawl of RAW customers dt path",
        "Targets": {
            "S3Targets": [
                {
                    "Path": "{{ ti.xcom_pull(task_ids='extract_rds_to_s3')['raw_s3_path'] }}",
                }
            ]
        },
        "TablePrefix": "raw2_",
        "RecrawlPolicy": {"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
        "Configuration": json.dumps({"Version": 1.0, "CreatePartitionIndex": True}),
    },
    dag=dag,
)

# -----------------------
# Task 3: Run Glue Job (reads RAW, writes REFINED dt path)
# -----------------------
glue_etl = GlueJobOperator(
    task_id="run_glue_job",
    job_name=GLUE_JOB_NAME,
    wait_for_completion=True,
    script_args={
        "--database": GLUE_DB,
        "--raw_table": RAW_TABLE,
        "--refined_table": REFINED_TABLE,
        "--input_s3": "{{ ti.xcom_pull(task_ids='extract_rds_to_s3')['raw_s3_path'] }}",
        "--output_s3": "{{ ti.xcom_pull(task_ids='extract_rds_to_s3')['refined_s3_path'] }}",
        "--run_date": "{{ ds }}",
    },
    verbose=True,
    dag=dag,
)

# -----------------------
# Task 4: Crawl REFINED (reuse one crawler; point at dt path)
# -----------------------
refined_crawler = GlueCrawlerOperator(
    task_id="crawl_refined_dt",
    wait_for_completion=True,
    config={
        "Name": REFINED_CRAWLER_NAME,
        "Role": "arn:aws:iam::180294202865:role/glue_role_to_give_full_access_to_s3",
        "DatabaseName": GLUE_DB,
        "Description": "Daily crawl of REFINED customers dt path",
        "Targets": {
            "S3Targets": [
                {
                    "Path": "{{ ti.xcom_pull(task_ids='extract_rds_to_s3')['refined_s3_path'] }}",
                }
            ]
        },
        "TablePrefix": "refined_",
        "RecrawlPolicy": {"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
        "Configuration": json.dumps({"Version": 1.0, "CreatePartitionIndex": True}),
    },
    dag=dag,
)

# -----------------------
# Task 5: Athena validations
# -----------------------
athena_row_counts = AthenaOperator(
    task_id="athena_compare_row_counts",
    database=GLUE_DB,
    workgroup=ATHENA_WORKGROUP,
    output_location=ATHENA_OUTPUT,
    query=f"""
    SELECT 'Raw Table' AS source, COUNT(*) AS row_count FROM {GLUE_DB}.{RAW_TABLE}
    UNION ALL
    SELECT 'Refined Table (SP only)' AS source, COUNT(*) AS row_count FROM {GLUE_DB}.{REFINED_TABLE};
    """,
    dag=dag,
)

athena_city_upper = AthenaOperator(
    task_id="athena_city_uppercase_check",
    database=GLUE_DB,
    workgroup=ATHENA_WORKGROUP,
    output_location=ATHENA_OUTPUT,
    query=f"""
    SELECT CASE WHEN customer_city = UPPER(customer_city) THEN 'PASS' ELSE 'FAIL' END AS status,
           COUNT(*) AS cnt
    FROM {GLUE_DB}.{REFINED_TABLE}
    GROUP BY 1;
    """,
    dag=dag,
)

# -----------------------
# Dependencies
# -----------------------
extract >> raw_crawler >> glue_etl >> refined_crawler >> [athena_row_counts, athena_city_upper]
