import logging
import time
from typing import List, Dict, Any
import psycopg2
from psycopg2 import sql
import boto3
import pandas as pd
from io import StringIO

# --- Logger Setup ---
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PostgresExtractor:
    def __init__(self):
        """Initialize the PostgreSQL extractor."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Initializing extractor.")

        self.pg_config = {
            'host': 'localhost',
            'port': '5432',
            'user': 'postgres',
            'password': '4518',
            'database': 'kaggle_practice'
        }
        
        self.aws_config = {
            's3_bucket': 'golu-aws-project-bucket',
            'aws_access_key_id': '',  
            'aws_secret_access_key': '' 
        }
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_config['aws_access_key_id'],
            aws_secret_access_key=self.aws_config['aws_secret_access_key']
        )
            
    def connect(self):
        """Create a PostgreSQL connection."""
        try:
            return psycopg2.connect(**self.pg_config)
        except Exception as e:
            self.logger.error(f"Error connecting to PostgreSQL: {str(e)}")
            raise
            
    def extract_table(self, table_name: str, schema: str = "public", 
                     where_clause: str = None) -> pd.DataFrame:
        """Extract data from a PostgreSQL table."""
        
        safe_identifier = sql.Identifier(schema, table_name)
        table_name_literal = sql.Literal(f"{schema}.{table_name}")
        
        stats_query = sql.SQL("""
            SELECT COUNT(*) as row_count,
                   pg_size_pretty(pg_total_relation_size({literal_name})) as table_size
            FROM {identifier}
        """).format(literal_name=table_name_literal, identifier=safe_identifier)
        
        try:
            with self.connect() as conn:
                # Get table statistics
                stats_df = pd.read_sql(stats_query.as_string(conn), conn) 
                
                # Build extraction query
                query = sql.SQL("SELECT * FROM {identifier}").format(identifier=safe_identifier)
                
                if where_clause:
                    query = sql.SQL("{base_query} WHERE {where_clause}").format(
                        base_query=query,
                        where_clause=sql.SQL(where_clause)
                    )
                    
                self.logger.info(f"Executing query: {query.as_string(conn)}")
                start_time = time.time()
                df = pd.read_sql(query.as_string(conn), conn)
                duration = time.time() - start_time
                
                self.logger.info(f"Extracted {len(df)} rows in {duration:.2f} seconds")
                return df
                    
        except Exception as e:
            self.logger.error(f"Error extracting data from {schema}.{table_name}: {str(e)}")
            raise
            
    def upload_to_s3(self, df: pd.DataFrame, table_name: str, 
                     partition_date: str = None) -> str:
        """
        Upload extracted data to S3.
        
        Args:
            df (DataFrame): Data to upload
            table_name (str): Source table name
            partition_date (str): Optional partition date (YYYY-MM-DD)
            
        Returns:
            str: S3 URI of the uploaded file
        """
        try:
            bucket_name = self.aws_config['s3_bucket']
            
            # Create S3 key with optional partitioning
            s3_key = f"raw/{table_name}"
            if partition_date:
                s3_key += f"/dt={partition_date}"
            s3_key += f"/{table_name}.csv"
            
            # Convert DataFrame to CSV
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue()
            )
            
            s3_uri = f"s3://{bucket_name}/{s3_key}/{table_name}/"
            self.logger.info(f"Successfully uploaded data to {s3_uri}")
            return s3_uri
            
        except Exception as e:
            self.logger.error(f"Error uploading to S3: {str(e)}")
            raise
            
    def extract_and_upload(self, table_name: str, schema: str = "public",
                          where_clause: str = None, partition_date: str = None) -> str:
        """Extract data from PostgreSQL and upload to S3."""
        try:
            # Extract data from PostgreSQL
            df = self.extract_table(table_name, schema, where_clause)
            
            if df.empty:
                self.logger.warning(f"No data extracted for {schema}.{table_name}. Skipping upload.")
                return None
            
            # Upload to S3
            return self.upload_to_s3(df, table_name, partition_date)
            
        except Exception as e:
            self.logger.error(f"Error in extract and upload process: {str(e)}")
            raise


# Usage example:
if __name__ == "__main__":
    logger.info("Starting standalone extractor test...")
    
    try:
        extractor = PostgresExtractor()
        s3_uri = extractor.extract_and_upload(
            table_name="customers", 
            schema="olist_brazil_e_commerce",
            # where_clause="created_at >= '2025-01-01'" # Optional filter
        )
        
        if s3_uri:
            logger.info(f"Test run complete. File uploaded to: {s3_uri}")
        else:
            logger.info("Test run complete. No file uploaded (table might be empty).")
            
    except Exception as e:
        logger.error(f"Main execution failed: {e}", exc_info=True)