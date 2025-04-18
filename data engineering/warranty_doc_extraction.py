# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CreateEmptyTable").getOrCreate()

# Specify catalog and schema (replace with your own)
catalog_name = "<your_catalog_name>"
schema_name = "<your_schema_name>"
table_name = "warranty_info"

# Create the schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Create the empty table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
        product_id STRING,
        warranty_info STRING
    )
    USING DELTA
""")

print(f"Empty table '{catalog_name}.{schema_name}.{table_name}' created successfully!")

# COMMAND ----------

# Install required libraries (Databricks magic commands)
!pip install pyspark azure-storage-blob delta-spark

# COMMAND ----------

# MAGIC %pip install python-docx

# COMMAND ----------

from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
import re
from docx import Document
import io

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("WarrantyLoader") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# SAS token and Blob Storage details (REPLACE with your own for actual use)
sas_token = "<your_sas_token>"
storage_account_name = "<your_storage_account_name>"
container_name = "<your_container_name>"
blob_service_url = f"https://{storage_account_name}.blob.core.windows.net"

# Blob service client
blob_service_client = BlobServiceClient(account_url=blob_service_url, credential=sas_token)
container_client = blob_service_client.get_container_client(container_name)

# Fetch blob list
blobs = container_client.list_blobs()

# Prepare a list to store extracted data
warranty_data = []

for blob in blobs:
    blob_name = blob.name
    if blob_name.startswith("Warranty_") and blob_name.endswith(".docx"):
        # Extract product_id from filename
        productid_match = re.search(r"Warranty_(.+)\.doc", blob_name)
        print(productid_match)
        if productid_match:
            productid = productid_match.group(1)
            
            # Download the blob content
            blob_client = container_client.get_blob_client(blob_name)
            file_content = blob_client.download_blob().readall()
            
            # Convert bytes to a file-like object
            file_like_object = io.BytesIO(file_content)
            # Extract warranty_info using python-docx
            document = Document(file_like_object)
            warranty_info = "\n".join([para.text for para in document.paragraphs])
            
            # Append to data list
            warranty_data.append((productid, warranty_info))

# Create a DataFrame from the extracted data
warranty_df = spark.createDataFrame(warranty_data, schema=["product_id", "warranty_info"])

# Define the path to your Delta table (replace with your actual target table)
table_path = f"{catalog_name}.{schema_name}.{table_name}"

# Write data to Delta table
warranty_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(table_path)

print("Data successfully loaded into Delta table.")
