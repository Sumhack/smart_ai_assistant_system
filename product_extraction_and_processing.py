# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CreateEmptyTable").getOrCreate()

# Catalog and schema configuration
catalog_name = "<your_catalog_name>"
schema_name = "<your_schema_name>"
table_name = "product_data"

# Create schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Create Delta table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
        uniqid STRING,
        Category STRING,
        product_title STRING,
        productdescription STRING,
        brand STRING,
        pack_size_quantity STRING,
        mrp FLOAT,
        price FLOAT,
        offers STRING,
        combo_offers STRING,
        stock_availibility STRING,
        product_title_needs_translation BOOLEAN,
        product_description_needs_translation BOOLEAN
    )
    USING DELTA
""")

print(f"Empty table '{catalog_name}.{schema_name}.{table_name}' created successfully!")

# COMMAND ----------

# Databricks notebook source
from pyspark.sql.functions import col, trim

# Azure Storage configuration
storage_account_name = "<your_storage_account_name>"
container_name = "<your_container_name>"
file_path = "productsdata.csv"
sas_token = "<your_sas_token>"

# Table details
catalog_name = "<your_catalog_name>"
schema_name = "<your_schema_name>"
table_name = "product_data"

# WASBS URL
wasbs_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{file_path}"

# Set Spark config with SAS token
spark.conf.set(
    f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net", 
    sas_token.lstrip('?')
)

# Read the CSV
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("mode", "DROPMALFORMED") \
    .load(wasbs_path)

# Column mapping
column_mapping = {
    "uniqid": "uniqid",
    "Category": "Category",
    "Product Title": "product_title",
    "Product Description": "productdescription",
    "Brand": "brand",
    "Pack Size Or Quantity": "pack_size_quantity",
    "Mrp": "mrp",
    "Price": "price",
    "Offers": "offers",
    "Combo Offers": "combo_offers",
    "Stock Availibility": "stock_availibility",
    "Product Title_Needs_Translation": "product_title_needs_translation",
    "Product Description_Needs_Translation": "product_description_needs_translation"
}

# Transform columns
df_transformed = df.select([
    trim(col(csv_col)).alias(table_col) 
    for csv_col, table_col in column_mapping.items()
])

# Cast column types
df_final = df_transformed \
    .withColumn("mrp", col("mrp").cast("float")) \
    .withColumn("price", col("price").cast("float")) \
    .withColumn("product_title_needs_translation", col("product_title_needs_translation").cast("boolean")) \
    .withColumn("product_description_needs_translation", col("product_description_needs_translation").cast("boolean"))

# Write to Delta table
table_path = f"{catalog_name}.{schema_name}.{table_name}"
print(f"Loading data into table: {table_path}")

df_final.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(table_path)

print("\nValidating the data load...")
print(f"Total records loaded: {df_final.count()}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Spark session
spark = SparkSession.builder.appName("CreateEmptyTable").getOrCreate()

# Catalog/schema setup
catalog_name = "<your_catalog_name>"
schema_name = "<your_schema_name>"
table_name = "product_data_refined"

# Create schema and table
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
        uniqid STRING,
        Category STRING,
        product_title STRING,
        productdescription STRING,
        brand STRING,
        pack_size_quantity STRING,
        mrp FLOAT,
        price FLOAT,
        offers STRING,
        combo_offers STRING,
        stock_availibility STRING,
        product_title_needs_translation BOOLEAN,
        product_description_needs_translation BOOLEAN
    )
    USING DELTA
""")

print(f"Empty table '{catalog_name}.{schema_name}.{table_name}' created successfully!")

# COMMAND ----------

# MAGIC %pip install langdetect

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import langdetect
import re

# Language detection UDF
def is_english(text):
    if not text:
        return False

    clean_text = re.sub(r'[0-9®™°]+', '', text)
    clean_text = re.sub(r'[^\w\s]', '', clean_text)
    words = [word for word in clean_text.split() if len(word) > 2]
    clean_text = ' '.join(words)

    try:
        if len(clean_text.split()) < 3:
            return False
        return langdetect.detect(clean_text) == 'en'
    except:
        return False

is_english_udf = F.udf(is_english, BooleanType())

# Query to select relevant rows
initial_query = """
SELECT * FROM <your_catalog_name>.<your_schema_name>.product_data 
WHERE product_title_needs_translation = true 
   OR product_description_needs_translation = true
"""

# Read pre-filtered data
df = spark.sql(initial_query)

# Apply additional language check
english_df = df.filter(
    (is_english_udf(F.col("product_title")) == True) & 
    (is_english_udf(F.col("productdescription")) == True)
)

# Write filtered data to new table
refined_table_path = f"{catalog_name}.{schema_name}.product_data_refined"

english_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(refined_table_path)

print("Refined product data saved.")
