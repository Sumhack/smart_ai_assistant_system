# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CreateEmptyTable").getOrCreate()

# Specify catalog and schema
catalog_name = "smartai"  # Replace with your catalog name
schema_name = "customer_query"  # Replace with your schema name
table_name = "customer_queries_data"

# Create the schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Create the empty table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
        queryid STRING,# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CreateEmptyTable").getOrCreate()

# Specify catalog and schema (replace with your own names)
catalog_name = "<your_catalog_name>"
schema_name = "<your_schema_name>"
table_name = "customer_queries_data"

# Create the schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Create the empty table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
        queryid STRING,
        customer_query STRING,
        query_class STRING
    )
    USING DELTA
""")

print(f"Empty table '{catalog_name}.{schema_name}.{table_name}' created successfully!")

# COMMAND ----------

from pyspark.sql.functions import col, trim

# Configuration for Azure Storage access (replace placeholders with actual values)
storage_account_name = "<your_storage_account_name>"
container_name = "<your_container_name>"
file_path = "<your_file_name>.csv"
sas_token = "<your_sas_token>"

# Table details
catalog_name = "<your_catalog_name>"
schema_name = "<your_schema_name>"
table_name = "customer_queries_data"

# Construct the Azure Blob Storage URL with SAS token
wasbs_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{file_path}"

# Configure Spark to use the SAS token for authentication
spark.conf.set(
    f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net", 
    sas_token.lstrip('?')
)

# Read the CSV file
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("mode", "DROPMALFORMED") \
    .load(wasbs_path)

# Map columns from CSV to target table
column_mapping = {
    "Query ID": "queryid",
    "Customer query": "customer_query",
    "Query Class": "query_class"
}

# Select and rename columns
df_transformed = df.select([
    trim(col(csv_col)).alias(table_col) 
    for csv_col, table_col in column_mapping.items()
])

# Write to the target table
table_path = f"{catalog_name}.{schema_name}.{table_name}"
print(f"Loading data into table: {table_path}")

df_transformed.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(table_path)

# Validate the load
print("\nValidating the data load...")
print(f"Total records loaded: {df_transformed.count()}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CreateEmptyTable").getOrCreate()

# Specify catalog and schema (replace with your own)
catalog_name = "<your_catalog_name>"
schema_name = "<your_schema_name>"
table_name = "customer_queries_data_deduped"

# Create the schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Create the empty table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
        queryid INTEGER,
        customer_query STRING,
        query_class STRING
    )
    USING DELTA
""")

print(f"Empty table '{catalog_name}.{schema_name}.{table_name}' created successfully!")

# Deduplicate and insert into new table
spark.sql(f"""
    INSERT INTO {catalog_name}.{schema_name}.{table_name}
    SELECT queryid, customer_query, query_class
    FROM (
        SELECT *, 
               RANK() OVER (PARTITION BY customer_query ORDER BY queryid ASC) AS rank_num
        FROM (
            SELECT CAST(queryid AS INT) AS queryid, customer_query, query_class
            FROM {catalog_name}.{schema_name}.customer_queries_data
        )
    )
    WHERE rank_num = 1
""")

        customer_query STRING,
        query_class STRING
    )
    USING DELTA
""")

print(f"Empty table '{catalog_name}.{schema_name}.{table_name}' created successfully!")

# COMMAND ----------

# Databricks notebook source
from pyspark.sql.functions import col, trim

# Configuration for Azure Storage access
storage_account_name = "warrantystoredoc"
container_name = "customerqueries"
file_path = "globalmart_customer_queries.csv"
sas_token = "sp=r&st=2024-11-24T09:14:07Z&se=2024-11-24T17:14:07Z&spr=https&sv=2022-11-02&sr=b&sig=wWrfjx9G5heygmlSpa7mRkkRMv3XQFF3IwtIdSlKvmk%3D"

# Table details
catalog_name = "smartai"
schema_name = "customer_query"
table_name = "customer_queries_data"

# Construct the Azure Blob Storage URL with SAS token
wasbs_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{file_path}"

# Configure Spark to use the SAS token for authentication
spark.conf.set(
    f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net", 
    sas_token.lstrip('?')
)

# Read the CSV file
# Note: Using the CSV column names exactly as they appear in your sample
# Read the CSV file with specific options to handle the parsing issues
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("mode", "DROPMALFORMED") \
    .load(wasbs_path)

# Column mapping from CSV to table
# The keys are the CSV column names, values are the target table column names
column_mapping = {
    "Query ID": "queryid",
    "Customer query": "customer_query",
    "Query Class": "query_class"
   
}

# Select and rename columns according to the mapping
df_transformed = df.select([
    trim(col(csv_col)).alias(table_col) 
    for csv_col, table_col in column_mapping.items()
])



# Write to the target table
table_path = f"{catalog_name}.{schema_name}.{table_name}"
print(f"Loading data into table: {table_path}")

df_transformed.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(table_path)

# Validation
print("\nValidating the data load...")
print(f"Total records loaded: {df_transformed.count()}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CreateEmptyTable").getOrCreate()

# Specify catalog and schema
catalog_name = "smartai"  # Replace with your catalog name
schema_name = "customer_query"  # Replace with your schema name
table_name = "customer_queries_data_deduped"

# Create the schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Create the empty table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
        queryid INTEGER,
        customer_query STRING,
        query_class STRING
    )
    USING DELTA
""")

print(f"Empty table '{catalog_name}.{schema_name}.{table_name}' created successfully!")

spark.sql(f"""
          Insert into {catalog_name}.{schema_name}.{table_name}

          select queryid,customer_query,query_class from (select *, rank() over (partition by customer_query order by queryid asc ) as rank_num from 

(select cast(queryid as numeric) as queryid,customer_query,query_class  from smartai.customer_query.customer_queries_data )) where rank_num = 1
 """)
