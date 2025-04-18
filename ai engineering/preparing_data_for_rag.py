# Databricks notebook source
import pandas as pd

# COMMAND ----------

from pyspark.sql.functions import pandas_udf

@pandas_udf("array<float>")
def get_embedding(contents: pd.Series) -> pd.Series:
    import mlflow.deployments
    deploy_client = mlflow.deployments.get_deploy_client("<YOUR_DEPLOYMENT_TARGET>")  # Replace with your target (e.g., "databricks")
    
    def get_embeddings(batch):
        # Replace the endpoint with your model endpoint name
        response = deploy_client.predict(endpoint="<YOUR_EMBEDDING_ENDPOINT>", inputs={"input": batch})
        return [e["embedding"] for e in response.data]

    max_batch_size = 150
    batches = [contents.iloc[i:i + max_batch_size] for i in range(0, len(contents), max_batch_size)]
    
    all_embeddings = []
    for batch in batches:
        all_embeddings += get_embeddings(batch.tolist())
        
    return pd.Series(all_embeddings)

# COMMAND ----------

# Replace with your actual table or query
initial_query = """
SELECT * FROM <your_catalog>.<your_schema>.products
"""

df = spark.sql(initial_query)

# COMMAND ----------

df_chunk_pd = (df
    .withColumn("product_description_embedding", get_embedding("Product Description"))
    .select("*")
)
display(df_chunk_pd)

# COMMAND ----------

schema = df_chunk_pd.schema
print(schema)

# COMMAND ----------

# Clean up column names for consistency
df_chunk_pd = df_chunk_pd.withColumnRenamed("Combo Offers", "Combo_Offers") \
                         .withColumnRenamed("Stock Availibility", "Stock_Availability") \
                         .withColumnRenamed("Product Title", "Product_Title") \
                         .withColumnRenamed("Product Description", "Product_Description")

# Replace with your actual target table
df_chunk_pd.writeTo("<your_catalog>.<your_schema>.product_info_embedd").createOrReplace()

# COMMAND ----------

# Join with warranty data
warranty_query = """
SELECT pd.*, wr.warranty_info 
FROM <your_catalog>.<your_schema>.product_info_embedd as pd 
LEFT JOIN <your_catalog>.<your_schema>.warranty_info as wr 
ON pd.Uniq_Id = wr.product_id
"""

wr_df = spark.sql(warranty_query)

display(wr_df)

# COMMAND ----------

# Write final data to new table
wr_df.writeTo("<your_catalog>.<your_schema>.product_final").createOrReplace()
