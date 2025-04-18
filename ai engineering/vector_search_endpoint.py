# Databricks notebook source
# Define your vector search endpoint name
vs_endpoint_name = '<your_vector_search_endpoint>'

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

# Restart Python environment to apply new libraries (Databricks specific)
dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

vsc = VectorSearchClient(disable_notice=True)

# COMMAND ----------

# Create a new vector search endpoint
vsc.create_endpoint(name=vs_endpoint_name, endpoint_type="STANDARD")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE <your_catalog>.<your_schema>.product_final 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# Table and index definitions
source_table_fullname = "<your_catalog>.<your_schema>.product_final"
vs_index_fullname = "<your_catalog>.<your_schema>.product_final_vs_index"

# Create or sync the index
vsc.create_delta_sync_index(
    endpoint_name=vs_endpoint_name,
    index_name=vs_index_fullname,
    source_table_name=source_table_fullname,
    pipeline_type="TRIGGERED",  # Sync needs to be manually triggered
    primary_key="Uniq_Id",
    embedding_dimension=1024,  # Match this with your embedding model output
    embedding_vector_column="product_description_embedding"
)

# COMMAND ----------

import mlflow.deployments

# Replace with your actual deployment target (e.g., "databricks")
deploy_client = mlflow.deployments.get_deploy_client("<your_deployment_target>")

question = "Do you have organic skincare products with anti-aging properties?"

# Replace with your actual model endpoint name
response = deploy_client.predict(endpoint="<your_embedding_model>", inputs={"input": [question]})
embeddings = [e["embedding"] for e in response.data]

# COMMAND ----------

# Vector search index name
vs_index_fullname = "<your_catalog>.<your_schema>.product_final_vs_index"

# COMMAND ----------

from pprint import pprint

# Perform vector similarity search
results = vsc.get_index(vs_endpoint_name, vs_index_fullname).similarity_search(
    query_vector=embeddings[0],
    columns=["Product_Title", "Product_Description"],
    num_results=5
)

# Format result to align with reranker library format (optional)
passages = []
for doc in results.get("result", {}).get("data_array", []):
    new_doc = {"file": doc[0], "text": doc[1]}
    passages.append(new_doc)

pprint(passages)
