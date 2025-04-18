# Databricks notebook source
# MAGIC %pip install databricks-sdk
# MAGIC %pip install databricks-cli --upgrade

# COMMAND ----------

# Restart the Python environment (Databricks specific)
dbutils.library.restartPython()

# COMMAND ----------

# CLI configuration for Databricks (handled locally; do not include secrets in code)
!databricks configure --token

# COMMAND ----------

# (Intentionally left blank – can include setup validation/logging if needed)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, 
    ServedModelInput, 
    ServedModelInputWorkloadSize
)

# Initialize workspace client with authentication
w = WorkspaceClient()

# Define serving endpoint configuration
endpoint_config = EndpointCoreConfigInput(
    name="<your_endpoint_name>",
    served_models=[
        ServedModelInput(
            model_name="<your_model_path>",  # e.g., <catalog>.<schema>.<model>
            model_version="1",  
            workload_size=ServedModelInputWorkloadSize.SMALL,  
            scale_to_zero_enabled=True  
        )
    ]
)

# Create serving endpoint
endpoint = w.serving_endpoints.create(
    name=endpoint_config.name, 
    config=endpoint_config
)

print(f"Endpoint created: {endpoint.name}")

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("<your_target>")  # e.g., "databricks"
endpoint = client.create_endpoint(
    name="<your_endpoint_name>",
    config={
        "served_entities": [
            {
                "name": "<your_served_entity_name>",
                "entity_name": "<your_model_path>",  # e.g., <catalog>.<schema>.<model>
                "entity_version": "1",
                "workload_size": "Small",
                "scale_to_zero_enabled": True
            }
        ]
    }
)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedModelInput

# Initialize client
w = WorkspaceClient()

# Re-define endpoint config (example reuse)
endpoint_config = EndpointCoreConfigInput(
    name="<your_endpoint_name>",
    served_models=[
        ServedModelInput(
            model_name="<your_model_path>",
            model_version="1"
        )
    ]
)

# Create endpoint
endpoint = w.serving_endpoints.create(
    name=endpoint_config.name, 
    config=endpoint_config
)

print(f"Endpoint created: {endpoint.name}")

# COMMAND ----------

from mlflow.models import infer_signature
import mlflow
import langchain
import os

# COMMAND ----------

# Set model registry URI
mlflow.set_registry_uri("databricks-uc")
model_name = "<your_model_path>"

# COMMAND ----------

# Install latest SDK if needed
%pip install databricks-sdk --upgrade

# COMMAND ----------

# Restart to reflect new package installs
dbutils.library.restartPython()

# COMMAND ----------

from databricks import agents

# Deploy model using Databricks agents
agents.deploy("<your_model_path>", "1")
