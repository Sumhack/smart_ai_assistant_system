# smart_ai_assistant_system

# 🛍️ Smart AI Product Assistant on Databricks

This project builds a **retrieval-augmented generation (RAG)** system that enables semantic search and natural language Q&A over a product catalog using **Databricks**, **MLflow**, **Vector Search**, **LangChain**, and **Gradio**.

---

## 🚀 Project Overview

This solution allows end-users to ask questions like:

> *"Do you have organic skincare products with anti-aging properties?"*

The assistant then retrieves the most relevant product descriptions and generates a concise, helpful answer using a language model.

---

## 📐 Architecture

```text
                   ┌────────────────────┐
                   │  Product Catalog   │
                   │ Delta Table (UC)   │
                   └────────┬───────────┘
                            │
                            ▼
                ┌──────────────────────────┐
                │ Embedding UDF (MLflow)   │
                │ batched via pandas_udf() │
                └────────┬─────────────────┘
                         ▼
              ┌──────────────────────────────┐
              │ Vector Index (Databricks VS) │
              └────────┬─────────────────────┘
                       ▼
             ┌────────────────────────────┐
             │ LangChain RAG Pipeline     │
             │ - Retriever (VectorSearch) │
             │ - LLM (ChatDatabricks)     │
             └────────┬───────────────────┘
                      ▼
             ┌────────────────────────────┐
             │ MLflow Model Logging       │
             │ & Unity Catalog Registry   │
             └────────┬───────────────────┘
                      ▼
             ┌────────────────────────────┐
             │ Gradio UI (Frontend Demo)  │
             └────────────────────────────┘
