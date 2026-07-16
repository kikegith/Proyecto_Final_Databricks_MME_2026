# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Preparación del ambiente
# MAGIC
# MAGIC Este notebook prepara la estructura inicial del proyecto ETL en Azure Databricks.
# MAGIC
# MAGIC ## Objetivos
# MAGIC
# MAGIC - Definir parámetros generales del proyecto.
# MAGIC - Crear el catálogo de Unity Catalog.
# MAGIC - Crear los schemas Bronze, Silver y Gold.
# MAGIC - Validar la estructura creada.
# MAGIC
# MAGIC

# COMMAND ----------


dbutils.widgets.removeAll()

# COMMAND ----------


dbutils.widgets.text(
    "storageName",
    "adlstransaction2026",
    "Storage Account"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION ext_datalake
# MAGIC URL 'abfss://datalake@adlstransaction2026.dfs.core.windows.net/'
# MAGIC WITH (
# MAGIC     STORAGE CREDENTIAL credential_datalake
# MAGIC )
# MAGIC COMMENT 'External Location del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS databricks_etl
# MAGIC MANAGED LOCATION 'abfss://datalake@adlstransaction2026.dfs.core.windows.net/databricks_etl'
# MAGIC COMMENT 'Catálogo del proyecto final ETL con arquitectura Medallion';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Capa Bronze: datos originales provenientes de las fuentes';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Capa Silver: datos limpios, transformados y validados';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Capa Gold: datos consolidados para análisis y consumo';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN databricks_etl;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN databricks_etl.bronze;