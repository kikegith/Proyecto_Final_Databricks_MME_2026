# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión del proyecto
# MAGIC
# MAGIC Este notebook contiene las instrucciones necesarias para eliminar los objetos creados durante la ejecución del proyecto ETL.
# MAGIC
# MAGIC La reversión contempla:
# MAGIC
# MAGIC - Tablas de la capa Gold.
# MAGIC - Tablas de la capa Silver.
# MAGIC - Tablas de la capa Bronze.
# MAGIC - Schemas.
# MAGIC - Catálogo.
# MAGIC - External Location.
# MAGIC - Storage Credential.
# MAGIC
# MAGIC
# MAGIC ADVERTENCIA: No debe ejecutarse sobre el ambiente actual del proyecto.

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
# MAGIC
# MAGIC DROP TABLE IF EXISTS databricks_etl.gold.resumen_clientes;
# MAGIC
# MAGIC DROP TABLE IF EXISTS databricks_etl.gold.resumen_tipo_transaccion;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS databricks_etl.silver.clientes;
# MAGIC
# MAGIC DROP TABLE IF EXISTS databricks_etl.silver.transacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS databricks_etl.bronze.clientes;
# MAGIC
# MAGIC DROP TABLE IF EXISTS databricks_etl.bronze.transacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS databricks_etl.gold CASCADE;
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS databricks_etl.silver CASCADE;
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS databricks_etl.bronze CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP CATALOG IF EXISTS databricks_etl CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP EXTERNAL LOCATION IF EXISTS extloc_datalake FORCE;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP STORAGE CREDENTIAL IF EXISTS credential_datalake;