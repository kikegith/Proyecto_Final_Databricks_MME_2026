# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

spark.sql("USE CATALOG databricks_etl")

# COMMAND ----------

spark.sql("USE SCHEMA bronze")

# COMMAND ----------

df_clientes = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .option("sep", ";")
         .option("encoding", "UTF-8")
         .csv("abfss://datalake@adlstransaction2026.dfs.core.windows.net/raw/clientes.csv")
)

# COMMAND ----------

df_transacciones = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .option("sep", ";")
         .option("encoding", "UTF-8")
         .csv("abfss://datalake@adlstransaction2026.dfs.core.windows.net/raw/transacciones.csv")
)

# COMMAND ----------

df_clientes.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("databricks_etl.bronze.clientes")

# COMMAND ----------

df_transacciones.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("databricks_etl.bronze.transacciones")

# COMMAND ----------

spark.sql("SHOW TABLES IN databricks_etl.bronze").show()

# COMMAND ----------

display(df_clientes)

# COMMAND ----------

display(df_transacciones)