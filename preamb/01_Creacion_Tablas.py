# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Creación de objetos
# MAGIC
# MAGIC Este notebook contiene la definición de las tablas Delta utilizadas en el proyecto ETL.
# MAGIC
# MAGIC La estructura se organiza mediante la arquitectura Medallion:
# MAGIC
# MAGIC - **Bronze:** almacenamiento de los datos provenientes de los archivos fuente.
# MAGIC - **Silver:** datos limpios, estandarizados y validados.
# MAGIC - **Gold:** información consolidada para análisis y reportes.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_etl;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas de la capa Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_etl.bronze.clientes
# MAGIC (
# MAGIC     ClienteId       STRING,
# MAGIC     Nombre_Completo STRING,
# MAGIC     Ciudad          STRING,
# MAGIC     Estado          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Datos originales de clientes cargados desde el archivo clientes.csv';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_etl.bronze.transacciones
# MAGIC (
# MAGIC     ClienteId       STRING,
# MAGIC     TipoTransaccion STRING,
# MAGIC     Monto           INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Datos originales de transacciones cargados desde el archivo transacciones.csv';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas de la capa Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_etl.silver.clientes
# MAGIC (
# MAGIC     cliente_id      STRING    NOT NULL,
# MAGIC     nombre_completo STRING,
# MAGIC     ciudad          STRING,
# MAGIC     estado          STRING,
# MAGIC     fecha_proceso   TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Clientes limpios, normalizados y validados provenientes de la capa Bronze';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_etl.silver.transacciones
# MAGIC (
# MAGIC     cliente_id       STRING    NOT NULL,
# MAGIC     tipo_transaccion STRING,
# MAGIC     monto            DECIMAL(18,2),
# MAGIC     fecha_proceso    TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Transacciones limpias y validadas provenientes de la capa Bronze';

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Tablas de la capa Gold**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_etl.gold.resumen_clientes
# MAGIC (
# MAGIC     cliente_id          STRING,
# MAGIC     nombre_completo     STRING,
# MAGIC     ciudad              STRING,
# MAGIC     estado              STRING,
# MAGIC     total_transacciones BIGINT,
# MAGIC     monto_total         DECIMAL(18,2),
# MAGIC     monto_promedio      DECIMAL(18,2),
# MAGIC     monto_minimo        DECIMAL(18,2),
# MAGIC     monto_maximo        DECIMAL(18,2),
# MAGIC     fecha_actualizacion TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Resumen de transacciones y montos consolidados por cliente';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_etl.gold.resumen_tipo_transaccion
# MAGIC (
# MAGIC     tipo_transaccion    STRING,
# MAGIC     total_transacciones BIGINT,
# MAGIC     cantidad_clientes   BIGINT,
# MAGIC     monto_total         DECIMAL(18,2),
# MAGIC     monto_promedio      DECIMAL(18,2),
# MAGIC     monto_minimo        DECIMAL(18,2),
# MAGIC     monto_maximo        DECIMAL(18,2),
# MAGIC     fecha_actualizacion TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Resumen de operaciones agrupadas por tipo de transacción';