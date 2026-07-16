# Databricks notebook source
# MAGIC %md
# MAGIC # Asignación de permisos
# MAGIC
# MAGIC Este notebook contiene la asignación de permisos sobre los objetos del proyecto mediante Unity Catalog.
# MAGIC
# MAGIC Se consideran los siguientes grupos:
# MAGIC
# MAGIC - **DEs:** Ingenieros de Datos.
# MAGIC - **CTs:** Consultores técnicos.
# MAGIC - **BI:** Analistas de Business Intelligence.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rol de Data Engineers (DEs)
# MAGIC
# MAGIC Los ingenieros de datos requieren permisos para utilizar el catálogo, administrar los schemas y consultar o modificar las tablas del proyecto.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- PERMISOS PARA DATA ENGINEERS
# MAGIC -- =====================================================
# MAGIC
# MAGIC -- CATÁLOGO
# MAGIC GRANT USE CATALOG
# MAGIC ON CATALOG databricks_etl
# MAGIC TO `DEs`;
# MAGIC
# MAGIC -- SCHEMA BRONZE
# MAGIC GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
# MAGIC ON SCHEMA databricks_etl.bronze
# MAGIC TO `DEs`;
# MAGIC
# MAGIC -- SCHEMA SILVER
# MAGIC GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
# MAGIC ON SCHEMA databricks_etl.silver
# MAGIC TO `DEs`;
# MAGIC
# MAGIC -- SCHEMA GOLD
# MAGIC GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
# MAGIC ON SCHEMA databricks_etl.gold
# MAGIC TO `DEs`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- TABLAS BRONZE
# MAGIC GRANT SELECT, MODIFY
# MAGIC ON TABLE databricks_etl.bronze.clientes
# MAGIC TO `DEs`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY
# MAGIC ON TABLE databricks_etl.bronze.transacciones
# MAGIC TO `DEs`;
# MAGIC
# MAGIC -- TABLAS SILVER
# MAGIC GRANT SELECT, MODIFY
# MAGIC ON TABLE databricks_etl.silver.clientes
# MAGIC TO `DEs`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY
# MAGIC ON TABLE databricks_etl.silver.transacciones
# MAGIC TO `DEs`;
# MAGIC
# MAGIC -- TABLAS GOLD
# MAGIC GRANT SELECT, MODIFY
# MAGIC ON TABLE databricks_etl.gold.resumen_clientes
# MAGIC TO `DEs`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY
# MAGIC ON TABLE databricks_etl.gold.resumen_tipo_transaccion
# MAGIC TO `DEs`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT READ FILES, WRITE FILES
# MAGIC ON EXTERNAL LOCATION `extloc_datalake`
# MAGIC TO `DEs`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rol de Consultor Técnico (CTs)
# MAGIC
# MAGIC Los consultores técnicos pueden visualizar la información de las capas Bronze, Silver y Gold, pero no pueden modificar las tablas.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- PERMISOS PARA CONSULTORES TÉCNICOS
# MAGIC -- =====================================================
# MAGIC
# MAGIC -- CATÁLOGO
# MAGIC GRANT USE CATALOG
# MAGIC ON CATALOG databricks_etl
# MAGIC TO `CTs`;
# MAGIC
# MAGIC -- SCHEMAS
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA databricks_etl.bronze
# MAGIC TO `CTs`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA databricks_etl.silver
# MAGIC TO `CTs`;
# MAGIC
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA databricks_etl.gold
# MAGIC TO `CTs`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- TABLAS BRONZE
# MAGIC GRANT SELECT
# MAGIC ON TABLE databricks_etl.bronze.clientes
# MAGIC TO `CTs`;
# MAGIC
# MAGIC GRANT SELECT
# MAGIC ON TABLE databricks_etl.bronze.transacciones
# MAGIC TO `CTs`;
# MAGIC
# MAGIC -- TABLAS SILVER
# MAGIC GRANT SELECT
# MAGIC ON TABLE databricks_etl.silver.clientes
# MAGIC TO `CTs`;
# MAGIC
# MAGIC GRANT SELECT
# MAGIC ON TABLE databricks_etl.silver.transacciones
# MAGIC TO `CTs`;
# MAGIC
# MAGIC -- TABLAS GOLD
# MAGIC GRANT SELECT
# MAGIC ON TABLE databricks_etl.gold.resumen_clientes
# MAGIC TO `CTs`;
# MAGIC
# MAGIC GRANT SELECT
# MAGIC ON TABLE databricks_etl.gold.resumen_tipo_transaccion
# MAGIC TO `CTs`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT READ FILES
# MAGIC ON EXTERNAL LOCATION `extloc_datalake`
# MAGIC TO `CTs`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rol de Analista BI (BI)
# MAGIC
# MAGIC Los analistas BI acceden únicamente a la capa Gold, donde se encuentran los datos consolidados y preparados para análisis y reportes.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- PERMISOS PARA ANALISTAS BI
# MAGIC -- =====================================================
# MAGIC
# MAGIC -- CATÁLOGO
# MAGIC GRANT USE CATALOG
# MAGIC ON CATALOG databricks_etl
# MAGIC TO `BI`;
# MAGIC
# MAGIC -- SCHEMA GOLD
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA databricks_etl.gold
# MAGIC TO `BI`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT SELECT
# MAGIC ON TABLE databricks_etl.gold.resumen_clientes
# MAGIC TO `BI`;
# MAGIC
# MAGIC GRANT SELECT
# MAGIC ON TABLE databricks_etl.gold.resumen_tipo_transaccion
# MAGIC TO `BI`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT SELECT, MODIFY
# MAGIC ON FUTURE TABLES IN SCHEMA databricks_etl.bronze
# MAGIC TO `DEs`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY
# MAGIC ON FUTURE TABLES IN SCHEMA databricks_etl.silver
# MAGIC TO `DEs`;
# MAGIC
# MAGIC GRANT SELECT, MODIFY
# MAGIC ON FUTURE TABLES IN SCHEMA databricks_etl.gold
# MAGIC TO `DEs`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT SELECT
# MAGIC ON FUTURE TABLES IN SCHEMA databricks_etl.bronze
# MAGIC TO `CTs`;
# MAGIC
# MAGIC GRANT SELECT
# MAGIC ON FUTURE TABLES IN SCHEMA databricks_etl.silver
# MAGIC TO `CTs`;
# MAGIC
# MAGIC GRANT SELECT
# MAGIC ON FUTURE TABLES IN SCHEMA databricks_etl.gold
# MAGIC TO `CTs`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT SELECT
# MAGIC ON FUTURE TABLES IN SCHEMA databricks_etl.gold
# MAGIC TO `BI`;

# COMMAND ----------

# MAGIC %md
# MAGIC