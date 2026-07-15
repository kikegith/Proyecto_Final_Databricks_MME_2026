# Databricks notebook source
# MAGIC %md
# MAGIC # Capa Gold — Modelo analítico
# MAGIC
# MAGIC Este notebook construye tablas analíticas a partir de la capa Silver.
# MAGIC
# MAGIC Se generan los siguientes objetos:
# MAGIC
# MAGIC - Resumen de transacciones por cliente.
# MAGIC - Resumen de transacciones por ciudad.
# MAGIC - Resumen por tipo de transacción.
# MAGIC - Resumen por estado del cliente.
# MAGIC - Indicadores ejecutivos.
# MAGIC - Tabla detalle para visualización.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# COMMAND ----------


CATALOGO = "databricks_etl"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

spark.sql(f"USE CATALOG {CATALOGO}")

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_GOLD}"
)

# COMMAND ----------

df_clientes = spark.table(
    f"{CATALOGO}.{SCHEMA_SILVER}.clientes"
)

df_transacciones = spark.table(
    f"{CATALOGO}.{SCHEMA_SILVER}.transacciones"
)

print("Clientes Silver:", df_clientes.count())
print("Transacciones Silver:", df_transacciones.count())

display(df_clientes.limit(10))
display(df_transacciones.limit(10))

# COMMAND ----------

df_detalle_transacciones = (
    df_transacciones.alias("t")
    .join(
        df_clientes.alias("c"),
        F.col("t.ClienteId") == F.col("c.ClienteId"),
        "inner"
    )
    .select(
        F.col("t.ClienteId"),
        F.col("c.Nombre_Completo"),
        F.col("c.Ciudad"),
        F.col("c.Estado"),
        F.col("t.TipoTransaccion"),
        F.col("t.Monto"),
        F.col("t.FechaProceso").alias("FechaProcesoSilver")
    )
    .withColumn(
        "FechaProcesoGold",
        F.current_timestamp()
    )
)

display(df_detalle_transacciones.limit(20))

# COMMAND ----------

print(
    "Registros detalle Gold:",
    df_detalle_transacciones.count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Resumen por cliente**

# COMMAND ----------

df_resumen_cliente = (
    df_detalle_transacciones
    .groupBy(
        "ClienteId",
        "Nombre_Completo",
        "Ciudad",
        "Estado"
    )
    .agg(
        F.count("*").alias("CantidadTransacciones"),

        F.round(
            F.sum("Monto"),
            2
        ).alias("MontoTotal"),

        F.round(
            F.avg("Monto"),
            2
        ).alias("MontoPromedio"),

        F.round(
            F.max("Monto"),
            2
        ).alias("MontoMaximo"),

        F.round(
            F.min("Monto"),
            2
        ).alias("MontoMinimo"),

        F.countDistinct(
            "TipoTransaccion"
        ).alias("CantidadTiposTransaccion")
    )
    .withColumn(
        "FechaProceso",
        F.current_timestamp()
    )
)

display(
    df_resumen_cliente
    .orderBy(F.desc("MontoTotal"))
    .limit(20)
)

# COMMAND ----------

df_resumen_ciudad = (
    df_detalle_transacciones
    .groupBy("Ciudad")
    .agg(
        F.countDistinct(
            "ClienteId"
        ).alias("CantidadClientes"),

        F.count("*").alias(
            "CantidadTransacciones"
        ),

        F.round(
            F.sum("Monto"),
            2
        ).alias("MontoTotal"),

        F.round(
            F.avg("Monto"),
            2
        ).alias("MontoPromedio"),

        F.round(
            F.max("Monto"),
            2
        ).alias("MontoMaximo"),

        F.round(
            F.min("Monto"),
            2
        ).alias("MontoMinimo")
    )
    .withColumn(
        "FechaProceso",
        F.current_timestamp()
    )
)

display(
    df_resumen_ciudad
    .orderBy(F.desc("MontoTotal"))
)

# COMMAND ----------

df_resumen_tipo_transaccion = (
    df_detalle_transacciones
    .groupBy("TipoTransaccion")
    .agg(
        F.count("*").alias(
            "CantidadTransacciones"
        ),

        F.countDistinct(
            "ClienteId"
        ).alias("CantidadClientes"),

        F.round(
            F.sum("Monto"),
            2
        ).alias("MontoTotal"),

        F.round(
            F.avg("Monto"),
            2
        ).alias("MontoPromedio"),

        F.round(
            F.max("Monto"),
            2
        ).alias("MontoMaximo"),

        F.round(
            F.min("Monto"),
            2
        ).alias("MontoMinimo")
    )
    .withColumn(
        "FechaProceso",
        F.current_timestamp()
    )
)

display(
    df_resumen_tipo_transaccion
    .orderBy(F.desc("MontoTotal"))
)

# COMMAND ----------

df_resumen_estado = (
    df_detalle_transacciones
    .groupBy("Estado")
    .agg(
        F.countDistinct(
            "ClienteId"
        ).alias("CantidadClientes"),

        F.count("*").alias(
            "CantidadTransacciones"
        ),

        F.round(
            F.sum("Monto"),
            2
        ).alias("MontoTotal"),

        F.round(
            F.avg("Monto"),
            2
        ).alias("MontoPromedio")
    )
    .withColumn(
        "FechaProceso",
        F.current_timestamp()
    )
)

display(df_resumen_estado)

# COMMAND ----------

df_ranking_clientes = (
    df_resumen_cliente
    .orderBy(
        F.desc("MontoTotal"),
        F.asc("ClienteId")
    )
)

display(
    df_ranking_clientes.limit(20)
)

# COMMAND ----------

total_clientes = df_clientes.count()

clientes_con_transacciones = (
    df_detalle_transacciones
    .select("ClienteId")
    .distinct()
    .count()
)

total_transacciones = (
    df_detalle_transacciones.count()
)

monto_total = (
    df_detalle_transacciones
    .agg(F.sum("Monto").alias("MontoTotal"))
    .first()["MontoTotal"]
)

monto_promedio = (
    df_detalle_transacciones
    .agg(F.avg("Monto").alias("MontoPromedio"))
    .first()["MontoPromedio"]
)

monto_maximo = (
    df_detalle_transacciones
    .agg(F.max("Monto").alias("MontoMaximo"))
    .first()["MontoMaximo"]
)

monto_minimo = (
    df_detalle_transacciones
    .agg(F.min("Monto").alias("MontoMinimo"))
    .first()["MontoMinimo"]
)

cantidad_ciudades = (
    df_clientes
    .select("Ciudad")
    .distinct()
    .count()
)

cantidad_tipos_transaccion = (
    df_transacciones
    .select("TipoTransaccion")
    .distinct()
    .count()
)

clientes_activos = (
    df_clientes
    .filter(F.col("Estado") == "Activo")
    .count()
)

clientes_inactivos = (
    df_clientes
    .filter(F.col("Estado") == "Inactivo")
    .count()
)

# COMMAND ----------

datos_kpi = [
    (
        total_clientes,
        clientes_con_transacciones,
        total_transacciones,
        float(monto_total or 0),
        float(monto_promedio or 0),
        float(monto_maximo or 0),
        float(monto_minimo or 0),
        cantidad_ciudades,
        cantidad_tipos_transaccion,
        clientes_activos,
        clientes_inactivos
    )
]

columnas_kpi = [
    "TotalClientes",
    "ClientesConTransacciones",
    "TotalTransacciones",
    "MontoTotal",
    "MontoPromedio",
    "MontoMaximo",
    "MontoMinimo",
    "CantidadCiudades",
    "CantidadTiposTransaccion",
    "ClientesActivos",
    "ClientesInactivos"
]

df_kpi_ejecutivo = (
    spark.createDataFrame(
        datos_kpi,
        columnas_kpi
    )
    .withColumn(
        "MontoTotal",
        F.round(F.col("MontoTotal"), 2)
    )
    .withColumn(
        "MontoPromedio",
        F.round(F.col("MontoPromedio"), 2)
    )
    .withColumn(
        "MontoMaximo",
        F.round(F.col("MontoMaximo"), 2)
    )
    .withColumn(
        "MontoMinimo",
        F.round(F.col("MontoMinimo"), 2)
    )
    .withColumn(
        "PorcentajeClientesConTransacciones",
        F.round(
            F.col("ClientesConTransacciones")
            / F.col("TotalClientes") * 100,
            2
        )
    )
    .withColumn(
        "FechaProceso",
        F.current_timestamp()
    )
)

display(df_kpi_ejecutivo)

# COMMAND ----------

def guardar_tabla_gold(df, nombre_tabla):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{CATALOGO}.{SCHEMA_GOLD}.{nombre_tabla}"
        )
    )

    print(
        f"Tabla creada: "
        f"{CATALOGO}.{SCHEMA_GOLD}.{nombre_tabla}"
    )

# COMMAND ----------

guardar_tabla_gold(
    df_detalle_transacciones,
    "detalle_transacciones"
)

guardar_tabla_gold(
    df_resumen_cliente,
    "resumen_cliente"
)

guardar_tabla_gold(
    df_resumen_ciudad,
    "resumen_ciudad"
)

guardar_tabla_gold(
    df_resumen_tipo_transaccion,
    "resumen_tipo_transaccion"
)

guardar_tabla_gold(
    df_resumen_estado,
    "resumen_estado"
)

guardar_tabla_gold(
    df_ranking_clientes,
    "ranking_clientes"
)

guardar_tabla_gold(
    df_kpi_ejecutivo,
    "kpi_ejecutivo"
)

# COMMAND ----------

total_silver = df_transacciones.count()
total_detalle_gold = df_detalle_transacciones.count()

print("Transacciones Silver:", total_silver)
print("Detalle Gold:", total_detalle_gold)

if total_silver == total_detalle_gold:
    print("VALIDACIÓN CORRECTA: no se perdieron transacciones.")
else:
    print("ADVERTENCIA: existen diferencias entre Silver y Gold.")

# COMMAND ----------

monto_silver = (
    df_transacciones
    .agg(
        F.round(
            F.sum("Monto"),
            2
        ).alias("MontoTotal")
    )
    .first()["MontoTotal"]
)

monto_gold = (
    df_detalle_transacciones
    .agg(
        F.round(
            F.sum("Monto"),
            2
        ).alias("MontoTotal")
    )
    .first()["MontoTotal"]
)

print("Monto total Silver:", monto_silver)
print("Monto total Gold:", monto_gold)

if monto_silver == monto_gold:
    print("VALIDACIÓN CORRECTA: los montos coinciden.")
else:
    print("ADVERTENCIA: los montos no coinciden.")

# COMMAND ----------

monto_resumen_cliente = (
    df_resumen_cliente
    .agg(
        F.round(
            F.sum("MontoTotal"),
            2
        ).alias("MontoTotal")
    )
    .first()["MontoTotal"]
)

monto_resumen_ciudad = (
    df_resumen_ciudad
    .agg(
        F.round(
            F.sum("MontoTotal"),
            2
        ).alias("MontoTotal")
    )
    .first()["MontoTotal"]
)

monto_resumen_tipo = (
    df_resumen_tipo_transaccion
    .agg(
        F.round(
            F.sum("MontoTotal"),
            2
        ).alias("MontoTotal")
    )
    .first()["MontoTotal"]
)

print("Monto detalle:", monto_gold)
print("Monto por cliente:", monto_resumen_cliente)
print("Monto por ciudad:", monto_resumen_ciudad)
print("Monto por tipo:", monto_resumen_tipo)

# COMMAND ----------

spark.sql(
    f"SHOW TABLES IN {CATALOGO}.{SCHEMA_GOLD}"
).show(truncate=False)

# COMMAND ----------

display(
    spark.table(
        f"{CATALOGO}.{SCHEMA_GOLD}.kpi_ejecutivo"
    )
)

display(
    spark.table(
        f"{CATALOGO}.{SCHEMA_GOLD}.resumen_ciudad"
    )
)

display(
    spark.table(
        f"{CATALOGO}.{SCHEMA_GOLD}.resumen_tipo_transaccion"
    )
)

display(
    spark.table(
        f"{CATALOGO}.{SCHEMA_GOLD}.ranking_clientes"
    )
)