# Databricks notebook source
# MAGIC %md
# MAGIC # Capa Silver — Limpieza y calidad de datos
# MAGIC
# MAGIC Este notebook transforma las tablas Delta de la capa Bronze.
# MAGIC
# MAGIC Procesos aplicados:
# MAGIC
# MAGIC - Limpieza de espacios.
# MAGIC - Estandarización de textos.
# MAGIC - Conversión de tipos de datos.
# MAGIC - Validación de campos obligatorios.
# MAGIC - Validación del formato de ClienteId.
# MAGIC - Validación de estados.
# MAGIC - Validación de montos.
# MAGIC - Tratamiento de duplicados.
# MAGIC - Validación de integridad referencial.
# MAGIC - Creación de registros rechazados.
# MAGIC - Creación de columnas de auditoría.
# MAGIC - Escritura de tablas Delta en Silver.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

# COMMAND ----------

CATALOGO = "databricks_etl"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_SILVER}")

# COMMAND ----------

df_clientes_bronze = spark.table(
    f"{CATALOGO}.{SCHEMA_BRONZE}.clientes"
)

df_transacciones_bronze = spark.table(
    f"{CATALOGO}.{SCHEMA_BRONZE}.transacciones"
)

print("Registros Bronze - Clientes:", df_clientes_bronze.count())
print("Registros Bronze - Transacciones:", df_transacciones_bronze.count())

display(df_clientes_bronze.limit(10))
display(df_transacciones_bronze.limit(10))

# COMMAND ----------

df_clientes_limpios = (
    df_clientes_bronze
    .select(
        F.upper(
            F.trim(F.col("ClienteId").cast("string"))
        ).alias("ClienteId"),

        F.initcap(
            F.regexp_replace(
                F.trim(F.col("Nombre_Completo").cast("string")),
                r"\s+",
                " "
            )
        ).alias("Nombre_Completo"),

        F.initcap(
            F.regexp_replace(
                F.trim(F.col("Ciudad").cast("string")),
                r"\s+",
                " "
            )
        ).alias("Ciudad"),

        F.when(
            F.lower(F.trim(F.col("Estado"))) == "activo",
            F.lit("Activo")
        )
        .when(
            F.lower(F.trim(F.col("Estado"))) == "inactivo",
            F.lit("Inactivo")
        )
        .otherwise(F.trim(F.col("Estado")))
        .alias("Estado")
    )
    .withColumn("FechaProceso", F.current_timestamp())
    .withColumn("OrigenDatos", F.lit("bronze.clientes"))
)

display(df_clientes_limpios.limit(20))

# COMMAND ----------

PATRON_CLIENTE_ID = r"^CLI[0-9]{5}$"
df_clientes_validados = (
    df_clientes_limpios
    .withColumn(
        "MotivoRechazo",
        F.concat_ws(
            " | ",

            F.when(
                F.col("ClienteId").isNull() |
                (F.length(F.trim(F.col("ClienteId"))) == 0),
                F.lit("ClienteId vacío o nulo")
            ),

            F.when(
                F.col("ClienteId").isNotNull() &
                (~F.col("ClienteId").rlike(PATRON_CLIENTE_ID)),
                F.lit("Formato de ClienteId inválido")
            ),

            F.when(
                F.col("Nombre_Completo").isNull() |
                (F.length(F.trim(F.col("Nombre_Completo"))) == 0),
                F.lit("Nombre completo vacío o nulo")
            ),

            F.when(
                F.col("Ciudad").isNull() |
                (F.length(F.trim(F.col("Ciudad"))) == 0),
                F.lit("Ciudad vacía o nula")
            ),

            F.when(
                F.col("Estado").isNull() |
                (~F.col("Estado").isin("Activo", "Inactivo")),
                F.lit("Estado inválido")
            )
        )
    )
)

# COMMAND ----------

ventana_cliente = Window.partitionBy("ClienteId")

df_clientes_validados = (
    df_clientes_validados
    .withColumn(
        "CantidadClienteId",
        F.count("*").over(ventana_cliente)
    )
    .withColumn(
        "MotivoRechazo",
        F.when(
            F.col("CantidadClienteId") > 1,
            F.concat_ws(
                " | ",
                F.col("MotivoRechazo"),
                F.lit("ClienteId duplicado")
            )
        ).otherwise(F.col("MotivoRechazo"))
    )
)

df_clientes_rechazados = (
    df_clientes_validados
    .filter(F.length(F.col("MotivoRechazo")) > 0)
    .drop("CantidadClienteId")
)

df_clientes_silver = (
    df_clientes_validados
    .filter(F.length(F.col("MotivoRechazo")) == 0)
    .drop("MotivoRechazo", "CantidadClienteId")
)

# COMMAND ----------

df_clientes_silver = (
    df_clientes_silver
    .withColumn(
        "HashRegistro",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("ClienteId"), F.lit("")),
                F.coalesce(F.col("Nombre_Completo"), F.lit("")),
                F.coalesce(F.col("Ciudad"), F.lit("")),
                F.coalesce(F.col("Estado"), F.lit(""))
            ),
            256
        )
    )
)

display(df_clientes_silver.limit(20))
display(df_clientes_rechazados.limit(20))

# COMMAND ----------

df_transacciones_limpias = (
    df_transacciones_bronze
    .select(
        F.upper(
            F.trim(F.col("ClienteId").cast("string"))
        ).alias("ClienteId"),

        F.initcap(
            F.regexp_replace(
                F.trim(F.col("TipoTransaccion").cast("string")),
                r"\s+",
                " "
            )
        ).alias("TipoTransaccion"),

        F.col("Monto")
        .cast(DecimalType(18, 2))
        .alias("Monto")
    )
    .withColumn("FechaProceso", F.current_timestamp())
    .withColumn("OrigenDatos", F.lit("bronze.transacciones"))
)

display(df_transacciones_limpias.limit(20))

# COMMAND ----------

df_transacciones_validadas = (
    df_transacciones_limpias
    .withColumn(
        "MotivoRechazo",
        F.concat_ws(
            " | ",

            F.when(
                F.col("ClienteId").isNull() |
                (F.length(F.trim(F.col("ClienteId"))) == 0),
                F.lit("ClienteId vacío o nulo")
            ),

            F.when(
                F.col("ClienteId").isNotNull() &
                (~F.col("ClienteId").rlike(PATRON_CLIENTE_ID)),
                F.lit("Formato de ClienteId inválido")
            ),

            F.when(
                F.col("TipoTransaccion").isNull() |
                (F.length(F.trim(F.col("TipoTransaccion"))) == 0),
                F.lit("Tipo de transacción vacío o nulo")
            ),

            F.when(
                F.col("Monto").isNull(),
                F.lit("Monto nulo o no numérico")
            ),

            F.when(
                F.col("Monto").isNotNull() &
                (F.col("Monto") <= 0),
                F.lit("El monto debe ser mayor que cero")
            )
        )
    )
)

# COMMAND ----------

df_clientes_validos = (
    df_clientes_silver
    .select("ClienteId")
    .dropDuplicates()
)

df_transacciones_con_cliente = (
    df_transacciones_validadas.alias("t")
    .join(
        df_clientes_validos.alias("c"),
        F.col("t.ClienteId") == F.col("c.ClienteId"),
        "left"
    )
    .select(
        F.col("t.*"),
        F.col("c.ClienteId").alias("ClienteIdEncontrado")
    )
    .withColumn(
        "MotivoRechazo",
        F.when(
            F.col("ClienteIdEncontrado").isNull(),
            F.concat_ws(
                " | ",
                F.col("MotivoRechazo"),
                F.lit("ClienteId no existe en clientes Silver")
            )
        ).otherwise(F.col("MotivoRechazo"))
    )
    .drop("ClienteIdEncontrado")
)

# COMMAND ----------

df_transacciones_rechazadas = (
    df_transacciones_con_cliente
    .filter(F.length(F.col("MotivoRechazo")) > 0)
)

df_transacciones_silver = (
    df_transacciones_con_cliente
    .filter(F.length(F.col("MotivoRechazo")) == 0)
    .drop("MotivoRechazo")
)

# COMMAND ----------

df_transacciones_silver = (
    df_transacciones_silver
    .withColumn(
        "HashRegistro",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("ClienteId"), F.lit("")),
                F.coalesce(F.col("TipoTransaccion"), F.lit("")),
                F.coalesce(
                    F.col("Monto").cast("string"),
                    F.lit("")
                )
            ),
            256
        )
    )
)

display(df_transacciones_silver.limit(20))
display(df_transacciones_rechazadas.limit(20))

# COMMAND ----------

(
    df_clientes_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOGO}.{SCHEMA_SILVER}.clientes")
)

# COMMAND ----------

(
    df_clientes_rechazados.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOGO}.{SCHEMA_SILVER}.clientes_rechazados")
)

# COMMAND ----------

(
    df_transacciones_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOGO}.{SCHEMA_SILVER}.transacciones")
)

# COMMAND ----------

(
    df_transacciones_rechazadas.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOGO}.{SCHEMA_SILVER}.transacciones_rechazadas"
    )
)

# COMMAND ----------

total_clientes_bronze = df_clientes_bronze.count()
total_clientes_validos = df_clientes_silver.count()
total_clientes_rechazados = df_clientes_rechazados.count()

total_transacciones_bronze = df_transacciones_bronze.count()
total_transacciones_validas = df_transacciones_silver.count()
total_transacciones_rechazadas = df_transacciones_rechazadas.count()

print("===== RESULTADOS CLIENTES =====")
print(f"Bronze:     {total_clientes_bronze}")
print(f"Válidos:    {total_clientes_validos}")
print(f"Rechazados: {total_clientes_rechazados}")

print("\n===== RESULTADOS TRANSACCIONES =====")
print(f"Bronze:     {total_transacciones_bronze}")
print(f"Válidas:    {total_transacciones_validas}")
print(f"Rechazadas: {total_transacciones_rechazadas}")

# COMMAND ----------

datos_calidad = [
    (
        "clientes",
        total_clientes_bronze,
        total_clientes_validos,
        total_clientes_rechazados
    ),
    (
        "transacciones",
        total_transacciones_bronze,
        total_transacciones_validas,
        total_transacciones_rechazadas
    )
]

columnas_calidad = [
    "Tabla",
    "TotalBronze",
    "TotalValidos",
    "TotalRechazados"
]

df_calidad = (
    spark.createDataFrame(datos_calidad, columnas_calidad)
    .withColumn(
        "PorcentajeCalidad",
        F.round(
            F.col("TotalValidos") /
            F.col("TotalBronze") * 100,
            2
        )
    )
    .withColumn("FechaProceso", F.current_timestamp())
)

display(df_calidad)

# COMMAND ----------

(
    df_calidad.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOGO}.{SCHEMA_SILVER}.calidad_datos")
)

# COMMAND ----------

spark.sql(
    f"SHOW TABLES IN {CATALOGO}.{SCHEMA_SILVER}"
).show(truncate=False)

# COMMAND ----------

display(
    spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.clientes")
)

display(
    spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.transacciones")
)

display(
    spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.calidad_datos")
)

# COMMAND ----------

display(
    df_clientes_rechazados
    .groupBy("MotivoRechazo")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

display(
    df_transacciones_rechazadas
        .groupBy("MotivoRechazo")
        .count()
        .orderBy(F.desc("count"))
)