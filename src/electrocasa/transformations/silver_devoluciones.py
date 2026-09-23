# ============================================================
# SILVER - electrocasa.bronze.devoluciones_bronze
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. LECTURA BRONZE


df_devoluciones_bronze = spark.table(
    "electrocasa.bronze.devoluciones_bronze"
)

df_catalogo_productos = (
    spark.table(
        "electrocasa.silver.catalogo_productos_silver"
    )
    .select(
        F.trim(
            F.col("producto_id").cast("string")
        ).alias("producto_id")
    )
    .filter(
        F.col("producto_id").isNotNull()
    )
    .filter(
        F.col("producto_id") != ""
    )
    .dropDuplicates(["producto_id"])
)


# ============================================================
# 2. NORMALIZACION INICIAL
# ============================================================

# Normalización del monto:

df_base = (
    df_devoluciones_bronze

    # Claves normalizadas como string
    .withColumn(
        "devolucion_id",
        F.trim(
            F.col("devolucion_id").cast("string")
        )
    )
    .withColumn(
        "pedido_id",
        F.trim(
            F.col("pedido_id").cast("string")
        )
    )
    .withColumn(
        "producto_id",
        F.trim(
            F.col("producto_id").cast("string")
        )
    )
    .withColumn(
        "sucursal_id",
        F.trim(
            F.col("sucursal_id").cast("string")
        )
    )

    # Motivo normalizado
    .withColumn(
        "motivo",
        F.trim(
            F.col("motivo").cast("string")
        )
    )
    .withColumn(
        "motivo",
        F.when(
            F.col("motivo").isNull() |
            (F.col("motivo") == ""),
            F.lit("motivo no especificado")
        ).otherwise(
            F.col("motivo")
        )
    )

    # Conservamos el valor original para auditoría
    .withColumn(
        "monto_reembolso_original",
        F.col("monto_reembolso").cast("string")
    )

    # Limpieza inicial del monto
    .withColumn(
        "_monto_limpio",
        F.regexp_replace(
            F.trim(
                F.col("monto_reembolso").cast("string")
            ),
            r"\s+",
            ""
        )
    )
    .withColumn(
        "_monto_limpio",
        F.regexp_replace(
            F.col("_monto_limpio"),
            r"[^0-9,\.\-]",
            ""
        )
    )

    .withColumn(
        "_monto_normalizado",
        F.when(
            F.col("_monto_limpio").contains(",") &
            F.col("_monto_limpio").contains("."),
            F.regexp_replace(
                F.regexp_replace(
                    F.col("_monto_limpio"),
                    r"\.",
                    ""
                ),
                ",",
                "."
            )
        )
        .when(
            F.col("_monto_limpio").contains(","),
            F.regexp_replace(
                F.col("_monto_limpio"),
                ",",
                "."
            )
        )
        .otherwise(
            F.col("_monto_limpio")
        )
    )
    .withColumn(
        "monto_reembolso",
        F.col("_monto_normalizado")
        .cast("decimal(12,2)")
    )
    .drop(
        "_monto_limpio",
        "_monto_normalizado"
    )
)


# ============================================================
# REGLA 1
# DEVOLUCION_ID REINGESTADO
# ============================================================

ventana_devolucion = (
    Window
    .partitionBy("devolucion_id")
    .orderBy(
        F.col("batch_id").asc_nulls_last(),
        F.col("ingestion_timestamp").asc_nulls_last()
    )
)

ventana_conteo = (
    Window
    .partitionBy("devolucion_id")
)

df_con_numero_reingesta = (
    df_base
    .withColumn(
        "_numero_reingesta",
        F.row_number().over(
            ventana_devolucion
        )
    )
    .withColumn(
        "_cantidad_reingestas",
        F.count(
            F.lit(1)
        ).over(
            ventana_conteo
        )
    )
)

# Duplicados a cuarentena

cuarentena_devolucion_reingestada = (
    df_con_numero_reingesta
    .filter(
        F.col("devolucion_id").isNotNull()
        &
        (F.col("devolucion_id") != "")
        &
        (F.col("_numero_reingesta") > 1)
    )
    .withColumn(
        "motivo_error",
        F.lit(
            "DEVOLUCION_ID_REINGESTADO"
        )
    )
)

# Solo la primera ocurrencia continúa

df_validacion = (
    df_con_numero_reingesta
    .filter(
        (F.col("_numero_reingesta") == 1)
        |
        F.col("devolucion_id").isNull()
        |
        (F.col("devolucion_id") == "")
    )
    .drop(
        "_numero_reingesta",
        "_cantidad_reingestas"
    )
)

# ============================================================
# REGLA 2
# MONTO_REEMBOLSO NEGATIVO
# ============================================================

cuarentena_monto_negativo = (
    df_validacion
    .filter(
        F.col("monto_reembolso") < 0
    )
    .withColumn(
        "motivo_error",
        F.lit("MONTO_REEMBOLSO_NEGATIVO")
    )
)

df_validacion = (
    df_validacion
    .filter(
        F.col("monto_reembolso").isNull() |
        (F.col("monto_reembolso") >= 0)
    )
)


# ============================================================
# REGLA 3
# PEDIDO_ID NULO O VACIO
# ============================================================

cuarentena_pedido_id_faltante = (
    df_validacion
    .filter(
        F.col("pedido_id").isNull() |
        (F.col("pedido_id") == "")
    )
    .withColumn(
        "motivo_error",
        F.lit("PEDIDO_ID_FALTANTE")
    )
)

df_validacion = (
    df_validacion
    .filter(
        F.col("pedido_id").isNotNull()
    )
    .filter(
        F.col("pedido_id") != ""
    )
)


# ============================================================
# REGLA 4
# PRODUCTO_ID HUERFANO
# ============================================================

cuarentena_producto_huerfano = (
    df_validacion
    .join(
        df_catalogo_productos,
        on="producto_id",
        how="left_anti"
    )
    .withColumn(
        "motivo_error",
        F.lit("PRODUCTO_ID_HUERFANO")
    )
)

df_validacion = (
    df_validacion
    .join(
        df_catalogo_productos,
        on="producto_id",
        how="left_semi"
    )
)


# ============================================================
# 3. NORMALIZACION DE FECHA
# ============================================================

# Se intenta convertir la fecha considerando:
# - yyyy-MM-dd
# - dd/MM/yyyy
# - conversión estándar de Spark

df_validacion = (
    df_validacion
    .withColumn(
        "fecha_devolucion",
        F.coalesce(
            F.to_date(
                F.col("fecha_devolucion"),
                "yyyy-MM-dd"
            ),
            F.to_date(
                F.col("fecha_devolucion"),
                "dd/MM/yyyy"
            ),
            F.to_date(
                F.col("fecha_devolucion")
            )
        )
    )
)


# ============================================================
# 4. LIMPIEZA DE COLUMNAS TECNICAS TEMPORALES
# ============================================================

cuarentena_devolucion_reingestada = (
    cuarentena_devolucion_reingestada
    .drop(
        "_numero_reingesta",
        "_cantidad_reingestas"
    )
)


# ============================================================
# 5. TABLA DE CUARENTENA
# ============================================================

df_cuarentena = (
    cuarentena_devolucion_reingestada

    .unionByName(
        cuarentena_monto_negativo,
        allowMissingColumns=True
    )
    .unionByName(
        cuarentena_pedido_id_faltante,
        allowMissingColumns=True
    )
    .unionByName(
        cuarentena_producto_huerfano,
        allowMissingColumns=True
    )

    # Trazabilidad
    .withColumn(
        "fuente_origen",
        F.lit("devoluciones_bronze")
    )
    .withColumn(
        "capa_origen",
        F.lit("bronze")
    )
    .withColumn(
        "fecha_procesamiento",
        F.current_timestamp()
    )
)


# ============================================================
# 6. TABLA VALIDA FINAL PARA SILVER
# ============================================================

df_devoluciones_validas = (
    df_validacion

    # La columna original se conserva en cuarentena, pero no es
    # necesaria en la tabla Silver.
    .drop(
        "monto_reembolso_original"
    )
)


# ============================================================
# 7. GUARDAR CUARENTENA
# ============================================================

(
    df_cuarentena
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "electrocasa.auditoria.devoluciones_cuarentena"
    )
)


# ============================================================
# 8. GUARDAR SILVER
# ============================================================

(
    df_devoluciones_validas
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "electrocasa.silver.devoluciones_silver"
    )
)


# ============================================================
# 9. VALIDACIONES DE CONTROL
# ============================================================

# print("Proceso de devoluciones completado.")

# print(
#     "Registros en Bronze:",
#     df_devoluciones_bronze.count()
# )

# print(
#     "Registros válidos en Silver:",
#     df_devoluciones_validas.count()
# )

# print(
#     "Registros enviados a cuarentena:",
#     df_cuarentena.count()
# )

# print("Detalle de cuarentena por motivo:")

# (
#     df_cuarentena
#     .groupBy("motivo_error")
#     .count()
#     .orderBy(
#         F.col("count").desc()
#     )
#     .show(
#         truncate=False
#     )
# )