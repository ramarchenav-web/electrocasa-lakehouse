# ============================================================
# SILVER - electrocasa.bronze.tracking_envios_bronze
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. LECTURA BRONZE
# ============================================================

df_tracking_envios_bronze = spark.table(
    "electrocasa.bronze.tracking_envios_bronze"
)


# ============================================================
# 2. NORMALIZACION INICIAL
# ============================================================

df_base = (
    df_tracking_envios_bronze

    # --------------------------------------------------------
    # Claves normalizadas como string
    # --------------------------------------------------------

    .withColumn(
        "tracking_id",
        F.trim(
            F.col("tracking_id").cast("string")
        )
    )
    .withColumn(
        "pedido_id",
        F.trim(
            F.col("pedido_id").cast("string")
        )
    )
    .withColumn(
        "sucursal_origen",
        F.trim(
            F.col("sucursal_origen").cast("string")
        )
    )

    # --------------------------------------------------------
    # Conservamos valores originales para auditoria
    # --------------------------------------------------------

    .withColumn(
        "estado_entrega_original",
        F.col("estado_entrega").cast("string")
    )
    .withColumn(
        "courier_original",
        F.col("courier").cast("string")
    )
    .withColumn(
        "fecha_actualizacion_original",
        F.col("fecha_actualizacion").cast("string")
    )

    # --------------------------------------------------------
    # Normalizacion auxiliar de estado_entrega
    # --------------------------------------------------------

    .withColumn(
        "_estado_entrega_normalizado",
        F.lower(
            F.trim(
                F.col("estado_entrega").cast("string")
            )
        )
    )
    .withColumn(
        "_estado_entrega_normalizado",
        F.regexp_replace(
            F.col("_estado_entrega_normalizado"),
            r"[\s\-]+",
            "_"
        )
    )
    .withColumn(
        "_estado_entrega_normalizado",
        F.regexp_replace(
            F.col("_estado_entrega_normalizado"),
            r"_+",
            "_"
        )
    )

    # --------------------------------------------------------
    # Estandarizacion de estado_entrega
    # --------------------------------------------------------

    .withColumn(
        "estado_entrega",
        F.when(
            F.col("_estado_entrega_normalizado") == "entregado",
            F.lit("entregado")
        )
        .when(
            F.col("_estado_entrega_normalizado").isin(
                "en_camino",
                "en_transito"
            ),
            F.lit("en_transito")
        )
        .when(
            F.col("_estado_entrega_normalizado") == "pendiente",
            F.lit("pendiente")
        )
        .when(
            F.col("_estado_entrega_normalizado") == "devuelto",
            F.lit("devuelto")
        )
        .otherwise(
            F.col("_estado_entrega_normalizado")
        )
    )

    # --------------------------------------------------------
    # Estandarizacion de courier
    # --------------------------------------------------------

    .withColumn(
        "_courier_normalizado",
        F.trim(
            F.col("courier").cast("string")
        )
    )
    .withColumn(
        "_courier_normalizado",
        F.regexp_replace(
            F.col("_courier_normalizado"),
            r"\s+",
            " "
        )
    )
    .withColumn(
        "courier",
        F.when(
            F.col("_courier_normalizado").isNull() |
            (F.col("_courier_normalizado") == ""),
            F.col("_courier_normalizado")
        ).otherwise(
            F.initcap(
                F.lower(
                    F.col("_courier_normalizado")
                )
            )
        )
    )

    # --------------------------------------------------------
    # Conversion de fecha_actualizacion
    # --------------------------------------------------------

    .withColumn(
        "fecha_actualizacion",
        F.coalesce(
            F.col("fecha_actualizacion").cast("timestamp"),
            F.to_timestamp(
                F.col("fecha_actualizacion"),
                "yyyy-MM-dd HH:mm:ss"
            ),
            F.to_timestamp(
                F.col("fecha_actualizacion"),
                "yyyy-MM-dd'T'HH:mm:ss"
            ),
            F.to_timestamp(
                F.col("fecha_actualizacion"),
                "dd/MM/yyyy HH:mm:ss"
            ),
            F.to_timestamp(
                F.col("fecha_actualizacion"),
                "yyyy-MM-dd"
            ),
            F.to_timestamp(
                F.col("fecha_actualizacion"),
                "dd/MM/yyyy"
            )
        )
    )

    # Eliminamos columnas auxiliares de normalizacion.
    # Las columnas originales se mantienen hasta formar
    # la tabla de cuarentena.
    .drop(
        "_estado_entrega_normalizado",
        "_courier_normalizado"
    )
)


# ============================================================
# REGLA 1
# TRACKING_ID REINGESTADO
# ============================================================

ventana_tracking = (
    Window
    .partitionBy("tracking_id")
    .orderBy(
        F.col("batch_id").asc_nulls_last(),
        F.col("ingestion_timestamp").asc_nulls_last()
    )
)

ventana_conteo_tracking = (
    Window
    .partitionBy("tracking_id")
)

df_con_numero_reingesta = (
    df_base
    .withColumn(
        "_numero_reingesta",
        F.row_number().over(
            ventana_tracking
        )
    )
    .withColumn(
        "_cantidad_reingestas",
        F.count(
            F.lit(1)
        ).over(
            ventana_conteo_tracking
        )
    )
)


# ------------------------------------------------------------
# Reingestas a cuarentena
# ------------------------------------------------------------

cuarentena_tracking_reingestado = (
    df_con_numero_reingesta
    .filter(
        F.col("tracking_id").isNotNull()
        &
        (F.col("tracking_id") != "")
        &
        (F.col("_numero_reingesta") > 1)
    )
    .withColumn(
        "motivo_error",
        F.lit("TRACKING_ID_REINGESTADO")
    )
)

df_validacion = (
    df_con_numero_reingesta
    .filter(
        (F.col("_numero_reingesta") == 1)
        |
        F.col("tracking_id").isNull()
        |
        (F.col("tracking_id") == "")
    )
    .drop(
        "_numero_reingesta",
        "_cantidad_reingestas"
    )
)


# ============================================================
# REGLA 2 - FECHA_ACTUALIZACION NULA O NO CONVERTIBLE
# ============================================================

cuarentena_fecha_actualizacion_nula = (
    df_validacion
    .filter(
        F.col("fecha_actualizacion").isNull()
    )
    .withColumn(
        "motivo_error",
        F.lit("FECHA_ACTUALIZACION_NULA")
    )
)


# Solo los registros con fecha valida continuan a Silver.

df_validacion = (
    df_validacion
    .filter(
        F.col("fecha_actualizacion").isNotNull()
    )
)


# ============================================================
# 3. LIMPIEZA DE COLUMNAS TECNICAS TEMPORALES
# ============================================================

cuarentena_tracking_reingestado = (
    cuarentena_tracking_reingestado
    .drop(
        "_numero_reingesta",
        "_cantidad_reingestas"
    )
)


# ============================================================
# 4. TABLA DE CUARENTENA
# ============================================================

df_cuarentena = (
    cuarentena_tracking_reingestado

    .unionByName(
        cuarentena_fecha_actualizacion_nula,
        allowMissingColumns=True
    )

    .withColumn(
        "fuente_origen",
        F.lit("tracking_envios_bronze")
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
# 5. TABLA VALIDA FINAL PARA SILVER
# ============================================================

df_tracking_envios_validos = (
    df_validacion

    .drop(
        "estado_entrega_original",
        "courier_original",
        "fecha_actualizacion_original"
    )
)


# ============================================================
# 6. GUARDAR CUARENTENA
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
        "electrocasa.auditoria.tracking_envios_cuarentena"
    )
)


# ============================================================
# 7. GUARDAR SILVER
# ============================================================

(
    df_tracking_envios_validos
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "electrocasa.silver.tracking_envios_silver"
    )
)

# # ============================================================
# # 8. VALIDACIONES DE CONTROL
# # ============================================================

# print("Proceso de tracking de envios completado.")

# print(
#     "Registros en Bronze:",
#     df_tracking_envios_bronze.count()
# )

# print(
#     "Registros validos en Silver:",
#     df_tracking_envios_validos.count()
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

# print("Valores estandarizados de estado_entrega:")

# (
#     df_tracking_envios_validos
#     .groupBy("estado_entrega")
#     .count()
#     .orderBy("estado_entrega")
#     .show(
#         truncate=False
#     )
# )

# print("Valores estandarizados de courier:")

# (
#     df_tracking_envios_validos
#     .groupBy("courier")
#     .count()
#     .orderBy("courier")
#     .show(
#         truncate=False
#     )
# )