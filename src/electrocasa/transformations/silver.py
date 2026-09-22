# ============================================================
# SILVER - VENTAS (ventas_bronze)
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    when,
    to_date
)


# 1. LECTURA BRONZE
df_ventas_bronze = spark.table(
    "electrocasa.bronze.ventas_bronze"
)


# 2. TRANSFORMACIONES
df_ventas_transformadas = (

    df_ventas_bronze

    # -----------------
    # metodo_pago_std
    # -----------------
    .withColumn(
        "metodo_pago_std",
        F.when(
            F.upper(F.trim(F.col("metodo_pago"))).isin(
                "TARJETA",
                "TC"
            ),
            "TARJETA"
        )
        .when(
            F.lower(
                F.trim(F.col("metodo_pago"))
            ).isin(
                "tarjeta de credito",
                "tarjeta_credito"
            ),
            "TARJETA"
        )
        .when(
            F.upper(
                F.trim(F.col("metodo_pago"))
            ).isin(
                "EFECTIVO",
                "EFV"
            ),
            "EFECTIVO"
        )
        .when(
            F.upper(
                F.trim(F.col("metodo_pago"))
            ) == "YAPE",
            "YAPE"
        )
        .when(
            F.upper(
                F.trim(F.col("metodo_pago"))
            ) == "PLIN",
            "PLIN"
        )
        .when(
            F.lower(
                F.trim(F.col("metodo_pago"))
            ).contains(
                "transfer"
            ),
            "TRANSFERENCIA"
        )
        .otherwise("OTRO")
    )

    # -----------------
    # fecha_venta_std
    # -----------------
    .withColumn(
        "fecha_venta_std",
        when(
            col("fecha_venta").rlike(
                r"^\d{4}-\d{2}-\d{2}$"
            ),
            to_date(
                col("fecha_venta"),
                "yyyy-MM-dd"
            )
        )
        .when(
            col("fecha_venta").rlike(
                r"^\d{2}/\d{2}/\d{4}$"
            ),
            to_date(
                col("fecha_venta"),
                "dd/MM/yyyy"
            )
        )
    )
)


# 3. DEDUPLICACION
ventana_venta = (
    Window
    .partitionBy("venta_id")
    .orderBy(
        F.col("batch_id").asc_nulls_last()
    )
)

ventana_conteo = (
    Window
    .partitionBy("venta_id")
)

df_ventas_numeradas = (
    df_ventas_transformadas
    .withColumn(
        "rn_venta",
        F.row_number()
        .over(ventana_venta)
    )
    .withColumn(
        "cantidad_registros_venta_id",
        F.count(
            F.lit(1)
        ).over(
            ventana_conteo
        )
    )
)

df_ventas_sin_duplicados = (
    df_ventas_numeradas
    .filter(
        F.col("rn_venta") == 1
    )
)


# 4. CONDICIONES DE CALIDAD (monto_total y cantidad)
condicion_monto_invalido = (
    F.col("monto_total").isNull()
    |
    (F.col("monto_total") <= 0)
)

condicion_cantidad_invalida = (
    F.col("cantidad").isNull()
    |
    (F.col("cantidad") <= 0)
)

condicion_sucursal_invalida = (
    F.col("sucursal_id").isNull()
    |
    (
        F.trim(
            F.col("sucursal_id")
            .cast("string")
        ) == ""
    )
)


# 5. CUARENTENA DUPLICADOS
df_cuarentena_duplicados = (

    df_ventas_numeradas

    .filter(
        F.col("rn_venta") > 1
    )

    .withColumn(
        "regla_rechazo",
        F.lit(
            "VENTA_ID_DUPLICADO"
        )
    )

    .withColumn(
        "motivo_rechazo",
        F.concat(
            F.lit(
                "Reingesta duplicada. Copia "
            ),
            F.col("rn_venta"),
            F.lit(
                " de "
            ),
            F.col(
                "cantidad_registros_venta_id"
            )
        )
    )

    .withColumn(
        "columna_rechazo",
        F.lit("venta_id")
    )

    .withColumn(
        "valor_rechazado",
        F.col("venta_id")
        .cast("string")
    )

    .withColumn(
        "fecha_procesamiento",
        F.current_timestamp()
    )
)


# 6. CUARENTENA MONTO
df_cuarentena_monto = (

    df_ventas_sin_duplicados

    .filter(
        condicion_monto_invalido
    )

    .withColumn(
        "regla_rechazo",
        F.lit(
            "MONTO_TOTAL_INVALIDO"
        )
    )

    .withColumn(
        "motivo_rechazo",
        F.lit(
            "monto_total es nulo, cero o negativo"
        )
    )

    .withColumn(
        "columna_rechazo",
        F.lit(
            "monto_total"
        )
    )

    .withColumn(
        "valor_rechazado",
        F.col("monto_total")
        .cast("string")
    )

    .withColumn(
        "fecha_procesamiento",
        F.current_timestamp()
    )
)

# 7. CUARENTENA CANTIDAD
df_cuarentena_cantidad = (

    df_ventas_sin_duplicados

    .filter(
        condicion_cantidad_invalida
    )

    .withColumn(
        "regla_rechazo",
        F.lit(
            "CANTIDAD_INVALIDA"
        )
    )

    .withColumn(
        "motivo_rechazo",
        F.lit(
            "cantidad es nula o menor o igual a cero"
        )
    )

    .withColumn(
        "columna_rechazo",
        F.lit(
            "cantidad"
        )
    )

    .withColumn(
        "valor_rechazado",
        F.col("cantidad")
        .cast("string")
    )

    .withColumn(
        "fecha_procesamiento",
        F.current_timestamp()
    )
)

# 8. CUARENTENA SUCURSAL
df_cuarentena_sucursal = (
    df_ventas_sin_duplicados
    .filter(
        condicion_sucursal_invalida
    )

    .withColumn(
        "regla_rechazo",
        F.lit(
            "SUCURSAL_ID_NULO"
        )
    )

    .withColumn(
        "motivo_rechazo",
        F.lit(
            "sucursal_id nulo o vacío"
        )
    )

    .withColumn(
        "columna_rechazo",
        F.lit(
            "sucursal_id"
        )
    )

    .withColumn(
        "valor_rechazado",
        F.col("sucursal_id")
        .cast("string")
    )

    .withColumn(
        "fecha_procesamiento",
        F.current_timestamp()
    )
)


# 9. TABLA DE CUARENTENA UNIFICADA - VENTAS
df_ventas_cuarentena = (
    df_cuarentena_duplicados
    .unionByName(
        df_cuarentena_monto,
        allowMissingColumns=True
    )

    .unionByName(
        df_cuarentena_cantidad,
        allowMissingColumns=True
    )

    .unionByName(
        df_cuarentena_sucursal,
        allowMissingColumns=True
    )

    .drop(
        "rn_venta",
        "cantidad_registros_venta_id"
    )
)


# 10. SILVER
df_ventas_silver = (
    df_ventas_sin_duplicados
    .filter(
        ~condicion_monto_invalido
    )
    .filter(
        ~condicion_cantidad_invalida
    )
    .filter(
        ~condicion_sucursal_invalida
    )
    .drop(
        "rn_venta",
        "cantidad_registros_venta_id"
    )
)


# 11. GUARDAR CUARENTENA
(
    df_ventas_cuarentena.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "electrocasa.auditoria.ventas_cuarentena"
    )
)

# 12. GUARDAR SILVER
(
    df_ventas_silver.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "electrocasa.silver.ventas_silver"
    )
)