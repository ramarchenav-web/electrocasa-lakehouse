# ============================================================
# SILVER - catalogo_productos
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    when,
    to_date
)


# 1. LECTURA BRONZE
df_productos_bronze = spark.table(
    "electrocasa.bronze.catalogo_productos_bronze"
)


# 2. TRANSFORMACIONES
catalogo_silver = (
    df_productos_bronze

    # ===== PRECIO =====
    .withColumn(
        "precio_lista_std",
        F.regexp_replace(
            F.trim(F.col("precio_lista").cast("string")),
            r"S/\s*",
            ""
        ).cast("decimal(12,2)")
    )

    # ===== MARCA =====
    .withColumn(
        "marca_std",
        F.when(
            F.col("marca").isNull() |
            (F.trim(F.col("marca")) == ""),
            F.lit("SIN_MARCA")
        ).otherwise(F.upper(F.trim(F.col("marca"))))
    )

    # ===== CATEGORIA =====
    .withColumn(
        "categoria_std",
        F.upper(
            F.trim(
                F.translate(
                    F.col("categoria"),
                    "áéíóúÁÉÍÓÚ",
                    "aeiouAEIOU"
                )
            )
        )
    )
)

catalogo_valido = catalogo_silver.filter(
    (F.col("producto_id").isNotNull()) &
    (F.trim(F.col("producto_id")) != "") &
    (F.col("precio_lista_std").isNotNull()) &
    (F.col("precio_lista_std") > 0)
)

catalogo_quarantine = (
    catalogo_silver
    .filter(
        (F.col("producto_id").isNull()) |
        (F.trim(F.col("producto_id")) == "") |
        (F.col("precio_lista_std").isNull()) |
        (F.col("precio_lista_std") <= 0)
    )
    .withColumn(
        "regla_rechazo",
        F.when(
            F.col("producto_id").isNull(),
            "PRODUCTO_ID_NULL"
        )
        .when(
            F.trim(F.col("producto_id")) == "",
            "PRODUCTO_ID_EMPTY"
        )
        .when(
            F.col("precio_lista_std").isNull(),
            "PRECIO_LISTA_NULL"
        )
        .when(
            F.col("precio_lista_std") <= 0,
            "PRECIO_LISTA_NEGATIVO_CERO"
        )
    )
    .withColumn(
        "motivo_rechazo",
        F.when(
            F.col("producto_id").isNull(),
            "Producto sin identificador"
        )
        .when(
            F.trim(F.col("producto_id")) == "",
            "Producto con identificador vacío"
        )
        .when(
            F.col("precio_lista_std").isNull(),
            "Precio no convertible a valor numérico"
        )
        .when(
            F.col("precio_lista_std") <= 0,
            "Precio menor o igual a cero"
        )
    )
    .withColumn(
        "columna_rechazo",
        F.when(
            (F.col("producto_id").isNull()) |
            (F.trim(F.col("producto_id")) == ""),
            F.lit("producto_id")
        )
        .otherwise(F.lit("precio_lista"))
    )
    .withColumn(
        "valor_rechazado",
        F.when(
            (F.col("producto_id").isNull()) |
            (F.trim(F.col("producto_id")) == ""),
            F.col("producto_id").cast("string")
        )
        .otherwise(F.col("precio_lista").cast("string"))
    )
    .withColumn(
        "fecha_procesamiento",
        F.current_timestamp()
    )
)

window_spec = (
    Window
    .partitionBy("producto_id")
    .orderBy(F.col("ingestion_timestamp").desc())
)

catalogo_valido = (
    catalogo_valido
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
    .filter(F.col("rn") == 1)
    .drop("rn")
)

catalogo_valido = (
    catalogo_valido
    .drop("precio_lista", "marca", "categoria")
    .withColumnRenamed(
        "precio_lista_std",
        "precio_lista"
    )
    .withColumnRenamed(
        "marca_std",
        "marca"
    )
    .withColumnRenamed(
        "categoria_std",
        "categoria"
    )
)

# 3. ESCRITURA SILVER
(
    catalogo_valido
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "electrocasa.silver.catalogo_productos_silver"
    )
)


# 4. ESCRITURA CUARENTENA
(
    catalogo_quarantine
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "electrocasa.auditoria.catalogo_productos_cuarentena"
    )
)


