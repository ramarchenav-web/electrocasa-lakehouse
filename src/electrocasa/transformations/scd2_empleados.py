# ============================================================
# SCD TYPE 2 - EMPLEADOS
# HISTORIZACION DE CAMBIOS DE CARGO
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# 1. LECTURA DE SILVER LIMPIA
# ============================================================

df_empleados = spark.table(
    "electrocasa.silver.empleados_validos"
)

# ============================================================
# 2. ORDEN CRONOLOGICO POR EMPLEADO
# ============================================================

window_empleado = (
    Window
    .partitionBy("id_empleado")
    .orderBy("fecha_evento")
)

# ============================================================
# 3. IDENTIFICAR CAMBIOS DE CARGO
# ============================================================

df_hist = (
    df_empleados
    .withColumn(
        "cargo_anterior",
        F.lag("cargo").over(window_empleado)
    )
)

# Primer registro del empleado o cambio de cargo
df_hist = (
    df_hist
    .withColumn(
        "nuevo_periodo",
        F.when(
            F.col("cargo_anterior").isNull(),
            1
        )
        .when(
            F.col("cargo") != F.col("cargo_anterior"),
            1
        )
        .otherwise(0)
    )
)

# ============================================================
# 4. GENERAR VERSIONES SCD2
# ============================================================

df_hist = (
    df_hist
    .withColumn(
        "version_scd",
        F.sum("nuevo_periodo").over(
            window_empleado.rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow
            )
        )
    )
)

# ============================================================
# 5. AGRUPAR PERIODOS DE CARGO
# ============================================================

df_scd2 = (
    df_hist
    .groupBy(
        "id_empleado",
        "version_scd"
    )
    .agg(
        F.first("nombre").alias("nombre"),
        F.first("dni").alias("dni"),
        F.first("email").alias("email"),
        F.first("cargo").alias("cargo"),
        F.first("salario").alias("salario"),
        F.first("sucursal_id").alias("sucursal_id"),

        F.min("fecha_evento").alias(
            "fecha_inicio"
        )
    )
)

# ============================================================
# 6. FECHA FIN DEL PERIODO
# ============================================================

window_version = (
    Window
    .partitionBy("id_empleado")
    .orderBy("fecha_inicio")
)

df_scd2 = (
    df_scd2
    .withColumn(
        "siguiente_inicio",
        F.lead("fecha_inicio").over(window_version)
    )
)

df_scd2 = (
    df_scd2
    .withColumn(
        "fecha_fin",
        F.date_sub(
            F.col("siguiente_inicio"),
            1
        )
    )
)

# ============================================================
# 7. REGISTRO VIGENTE
# ============================================================

df_scd2 = (
    df_scd2
    .withColumn(
        "es_actual",
        F.when(
            F.col("siguiente_inicio").isNull(),
            True
        )
        .otherwise(False)
    )
)

# ============================================================
# 8. FECHA ABIERTA PARA REGISTRO ACTUAL
# ============================================================

df_scd2 = (
    df_scd2
    .withColumn(
        "fecha_fin",
        F.when(
            F.col("es_actual"),
            F.to_date(
                F.lit("9999-12-31")
            )
        )
        .otherwise(F.col("fecha_fin"))
    )
)

# ============================================================
# 9. COLUMNAS FINALES
# ============================================================

df_scd2 = (
    df_scd2
    .select(
        "id_empleado",
        "nombre",
        "dni",
        "email",
        "cargo",
        "salario",
        "sucursal_id",
        "fecha_inicio",
        "fecha_fin",
        "es_actual",
        "version_scd"
    )
)

# ============================================================
# 10. AUDITORIA
# ============================================================

df_scd2 = (
    df_scd2
    .withColumn(
        "fecha_procesamiento",
        F.current_timestamp()
    )
)

# ============================================================
# 11. GUARDAR TABLA HISTORIZADA
# ============================================================

(
    df_scd2
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "electrocasa.auditoria.empleados_cargo_scd2"
    )
)
