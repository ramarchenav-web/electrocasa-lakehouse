# ============================================================
# SILVER - electrocasa.bronze.empleados_bronze
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# 1. LECTURA BRONZE
# ============================================================

df_empleados_bronze = spark.table(
    "electrocasa.bronze.empleados_bronze"
)

# ============================================================
# 2. NORMALIZACION INICIAL
# ============================================================

df_base = (
    df_empleados_bronze
    .withColumn(
        "dni",
        F.trim(F.col("dni"))
    )
    .withColumn(
        "salario",
        F.col("salario").cast("decimal(12,2)")
    )
)

# ============================================================
# REGLA 1
# DNI NULO O VACIO
# ============================================================

cuarentena_dni_nulo = (
    df_base
    .filter(
        F.col("dni").isNull() |
        (F.trim(F.col("dni")) == "")
    )
    .withColumn(
        "motivo_error",
        F.lit("DNI_NULO_O_VACIO")
    )
)

df_validacion = (
    df_base
    .filter(F.col("dni").isNotNull())
    .filter(F.trim(F.col("dni")) != "")
)

# ============================================================
# REGLA 2
# MISMO ID_EMPLEADO CON MULTIPLES DNI
# ============================================================

empleados_dni_conflictivos = (
    df_validacion
    .groupBy("id_empleado")
    .agg(
        F.countDistinct("dni")
        .alias("cantidad_dnis")
    )
    .filter(F.col("cantidad_dnis") > 1)
    .select("id_empleado")
)

cuarentena_multiples_dni = (
    df_validacion
    .join(
        empleados_dni_conflictivos,
        on="id_empleado",
        how="inner"
    )
    .withColumn(
        "motivo_error",
        F.lit("ID_EMPLEADO_MULTIPLES_DNI")
    )
)

df_validacion = (
    df_validacion
    .join(
        empleados_dni_conflictivos,
        on="id_empleado",
        how="left_anti"
    )
)

# ============================================================
# REGLA 3
# MISMO DNI EN MULTIPLES EMPLEADOS
# ============================================================

dni_repetidos = (
    df_validacion
    .groupBy("dni")
    .agg(
        F.countDistinct("id_empleado")
        .alias("cantidad_empleados")
    )
    .filter(F.col("cantidad_empleados") > 1)
    .select("dni")
)

cuarentena_dni_repetido = (
    df_validacion
    .join(
        dni_repetidos,
        on="dni",
        how="inner"
    )
    .withColumn(
        "motivo_error",
        F.lit("DNI_ASOCIADO_A_MULTIPLES_EMPLEADOS")
    )
)

df_validacion = (
    df_validacion
    .join(
        dni_repetidos,
        on="dni",
        how="left_anti"
    )
)

# ============================================================
# REGLA 4
# ESTANDARIZACION Y VALIDACION TIPO_EVENTO
# ============================================================

eventos_validos = [
    "alta",
    "transferencia",
    "cambio_salario",
    "baja"
]

df_validacion = (
    df_validacion
    .withColumn(
        "tipo_evento_std",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.col("tipo_evento"),
                    r"[\s_]+",
                    "_"
                )
            )
        )
    )
)

cuarentena_evento_invalido = (
    df_validacion
    .filter(
        ~F.col("tipo_evento_std").isin(eventos_validos)
    )
    .withColumn(
        "motivo_error",
        F.lit("TIPO_EVENTO_INVALIDO")
    )
)

df_validacion = (
    df_validacion
    .filter(
        F.col("tipo_evento_std").isin(eventos_validos)
    )
)

df_validacion = (
    df_validacion
    .drop("tipo_evento")
    .withColumnRenamed(
        "tipo_evento_std",
        "tipo_evento"
    )
)

# ============================================================
# REGLA 5
# SALARIO
# ============================================================

cuarentena_salario = (
    df_validacion
    .filter(
        F.col("salario").isNull() |
        (F.col("salario") <= 0)
    )
    .withColumn(
        "motivo_error",
        F.lit("SALARIO_INVALIDO")
    )
)

df_validacion = (
    df_validacion
    .filter(
        F.col("salario").isNotNull()
    )
    .filter(
        F.col("salario") > 0
    )
)

# ============================================================
# REGLA 6
# FECHA_EVENTO
# ============================================================

cuarentena_fecha_evento = (
    df_validacion
    .filter(
        F.col("fecha_evento").isNull() |
        (F.trim(F.col("fecha_evento")) == "")
    )
    .withColumn(
        "motivo_error",
        F.lit("FECHA_EVENTO_NULA")
    )
)

df_validacion = (
    df_validacion
    .filter(F.col("fecha_evento").isNotNull())
    .filter(F.trim(F.col("fecha_evento")) != "")
)

# Conversión a DATE para futura historización

df_validacion = (
    df_validacion
    .withColumn(
        "fecha_evento",
        F.to_date(F.col("fecha_evento"))
    )
)

# ============================================================
# TABLA DE CUARENTENA
# ============================================================

df_cuarentena = (
    cuarentena_dni_nulo
    .unionByName(
        cuarentena_multiples_dni,
        allowMissingColumns=True
    )
    .unionByName(
        cuarentena_dni_repetido,
        allowMissingColumns=True
    )
    .unionByName(
        cuarentena_evento_invalido,
        allowMissingColumns=True
    )
    .unionByName(
        cuarentena_salario,
        allowMissingColumns=True
    )
    .unionByName(
        cuarentena_fecha_evento,
        allowMissingColumns=True
    )
    .withColumn(
        "fuente_origen",
        F.lit("empleados_bronze")
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
# TABLA VALIDA FINAL PARA SILVER
# ============================================================

df_empleados_validos = df_validacion

# ============================================================
# GUARDAR CUARENTENA
# ============================================================

(
    df_cuarentena
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "electrocasa.auditoria.empleados_cuarentena"
    )
)

# ============================================================
# GUARDAR SILVER
# ============================================================

(
    df_empleados_validos
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "electrocasa.silver.empleados_silver"
    )
)
