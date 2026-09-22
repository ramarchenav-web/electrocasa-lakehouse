# ============================================================
# BRONZE
# FUENTE: Ventas por sucursal
# ============================================================
from pyspark.sql import functions as F

path_ventas = (
    "/Volumes/electrocasa/bronze/vol_landing/"
    "ventas/ventas_sucursales.csv"
)

df_ventas_bronze = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path_ventas)
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "source_system",
            F.lit("ventas_sucursal")
        )
        .withColumn(
            "source_file",
            F.lit("ventas_sucursales.csv")
        )
        .withColumn(
            "batch_id",
            F.lit("batch_001")
        )
)
(
    df_ventas_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("electrocasa.bronze.ventas_bronze")
)
# print(
#     f"Filas agregadas: {df_ventas_bronze.count()}"
# )



# ============================================================
# FUENTE: Catalogo de productos
# ============================================================
from pyspark.sql.functions import current_timestamp, lit

df_catalogo_bronze = (
    spark.read
        .option("multiline", "true")
        .json("/Volumes/electrocasa/bronze/vol_landing/catalogo_productos/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit("catalogo_productos.json"))
)

(
    df_catalogo_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            "electrocasa.bronze.catalogo_productos_bronze"
        )
)
# ============================================================
# BRONZE - RESEÑAS CLIENTES
# ============================================================

from pyspark.sql.types import *
from pyspark.sql.functions import *

schema_resenas = StructType([
    StructField("resena_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("calificacion", IntegerType(), True),
    StructField("comentario", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField(
        "respuestas",
        ArrayType(
            StructType([
                StructField("autor", StringType(), True),
                StructField("texto", StringType(), True)
            ])
        ),
        True
    ),
    StructField("fecha_resena", StringType(), True)
])

(
    spark.read
        .format("json")
        .schema(schema_resenas)
        .load("/Volumes/electrocasa/bronze/vol_landing/resenas_clientes/")
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("batch_id", lit("001"))
        .write
        .mode("overwrite")
        .saveAsTable("electrocasa.bronze.resenas_clientes_bronze")
)

print("Bronze cargada correctamente")
