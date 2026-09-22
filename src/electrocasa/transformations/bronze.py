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

print(f"Filas agregadas: {df_catalogo_bronze.count()}")