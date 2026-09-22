# BRONZE
# 1. FUENTE: Ventas por sucursal
from pyspark.sql import functions as F

path_ventas = (
    "/Volumes/electrocasa/bronze/vol_landing/"
    "ventas_sucursales/ventas_sucursales.csv"
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

# 2. FUENTE: Catalogo de productos
from pyspark.sql.functions import current_timestamp, lit

df_catalogo_bronze = (
    spark.read
        .option("multiline", "true")
        .json("/Volumes/electrocasa/bronze/vol_landing/catalogo_productos/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit("catalogo_productos.json"))
        .withColumn("source_system", lit("catalogo_productos"))
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

# 3. FUENTE: RESEÑAS CLIENTES

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
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("batch_id", lit("batch_001"))
        .write
        .mode("overwrite")
        .saveAsTable("electrocasa.bronze.resenas_clientes_bronze")
)
#print("Bronze cargada correctamente")

# 4. FUENTE: EMPLEADOS RRHH (AUTO LOADER)

from pyspark.sql.functions import *

checkpoint_path = (
    "/Volumes/electrocasa/bronze/vol_landing/"
    "empleados_rrhh/checkpoint")
schema_path = (
    "/Volumes/electrocasa/bronze/vol_landing/"
    "empleados_rrhh/schema")
origen_path = (
    "/Volumes/electrocasa/bronze/vol_landing/"
    "empleados_rrhh/data")

df_empleados_bronze = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("header", "true")
        .option(
            "rescuedDataColumn",
            "_rescued_data"
        )
        .load(origen_path)
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "source_system",
            lit("rrhh")
        )
        .withColumn(
            "source_file",
            col("_metadata.file_path")
        )
        .withColumn(
            "batch_id",
            lit("batch_001")
        )
)

query = (
    df_empleados_bronze.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            checkpoint_path
        )
        .trigger(availableNow=True)
        .toTable(
            "electrocasa.bronze.empleados_bronze"
        )
)
query.awaitTermination()


# 5. FUENTE: DEVOLUCIONES (AUTO LOADER)

from pyspark.sql.functions import *

checkpoint_path = (
    "/Volumes/electrocasa/bronze/vol_landing/"
    "devoluciones/checkpoint"
)

schema_path = (
    "/Volumes/electrocasa/bronze/vol_landing/"
    "devoluciones/schema"
)

origen_path = (
    "/Volumes/electrocasa/bronze/vol_landing/"
    "devoluciones/data"
)

df_devoluciones_bronze = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("header", "true")
        .option(
            "rescuedDataColumn",
            "_rescued_data"
        )
        .load(origen_path)
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "source_system",
            lit("devoluciones")
        )
        .withColumn(
            "source_file",
            col("_metadata.file_path")
        )
        .withColumn(
            "batch_id",
            lit("batch_001")
        )
)

query = (
    df_devoluciones_bronze.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            checkpoint_path
        )
        .trigger(availableNow=True)
        .toTable(
            "electrocasa.bronze.devoluciones_bronze"
        )
)
query.awaitTermination()


# 6. FUENTE: ENVIOS (JDBC - SUPABASE)
from pyspark.sql.functions import current_timestamp, lit

SUPABASE_HOST = "aws-0-us-west-2.pooler.supabase.com"
SUPABASE_DB = "postgres"
jdbc_url = (
    f"jdbc:postgresql://{SUPABASE_HOST}:5432/{SUPABASE_DB}"
)

jdbc_properties = {
    "user": dbutils.secrets.get(
        "electrocasa-secrets",
        "user"
    ),
    "password": dbutils.secrets.get(
        "electrocasa-secrets",
        "password"
    ),
    "driver": "org.postgresql.Driver"
}
df_tracking_bronze = (
    spark.read.jdbc(
        url=jdbc_url,
        table="public.trackingenvios",
        properties=jdbc_properties
    )
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    .withColumn(
        "source_system",
        lit("SUPABASE_POSTGRES")
    )
    .withColumn(
        "source_file",
        lit("public.trackingenvios")
    )
    .withColumn(
        "batch_id",
        lit("batch_001")
    )
)
(
    df_tracking_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema","true")
        .saveAsTable(
            "electrocasa.bronze.tracking_envios_bronze"
        )
)