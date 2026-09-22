# ============================================================
# FUENTE: Reseña de clientes
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

df_resenas_bronze = (
    spark.read
        .format("json")
        .schema(schema_resenas)
        .load("/Volumes/electrocasa/bronze/vol_landing/resenas_clientes/")
        .withColumns({
            "ingestion_ts": current_timestamp(),
            "source_file": col("_metadata.file_path"),
            "_batch_id": lit(None).cast("string"),
        })
)

(
    df_resenas_bronze.write
        .mode("append")
        .saveAsTable("electrocasa.bronze.resenas_clientes_bronze")
)

print(f"Filas agregadas: {df_resenas_bronze.count()}")