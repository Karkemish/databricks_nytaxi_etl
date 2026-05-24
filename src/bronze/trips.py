from pyspark.sql import SparkSession
from src.bronze.base import BronzeBasePipeline

def ingest_taxi_trips(spark: SparkSession, config: dict) -> None:
    """
    Ingest taxi trip data from the specified source path and write it to the destination path.

    Args:
        spark (SparkSession): The SparkSession object.
        config (dict): A dictionary containing configuration parameters, including 'source_path' and 'destination_path'.
    """
    print(f"🚖 [Trips Pipeline] Iniciando streaming con Auto Loader para: {config['name']}")

    # 1. Configuración del lector Auto Loader
    df_stream = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("header", "true")
                .schema(config['schema'])
                .load(config['source_path']))
    
    # 2. Inyección de columnas de auditoría base
    df_audited = BronzeBasePipeline.add_audit_columns(df_stream)

    # 3. Escritura incremental con Checkpoint dedicado
    query = (df_audited.writeStream
             .format("delta")
             .outputMode("append")
             .option("checkpointLocation", config['checkpoint_path'])
             .option("mergeSchema", "true")
             .trigger(availableNow=True)
             .toTable(config['target_table']))
    
    query.awaitTermination()
    print(f"✅ [Trips Pipeline] Datos de {config['name']} guardados incrementalmente en {config['target_table']}\n")