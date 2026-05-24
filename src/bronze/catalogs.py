from pyspark.sql import SparkSession
from src.bronze.base import BronzeBasePipeline

def ingest_json_zones(spark: SparkSession, config: dict) -> None:
    """Ingest zone master data from a JSON source and write it to the destination path.
    Args:
        spark (SparkSession): The SparkSession object.
        config (dict): A dictionary containing configuration parameters, including 'source_path' and 'target_table'.
    """

    print(f"🗺️ [Catalog Pipeline] Cargando maestro de zonas JSON: {config['name']}")

    # 1. Lectura del maestro de zonas desde JSON
    df_raw = (spark.read.format("json").load(config['source_path']))

    # 2. Inyección de columnas de auditoría base
    df_audited = BronzeBasePipeline.add_audit_columns(df_raw)

    # 3. # Sobreescritura limpia para asegurar idempotencia en maestros
    (df_audited.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(config['target_table']))
    
    print(f"✅ [Catalog Pipeline] Maestro JSON guardado en {config['target_table']}\n")

def ingest_csv_description(spark: SparkSession, config: dict) -> None:
    """Ingest description master data from a CSV source and write it to the destination path.
    Args:
        spark (SparkSession): The SparkSession object.
        config (dict): A dictionary containing configuration parameters, including 'source_path' and 'target_table'.
    """

    print(f"📝 [Catalog Pipeline] Cargando maestro de descripciones CSV: {config['name']}")

    # 1. Lectura batch del maestro de descripciones desde CSV
    df_raw = (spark.read
               .format("csv")
               .option("header", "true")
               .option("inferSchema", "true")
               .load(config['source_path']))
    
    # 2. Inyección de columnas de auditoría base
    df_audited = BronzeBasePipeline.add_audit_columns(df_raw)

    # 3. Sobreescritura limpia para asegurar idempotencia en maestros
    (df_audited.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(config['target_table']))

    print(f"✅ [Catalog Pipeline] Maestro CSV guardado en {config['target_table']}\n")