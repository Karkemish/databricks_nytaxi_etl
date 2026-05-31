from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
from src.silver.base_transformer import SilverBaseTransformer

def process_dimensions_zone(spark: SparkSession, dim_config: dict):
    """
    Process the dimensions data and write it to the dimensions zone.

    Args:
        spark (SparkSession): The Spark session.
        dim_config (dict): A dictionary containing configuration parameters.
    """
    print(f"📦 [Silver Dimensions] Procesando dimensión: {dim_config['name']}")
    # Read the input data
    df_bronze = spark.read.table(dim_config["source_table"])

    # Perform transformations (placeholder for actual transformation logic)
    # For example, we can standardize column names using SilverBaseTransformer
    
    df_final =(df_bronze.select(
        col("locationid").cast("int"),
        col("location.borough").cast("string").alias("borough"),
        col("location.zone").cast("string").alias("zone"),
        col("location.service_zone").cast("string").alias("service_zone")
    ))

    df_final = SilverBaseTransformer.standardize_column_names(df_final)
    df_final = (df_final
                .withColumn("processed_at", current_timestamp())
                .dropDuplicates())

    # Write the transformed data to the dimensions zone
    (df_final.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(dim_config["target_table"]))

    print(f"✅ Dimensión guardada con éxito en: {dim_config['target_table']}\n")
    
def process_dimensions_description(spark: SparkSession, dim_config: dict):
    """
    Process the dimensions description data and write it to the dimensions zone.

    Args:
        spark (SparkSession): The Spark session.
        dim_config (dict): A dictionary containing configuration parameters.
    """
    print(f"📦 [Silver Dimensions] Procesando descripción de dimensión: {dim_config['name']}")
    # Read the input data
    df_bronze = spark.read.table(dim_config["source_table"])

    df_clean = SilverBaseTransformer.standardize_column_names(df_bronze)

    # Perform transformations (placeholder for actual transformation logic)
    df_final = df_clean.select(
        col("id").cast("int"),
        col("group_code").cast("string"),
        col("group_description").cast("string"),
        col("code").cast("string"),
        col("description").cast("string").alias("code_description")
                                )

    df_final = (df_final
                .withColumn("group_code", expr("substring(group_code, 1, 3)"))
                .withColumn("processed_at", current_timestamp())
                .dropDuplicates()
                )
    # Write the transformed data to the dimensions zone
    (df_final.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(dim_config["target_table"]))

    print(f"✅ Descripción de dimensión guardada con éxito en: {dim_config['target_table']}\n")
