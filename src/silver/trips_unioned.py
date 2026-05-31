from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

def _align_and_filter_taxi(df: DataFrame, service_type: str) -> DataFrame:
    """
    Align the taxi DataFrame to a common schema and filter by service type.

    Args:
        df (DataFrame): The input DataFrame.
        service_type (str): The service type to filter by (e.g., "yellow", "green", "fhv").

    Returns:
        DataFrame: The aligned and filtered DataFrame.
    """
    # 1. Identificar dinámicamente las columnas de fecha (varían entre Yellow y Green)
    pickup_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else "lpep_pickup_datetime"
    dropoff_col = "tpep_dropoff_datetime" if "tpep_dropoff_datetime" in df.columns else "lpep_dropoff_datetime"

    # 2. Manejo condicional para columnas exclusivas de Green Taxis
    trip_type_col = col("trip_type") if "trip_type" in df.columns else lit(1)
    ehail_fee_col = col("ehail_fee") if "ehail_fee" in df.columns else lit(0.0)

    # 3. Selección y Cast Homogéneo (dejamos nombres base para que el transformador ponga los '_id')
    df_aligned = df.select(
        col("vendorid").cast("int").alias("vendor_id"),
        col("ratecodeid").cast("int").alias("ratecode_id"),
        col("pulocationid").cast("int").alias("pickup_location_id"),
        col("dolocationid").cast("int").alias("dropoff_location_id"),
        col(pickup_col).cast("timestamp").alias("pickup_datetime"),
        col(dropoff_col).cast("timestamp").alias("dropoff_datetime"),
        col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        col("passenger_count").cast("int").alias("passenger_count"),
        col("trip_distance").cast("double").alias("trip_distance"),
        trip_type_col.cast("int").alias("trip_type"),
        col("fare_amount").cast("double").alias("fare_amount"),
        col("extra").cast("double").alias("extra"),
        col("mta_tax").cast("double").alias("mta_tax"),
        col("tip_amount").cast("double").alias("tip_amount"),
        col("tolls_amount").cast("double").alias("tolls_amount"),
        ehail_fee_col.cast("double").alias("ehail_fee"),
        col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        col("total_amount").cast("double").alias("total_amount"),
        col("payment_type").cast("int").alias("payment_type")
    )

    # 4. Filtrar registros corruptos donde no haya vendor_id
    df_filtered = df_aligned.filter((col("vendor_id").isNotNull()))

    # 5. Agregar etiqueta de tipo de servicio
    df_final = df_filtered.withColumn("service_type", lit(service_type))

    return df_final

def process_silver_trips_unioned(spark: SparkSession, config: dict):
    """"
    Process the trips data by unifying and cleaning the yellow and green taxi datasets.
    Args:
        spark (SparkSession): The Spark session.
        config (dict): A dictionary containing configuration parameters.
    """

    print("🚀 [Silver Trips] Iniciando unificación y limpieza de hechos...")
    
    # 1. Leer orígenes de Bronze
    df_yellow_raw = spark.read.table(config["source_yellow"])
    df_green_raw = spark.read.table(config["source_green"])
    
    # 2. Alinear y limpiar individualmente
    df_yellow = _align_and_filter_taxi(df_yellow_raw, "Yellow")
    df_green = _align_and_filter_taxi(df_green_raw, "Green")
    
    # 3. Unificación (Si necesitaramos soportar columnas faltantes rellenando con Null utlizamos allowMissingColumns=True)
    df_unioned = df_yellow.unionByName(df_green)
    
    # 4. Guardar el tablón unificado intermedio
    df_unioned.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(config["target_unioned"])
            
    print(f"✅ Capa Silver consolidada de viajes guardada exitosamente en {config['target_unioned']}\n")