# fct_trips.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, round, broadcast
from delta.tables import DeltaTable # pyright: ignore[reportMissingImports]

def process_gold_fct_trips(spark: SparkSession, config: dict):
    """
    Procesa la tabla de hechos fct_trips enriqueciéndola con zonas de manera incremental
    mediante una estrategia MERGE de Delta Lake.
    """
    print("🏆 [Gold] Procesando tabla de hechos: fct_trips (Incremental)")
    
    # 1. Cargar orígenes
    df_trips = spark.read.table(config["source_trips"])
    df_zones = spark.read.table(config["source_zones"])
    df_zones_bcast = broadcast(df_zones)
    
    # 2. Lógica Incremental: Buscar la última fecha procesada en el destino (Target)
    target_table_name = config["target_fct_trips"]
    is_incremental = spark.catalog.tableExists(target_table_name)
    
    if is_incremental:
        print("🔄 [Incremental] Detectada tabla existente. Filtrando nuevos registros...")
        # Buscar el MAX(pickup_datetime) en la tabla Gold actual
        max_pickup = spark.table(target_table_name).selectExpr("max(pickup_datetime)").collect()[0][0]
        if max_pickup:
            # Filtrar el DataFrame de origen para traer solo datos más nuevos
            df_trips = df_trips.filter(col("pickup_datetime") > max_pickup)
            print(f"📥 Registros nuevos a procesar a partir de: {max_pickup}")
    else:
        print("🆕 [Full Load] Tabla destino no existe. Iniciando carga histórica completa...")

    # 3. Enriquecer con zonas (Left Join) y calcular duración (Emulando macros de dbt)
    df_enriched = (df_trips.alias("t")
        .join(df_zones_bcast.alias("pz"), col("t.pickup_location_id") == col("pz.location_id"), "left")
        .join(df_zones_bcast.alias("dz"), col("t.dropoff_location_id") == col("dz.location_id"), "left")
        .select(
            col("t.trip_id"),
            col("t.vendor_id"),
            col("t.service_type"),
            col("t.rate_code_id"),
            col("t.pickup_location_id"),
            col("pz.borough").alias("pickup_borough"),
            col("pz.zone").alias("pickup_zone"),
            col("t.dropoff_location_id"),
            col("dz.borough").alias("dropoff_borough"),
            col("dz.zone").alias("dropoff_zone"),
            col("t.pickup_datetime"),
            col("t.dropoff_datetime"),
            col("t.store_and_fwd_flag"),
            col("t.passenger_count"),
            col("t.trip_distance"),
            col("t.trip_type"),
            round((col("t.dropoff_datetime").cast("long") - col("t.pickup_datetime").cast("long")) / 60, 2).alias("trip_duration_minutes"),
            col("t.fare_amount"),
            col("t.extra"),
            col("t.mta_tax"),
            col("t.tip_amount"),
            col("t.tolls_amount"),
            col("t.ehail_fee"),
            col("t.improvement_surcharge"),
            col("t.total_amount"),
            col("t.payment_type"),
            col("t.payment_type_description")
        ))

    # 4. Escritura con Estrategia MERGE (Upsert)
    if is_incremental:
        delta_target = DeltaTable.forName(spark, target_table_name)
        (delta_target.alias("target")
         .merge(df_enriched.alias("source"), "target.trip_id = source.trip_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        print(f"✅ MERGE incremental ejecutado con éxito en: {target_table_name}\n")
    else:
        # Si es la primera ejecución, se crea la tabla Delta directamente
        (df_enriched.write
         .format("delta")
         .mode("overwrite")
         .saveAsTable(target_table_name))
        print(f"✅ Tabla fct_trips creada e instanciada en: {target_table_name}\n")