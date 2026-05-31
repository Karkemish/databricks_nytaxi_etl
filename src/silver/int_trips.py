from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, lit, sha2, concat_ws, coalesce

def process_silver_int_trips(spark: SparkSession, config: dict):
    """
    Process the intermediate trips data by unifying and cleaning the yellow and green taxi datasets.

    Args:
        spark (SparkSession): The Spark session.
        config (dict): A dictionary containing configuration parameters.
    """
    print(f"📦 [Silver Trips] Procesando datos de viajes intermedios...")

    # 1. Leer orígenes de unioned silver
    unioned = spark.read.table(config["source_unioned"])
    payment_type_desc = spark.read.table(config["source_payment_type_desc"])
    payment_type_desc = broadcast(payment_type_desc.filter(col("group_code") == "002"))

    # 2. Realizar transformaciones adicionales (ejemplo: generar trip_id, enriquecer con descripción de payment_type)
    df_final = (unioned.alias("u").join(payment_type_desc.alias("p"), coalesce(col("u.payment_type"), lit(0)) == col("p.code"), "left")
                .select(
                    sha2(concat_ws("||", col("u.vendor_id"), col("u.pickup_datetime"), col("u.pickup_location_id"), col("u.service_type")), 256).alias("trip_id"),
                    col("u.vendor_id"),
                    col("u.service_type"),
                    col("u.rate_code_id"),
                    col("u.pickup_location_id"),
                    col("u.dropoff_location_id"),
                    col("u.pickup_datetime"),
                    col("u.dropoff_datetime"),
                    col("u.store_and_fwd_flag"),
                    col("u.passenger_count"),
                    col("u.trip_distance"),
                    col("u.trip_type"),
                    col("u.fare_amount"),
                    col("u.extra"),
                    col("u.mta_tax"),
                    col("u.tip_amount"),
                    col("u.tolls_amount"),
                    col("u.ehail_fee"),
                    col("u.improvement_surcharge"),
                    col("u.total_amount"),
                    coalesce(col("u.payment_type"), lit(0)).alias("payment_type"),
                    col("p.code_description").alias("payment_type_description")
                ))
    
    # 3. Guardar resultado en Silver
    (df_final.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(config["target_table"]))

    print(f"✅ Datos de viajes intermedios guardados con éxito en: {config['target_table']}\n")