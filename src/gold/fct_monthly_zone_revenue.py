# src/gold/fct_monthly_zone_revenue.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, date_trunc, sum, count, avg, current_timestamp

def process_gold_monthly_revenue(spark: SparkSession, config: dict):
    """
    Genera el data mart fct_monthly_zone_revenue acumulando métricas operacionales
    y financieras agrupadas por zona, mes y tipo de servicio.
    """
    print("🏆 [Gold] Procesando Data Mart: fct_monthly_zone_revenue")
    
    # 1. Leer de tu recien procesada fct_trips en Gold
    df_fct_trips = spark.read.table(config["target_fct_trips"])
    
    # 2. Aplicar Transformaciones y Agregaciones del Modelo dbt
    df_mart = (df_fct_trips
        .withColumn("pickup_zone", coalesce(col("pickup_zone"), lit("Unknown Zone")))
        .withColumn("revenue_month", date_trunc("month", col("pickup_datetime")).cast("date"))
        .groupBy("pickup_zone", "revenue_month", "service_type")
        .agg(
            sum("total_amount").alias("revenue_monthly_total_amount"),
            count("trip_id").alias("total_monthly_trips")
        )
        .withColumn("calculated_at", current_timestamp())
    )
    
    # 3. Escribir el Data Mart de Reportes
    (df_mart.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(config["target_monthly_revenue"]))
    
    print(f"✅ Data Mart fct_monthly_zone_revenue guardado exitosamente en: {config['target_monthly_revenue']}\n")