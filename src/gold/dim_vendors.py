# src/gold/dim_vendors.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

def process_gold_dim_vendors(spark: SparkSession, config: dict):
    """
    Genera la dimensión de proveedores (dim_vendors) mapeando los IDs numéricos
    a nombres comerciales legibles.
    Args:
        spark (SparkSession): La sesión de Spark activa.
        config (dict): Configuración con rutas y nombres de tablas.
    Returns:
        None: Guarda la tabla resultante directamente en el destino configurado.
    """
    print("🏆 [Gold] Procesando dimensión: dim_vendors")
    
    # 1. Leer de la tabla Silver unificada (int_trips_unioned)
    df_trips = spark.read.table(config["source_unioned"])
    df_desc = spark.read.table(config["source_payment_type_desc"])
    df_desc = df_desc.filter(col("group_code") == "003") # Filtrar solo por vendor_id
    
    # 2. Obtener IDs únicos y emular el macro get_vendor_names
    df_vendors = (df_trips
                  .select("vendor_id")
                  .distinct()
                  .filter(col("vendor_id").isNotNull()))
    
    df_desc_bcast = broadcast(df_desc)

    # 3. Mapear los IDs a nombres comerciales usando un join con la tabla de descripciones
    df_final = (df_vendors.alias("v")
                .join(df_desc_bcast.alias("d"), col("v.vendor_id") == col("d.code"), "left")
                .select(col("v.vendor_id").cast("int").alias("vendor_id"),
                        col("d.code_description").cast("string").alias("vendor_name")))
    
    # 4. Escribir la dimensión final (Overwrite para asegurar frescura)
    (df_final.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(config["target_dim_vendors"]))
    
    print(f"✅ Dimensión dim_vendors guardada con éxito en: {config['target_dim_vendors']}\n")