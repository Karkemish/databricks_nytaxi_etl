# run_gold.py
# pylint: disable=undefined-variable
# type: ignore
import json
import sys
import os
from pyspark.sql import SparkSession
from src.gold.dim_vendors import process_gold_dim_vendors
from src.gold.fct_trips import process_gold_fct_trips
from src.gold.fct_monthly_zone_revenue import process_gold_monthly_revenue

def main():
    # 1. Inicializar la sesión de Spark
    spark = SparkSession.builder.getOrCreate()

    # 2. Capturar parámetro dinámico del entorno (Widget de Databricks)
    try:
        catalog = dbutils.widgets.get("catalog")
    except Exception:
        catalog = "dev"

    print(f"🏆 [Gold Orchestrator] Iniciando capa de Negocio en el catálogo: {catalog.upper()}")

    # 3. Cargar archivo de configuración declarativo

    if "get_ipython" in globals() or "__file__" not in globals():
        script_path = os.path.abspath(sys.argv[0])
    else:
        script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)
    project_root = os.path.dirname(script_dir)
    config_path = os.path.join(project_root, "config", "gold_config.json")
    with open(config_path, "r") as f:
        config_data = json.load(f)

    # Formatear todos los nombres de tablas inyectando el catálogo real
    gold_config = config_data["gold_pipeline"]
    for key in gold_config:
        gold_config[key] = gold_config[key].format(catalog=catalog)

    # 4. Fase 1: Procesar Dimensión Maestra de Proveedores
    print("\n--- 🛒 FASE 1 GOLD: DIM_VENDORS ---")
    try:
        process_gold_dim_vendors(spark, gold_config)
    except Exception as e:
        print(f"❌ Error crítico en dim_vendors: {str(e)}")
        raise e

    # 5. Fase 2: Procesar Tabla de Hechos Incremental (Con cruces optimizados por Broadcast)
    print("\n--- 🚖 FASE 2 GOLD: FCT_TRIPS (INCREMENTAL) ---")
    try:
        process_gold_fct_trips(spark, gold_config)
    except Exception as e:
        print(f"❌ Error crítico en fct_trips: {str(e)}")
        raise e

    # 6. Fase 3: Procesar Data Mart Agregado para Negocio/Reportes
    print("\n--- 📊 FASE 3 GOLD: FCT_MONTHLY_ZONE_REVENUE ---")
    try:
        process_gold_monthly_revenue(spark, gold_config)
    except Exception as e:
        print(f"❌ Error crítico en el Data Mart mensual: {str(e)}")
        raise e

    print(f"\n🎉 [Gold Orchestrator] ¡Capa Gold procesada con éxito absoluto para el entorno '{catalog}'!")

if __name__ == "__main__":
    main()