# run_silver.py
# pylint: disable=undefined-variable
# type: ignore
import json
from pyspark.sql import SparkSession
from src.silver.dimensions import process_dimensions_zone, process_dimensions_description
from src.silver.trips_unioned import process_silver_trips_unioned
from src.silver.int_trips import process_silver_int_trips

def main():
    # 1. Inicializar la sesión de Spark
    spark = SparkSession.builder.getOrCreate()

    # 2. Capturar parámetros dinámicas de Databricks Widgets
    try:
        catalog = dbutils.widgets.get("catalog")
    except Exception:
        catalog = "dev"

    print(f"🚀 [Silver Orchestrator] Iniciando procesamiento en el catálogo: {catalog.upper()}")

    # 3. Cargar archivo de configuración declarativo
    with open("config/silver_config.json", "r") as f:
        config_data = json.load(f)

    # 4. Fase 1: Procesar Dimensiones / Tablas Maestras
    print("\n--- 🏗️ FASE 1: PROCESAMIENTO DE DIMENSIONES ---")
    for dim in config_data["dimensions"]:
        dim["source_table"] = dim["source_table"].format(catalog=catalog)
        dim["target_table"] = dim["target_table"].format(catalog=catalog)
        
        try:
            if dim["type"] == "zone":
                process_dimensions_zone(spark, dim)
            elif dim["type"] == "description":
                process_dimensions_description(spark, dim)
            else:
                print(f"⚠️ Tipo de dimensión desconocido: {dim['type']} para {dim['name']}")
        except Exception as e:
            print(f"❌ Error procesando dimensión {dim['name']}: {str(e)}")
            raise e

    # 5. Fase 2: Procesar Hechos (Unificación de Viajes)
    print("\n--- 🚖 FASE 2: UNIFICACIÓN DE VIAJES (YELLOW & GREEN) ---")
    trips_config = config_data["trips_pipeline"]
    
    # Formatear todas las tablas del pipeline de viajes con el catálogo correspondiente
    for key in trips_config:
        trips_config[key] = trips_config[key].format(catalog=catalog)

    try:
        # Ejecutar la alineación y unión base
        process_silver_trips_unioned(spark, trips_config)
    except Exception as e:
        print(f"❌ Error crítico en la unión de viajes: {str(e)}")
        raise e

    # 6. Fase 3: Procesar Viajes Intermedios (Enriquecimiento Final)
    print("\n--- 💎 FASE 3: ENRIQUECIMIENTO Y CLAVES DE NEGOCIO ---")
    try:
        # Ejecutar el join final y generación de llaves subrogadas (trip_id)
        process_silver_int_trips(spark, trips_config)
    except Exception as e:
        print(f"❌ Error crítico en el enriquecimiento de viajes intermedios: {str(e)}")
        raise e

    print(f"🎉 [Silver Orchestrator] Capa Silver completada con éxito para el catálogo '{catalog}'.")

if __name__ == "__main__":
    main()