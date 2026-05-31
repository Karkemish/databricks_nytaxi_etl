# run_bronze.py
# pylint: disable=undefined-variable
# type: ignore
import json
import sys
import os
from pyspark.sql import SparkSession
from src.bronze.trips import ingest_taxi_trips
from src.bronze.catalogs import ingest_json_zones, ingest_csv_description

def main():
    spark = SparkSession.builder.getOrCreate()

    try:
        env = dbutils.widgets.get("env")
        catalog = dbutils.widgets.get("catalog")
    except Exception:
        env = "dev"
        catalog = "dev"

    storage_mapping = {
        "dev": "databricksadlsproject01",
    }
    storage_account = storage_mapping.get(env, "")

    if "get_ipython" in globals() or "__file__" not in globals():
        script_path = os.path.abspath(sys.argv[0])
    else:
        script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)
    project_root = os.path.dirname(script_dir)
    config_path = os.path.join(project_root, "config", "bronze_config.json")
    with open(config_path, "r") as f:
        config_data = json.load(f)

    for pipe in config_data["pipelines"]:
        pipe["source_path"] = pipe["source_path"].format(storage_account=storage_account)
        pipe["checkpoint_path"] = pipe["checkpoint_path"].format(storage_account=storage_account)
        pipe["target_table"] = pipe["target_table"].format(catalog=catalog)
        
        pipeline_type = pipe.get("type")
        
        try:
            if pipeline_type == "trips":
                ingest_taxi_trips(spark, pipe)
            elif pipeline_type == "catalog_json":
                ingest_json_zones(spark, pipe)
            elif pipeline_type == "catalog_csv":
                ingest_csv_description(spark, pipe)
            else:
                print(f"⚠️ Tipo de pipeline desconocido para: {pipe['name']}")
        except Exception as e:
            print(f"❌ Error crítico en el pipeline {pipe['name']}: {str(e)}")
            continue

if __name__ == "__main__":
    main()