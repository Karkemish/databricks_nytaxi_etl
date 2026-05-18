import os
import json
from dotenv import load_dotenv
from data.orchestrator.load_adls import ADLSOrchestrator
from src.common.logger import get_logger

load_dotenv()
logger = get_logger("UploadManager")

STATE_FILE = "data/tmp/ingestion_state.json"

def load_state() -> dict:
    """Carga el archivo de estado de cargas previas."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state: dict):
    """Guarda el progreso actual en el archivo de estado."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

def main():
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "")
    if not account_name:
        logger.error("Falta la variable AZURE_STORAGE_ACCOUNT_NAME.")
        return

    orchestrator = ADLSOrchestrator(account_name=account_name)
    state = load_state()

    # 1. Cargar datos maestros (Ideales para mantenerlos en el estado también)
    if not state.get("maestros_completados"):
        logger.info("Cargando catálogos maestros...")
        orchestrator.lookup_data_to_adls(name_file="taxi_zone_lookup")
        orchestrator.data_entry_to_adls(name_file="description_taxi_trip")
        state["maestros_completados"] = True
        save_state(state)

    # 2. Carga masiva con Checkpoint
    services = ["yellow", "green"]
    years = ["2024", "2025"]
    months = [f"{m:02d}" for m in range(1, 13)]

    for service in services:
        for year in years:
            for month in months:
                period = f"{year}-{month}"
                state_key = f"{service}_{period}"
                
                # 🔍 COMPROBACIÓN: Si ya se subió con éxito, pasamos al siguiente
                if state.get(state_key) == "success":
                    logger.info(f"⏩ Omitiendo {service} para {period} (Ya fue procesado con éxito).")
                    continue
                
                logger.info(f"🚀 Procesando: {service} -> {period}")
                try:
                    orchestrator.trip_record_to_adls(name_file=service, period=period)
                    
                    # 💾 GUARDAR PROGRESO: Si no explotó, marcamos como exitoso
                    state[state_key] = "success"
                    save_state(state)
                    
                except Exception as e:
                    logger.error(f"❌ Error crítico en {service} para {period}: {e}")
                    # Guardamos el fallo para saber exactamente qué falló en los logs
                    state[state_key] = f"failed: {str(e)}"
                    save_state(state)
                    
                    # Opcional: Detener la ejecución completa o continuar con el siguiente mes
                    # Si queremos que se detenga el script de golpe, descomenta la siguiente línea o comentarla si quieres que siga intentando con los siguientes meses:
                    return 

if __name__ == "__main__":
    main()