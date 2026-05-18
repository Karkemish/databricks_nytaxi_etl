import os
from src.common.logger import get_logger
from data.adapters.azure_adapter import AzureBlobStorageAdapter
from data.processed.file_extractor import FileExtractor
from data.processed.local_transformer import LocalTransformer

logger = get_logger("LoadADLSOrchestrator")

class ADLSOrchestrator:
    """
    Orquestador para cargar datos en ADLS Gen2.
    Coordina la extracción, transformación y carga de datos utilizando los adaptadores y transformadores definidos.
    Proporciona métodos específicos para diferentes tipos de datos (trip records, lookup data, data entry).
    """
    def __init__(self, account_name: str, temp_dir: str = "data/temp") -> None:
        self.temp_dir = temp_dir
        # Instanciamos los adaptadores una sola vez para reutilizar las sesiones/conexiones
        self.extractor = FileExtractor(local_dir=self.temp_dir)
        self.azure = AzureBlobStorageAdapter(account_name=account_name)
        self.container = "landing"  # Contenedor de destino en ADLS

    def cleanup(self, file_path: str) -> None:
        """
        Elimina un archivo local si existe.
        Arguments:
            file_path (str): Ruta del archivo a eliminar.
        """
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"🧹 Archivo temporal eliminado: {file_path}")
    
    def trip_record_to_adls(self, name_file: str, period: str) -> None:
        """
        Orquesta la descarga de un archivo de trip record, su transformación y carga en ADLS.
        Arguments:
            name_file (str): Nombre del archivo de trip record.
            period (str): Período de los datos a extraer.
        """
        local_path = self.extractor.download_with_progress(
            service="trip-data", 
            extension="parquet",
            name_file=name_file,
            period=period,
            desc=f"Descargando {name_file} {period}"
        )
        year = period.split("-")[0]
        blob_name = f"{name_file}_taxi_trip/{year}/{name_file}_tripdata_{period}.parquet"
        try:
            self.azure.upload_file_with_progress(
                container_name=self.container, 
                blob_name=blob_name, 
                file_path=local_path
            )
        finally:
            self.cleanup(local_path)
        
    def lookup_data_to_adls(self, name_file: str) -> None:
        """
        Orquesta la descarga de un archivo de lookup, su transformación a JSON y carga en ADLS.
        Arguments:
            name_file (str): Nombre del archivo de lookup.
        """
        local_path = self.extractor.download_with_progress(
            service="misc", 
            extension="csv",
            name_file=name_file,
            desc=f"Descargando {name_file}"
        )
        local_path_json = local_path.replace(".csv", ".json")
        try:
            LocalTransformer.csv_to_json_custom(input_csv=local_path, output_json=local_path_json)
            blob_name = f"zone_taxi_trip/{name_file}.json"
            self.azure.upload_file_with_progress(
                container_name=self.container, 
                blob_name=blob_name, 
                file_path=local_path_json
            )
        finally:
            self.cleanup(local_path)
            self.cleanup(local_path_json)

    def data_entry_to_adls(self, name_file: str = "description_taxi_trip") -> None:
        """
        Orquesta la creación de un archivo de catálogo de descripciones estáticas y su carga en ADLS.
        Arguments:
            name_file (str): Nombre del archivo de entrada de datos.
        """
        local_path = f"{self.temp_dir}/{name_file}.csv"
        try:
            LocalTransformer.create_data_entry(output_csv=local_path)
            blob_name = f"description_taxi_trip/{name_file}.csv"
            self.azure.upload_file_with_progress(
                container_name=self.container, 
                blob_name=blob_name, 
                file_path=local_path
            )
        finally:
            self.cleanup(local_path)


    

    
