import os
import requests
from tqdm import tqdm
from src.common.logger import get_logger
from typing import Any

logger = get_logger(__name__)

class FileExtractor:
    """
    Clase para manejar la extracción de datos desde una URL.
    Proporciona métodos para descargar archivos y manejar errores de red.
    """
    def __init__(self, local_dir: str) -> None:
        """
        Inicializa la clase con la URL de origen de los datos.
        Arguments:
            local_dir (str): Directorio local donde se guardarán los archivos descargados.
        """
        self.local_dir = local_dir
        os.makedirs(self.local_dir, exist_ok=True)

    def download_with_progress(self, service: str, name_file: str, extension: str, period: Any = None, desc: str = "Downloading") -> str:
        """
        Descarga un archivo desde la URL especificada y lo guarda en el destino indicado.
        Arguments:
            service (str): Servicio del cual se extraerán los datos.
            name_file (str): Nombre del archivo.
            extension (str): Extensión del archivo a descargar.
            period (str): Período de los datos a extraer, format: YYYY-MM.
            desc (str): Descripción para la barra de progreso.
        """
        if period is None:
            file_name = f"{name_file}.{extension}"
        else:        
            file_name = f"{name_file}_{service.replace('-', '')}_{period}.{extension}"
        url = f"https://d37ci6vzurychx.cloudfront.net/{service}/{file_name}"
        local_path = os.path.join(self.local_dir, file_name)

        logger.info(f"Descargando desde: {url}")
        
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            # Configure tqdm for bytes
            with (
                open(local_path, "wb") as f,
                tqdm(
                    total=total,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=desc,
                ) as bar,
            ):
                for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MB
                    if not chunk:
                        continue
                    size = f.write(chunk)
                    bar.update(size)

        return local_path
    
