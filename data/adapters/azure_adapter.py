import os
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from tqdm import tqdm
from azure.core.exceptions import ResourceExistsError
from dotenv import load_dotenv

load_dotenv()

class AzureBlobStorageAdapter:
    """
    Adaptador para conectar con ADLS Gen2.
    Utiliza autenticación con Service Principal y ofrece funcionalidades avanzadas como barra de progreso y manejo de errores.
    """
    def __init__(self, account_name: str) -> None:
        """
        Inicializa el adaptador con la cuenta de almacenamiento y las credenciales necesarias.
        Arguments:
            account_name (str): Nombre de la cuenta de almacenamiento en Azure.
        """
        self.account_url = f"https://{account_name}.blob.core.windows.net"
        
        # Autenticación usando el Service Principal definido en variables de entorno
        self.credential = ClientSecretCredential(
            tenant_id=os.environ.get("AZURE_TENANT_ID", ''),
            client_id=os.environ.get("AZURE_CLIENT_ID", ''),
            client_secret=os.environ.get("AZURE_CLIENT_SECRET", '')
        )
        
        self.blob_service_client = BlobServiceClient(
            account_url=self.account_url, 
            credential=self.credential
        )

    def upload_file_with_progress(self, container_name: str, blob_name: str, file_path: str) -> None:
        """
        Sube un archivo a ADLS Gen2 con barra de progreso y 
        verificación de existencia previa.
        """
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )

        # Verificar si el blob ya existe para evitar sobrescribir sin querer
        try:
            blob_client.get_blob_properties()
            print(f"El blob '{blob_name}' ya existe en el contenedor '{container_name}'.")
            return
        except ResourceExistsError:
            pass  # El blob no existe, proceder con la subida   

        file_size = os.path.getsize(file_path)

        # Subir el archivo con barra de progreso
        with open(file_path, "rb") as f:
            with tqdm.wrapattr(
                f, 
                "read", 
                total=file_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"Subiendo {os.path.basename(file_path)}",
            ) as wrapped_file:
                # Azure subirá el archivo en bloques automáticamente, por lo que no es necesario dividirlo manualmente.
                blob_client.upload_blob(
                    wrapped_file,  # type: ignore
                    overwrite=True,
                    max_concurrency=2  
                )
        print(f"✅ Cargado en Azure: {container_name}/{blob_name}")