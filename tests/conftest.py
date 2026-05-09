import pytest
import os
from dotenv import load_dotenv
from data.adapters.azure_adapter import AzureBlobStorageAdapter

def pytest_configure(config):
    """
    Configura pytest para cargar las variables de entorno necesarias para las pruebas.
    Esto asegura que las pruebas tengan acceso a las credenciales de Azure necesarias para interactuar con ADLS Gen2.
    Se llama automáticamente antes de ejecutar cualquier prueba y se ejecuta una unica vez al iniciar la sesin de pruebas.
    """
    load_dotenv()

@pytest.fixture(scope="session")
def azure_storage_name():
    """
    Fixture que proporciona el nombre de la cuenta de Azure Blob Storage.
    """
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", '')
    if not account_name:
        pytest.fail("❌ La variable de entorno 'AZURE_STORAGE_ACCOUNT_NAME' no está configurada.")
    return account_name

@pytest.fixture(scope="module")
def adapter(azure_storage_name):
    """
    Fixture que proporciona una instancia del adaptador de Azure Blob Storage para las pruebas.
    Esta fixture se ejecuta una vez por módulo de pruebas, lo que permite compartir la misma instancia del adaptador entre todas las pruebas dentro del mismo módulo.
    """
    return AzureBlobStorageAdapter(account_name=azure_storage_name)
