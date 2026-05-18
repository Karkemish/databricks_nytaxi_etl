import pytest
from unittest.mock import MagicMock, patch
from data.orchestrator.load_adls import ADLSOrchestrator

@pytest.fixture
def mock_orchestrator():
    with patch('data.orchestrator.load_adls.FileExtractor') as mock_extractor_cls, \
         patch('data.orchestrator.load_adls.AzureBlobStorageAdapter') as mock_azure_cls:
        
        # 1. Forzamos el tipado para que Pylance sepa qué son
        extractor_mock: MagicMock = mock_extractor_cls.return_value
        azure_mock: MagicMock = mock_azure_cls.return_value
        
        # 2. Configuramos el comportamiento del mock de la instancia
        extractor_mock.download_with_progress.return_value = "data/temp/mock_file.parquet"
        
        orchestrator = ADLSOrchestrator(account_name="test_account")
        
        # Guardamos las referencias para los asserts en los tests
        orchestrator.extractor = extractor_mock
        orchestrator.azure = azure_mock
        
        yield orchestrator

def test_trip_record_workflow(mock_orchestrator):
    # Ejecutamos el método que queremos probar
    mock_orchestrator.trip_record_to_adls(name_file="yellow", period="2024-01")
    
    # Verificamos que se haya llamado al método de descarga con los parámetros correctos
    mock_orchestrator.extractor.download_with_progress.assert_called_once_with(
        service="trip-data",
        extension="parquet",
        name_file="yellow",
        period="2024-01",
        desc="Descargando yellow 2024-01"
    )
    
    expected_blob = "yellow_taxi_trip/2024/yellow_tripdata_2024-01.parquet"

    # Verificamos que se haya llamado al método de subida con los parámetros correctos
    mock_orchestrator.azure.upload_file_with_progress.assert_called_once_with(
        container_name="landing",
        blob_name=expected_blob,
        file_path="data/temp/mock_file.parquet"
    )