import pytest
from unittest.mock import patch, MagicMock
from data.processed.file_extractor import FileExtractor

@pytest.fixture
def extractor(tmp_path):
    """
    Fixture para crear una instancia de FileExtractor con un directorio temporal.
    tmp_path es una fixture de pytest que crea una carpeta temporal para el testing, 
    asegurando que los archivos descargados durante las pruebas no afecten el sistema de archivos real y se limpien automáticamente después de las pruebas.
    """
    return FileExtractor(local_dir=tmp_path)

def test_download_url_construction_with_period(extractor):
    """
    Prueba para verificar que la URL de descarga se construye correctamente con periodo.
    Se utiliza patch para simular la función requests.get y evitar realizar una descarga real.
    Se verifica que la URL generada coincide con el formato esperado.
    """
    with patch("requests.get") as mock_get:
        # Simulamos una respuesta exitosa
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "100"}
        mock_response.__enter__.return_value = mock_response
        mock_get.return_value = mock_response
        
        service = "trip-data"
        name_file = "yellow"
        extension = "parquet"
        period = "2024-01"
        
        extractor.download_with_progress(service, name_file, extension, period)
        
        expected_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        mock_get.assert_called_with(expected_url, stream=True)

def test_download_url_construction_without_period(extractor):
    """
    Prueba para verificar que la URL de descarga se construye correctamente cuando no se proporciona un periodo.
    Se utiliza patch para simular la función requests.get y evitar realizar una descarga real.
    Se verifica que la URL generada coincide con el formato esperado sin el periodo.
    """
    with patch("requests.get") as mock_get:
        # Simulamos una respuesta exitosa
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "100"}
        mock_response.__enter__.return_value = mock_response
        mock_get.return_value = mock_response
        
        service = "misc"
        name_file = "taxi_zone_lookup"
        extension = "csv"
        
        extractor.download_with_progress(service, name_file, extension)
        
        expected_url = f"https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        mock_get.assert_called_with(expected_url, stream=True)