import pytest
import os

def test_azure_auth_connectity(adapter):
    """
    NIVEL1: Prueba de conectividad y autenticación con Azure Blob Storage.
    Verifica que el adaptador pueda autenticarse correctamente utilizando las credenciales proporcionadas.
    Si la autenticación falla, se lanzará una excepción que hará que la prueba falle.
    """
    try:
        # Intentar listar los contenedores para verificar la autenticación
        containers = list(adapter.blob_service_client.list_containers())
        assert len(containers) >= 0  # Si se puede listar, la autenticación es exitosa
    except Exception as e:
        pytest.fail(f"❌ Fallo en la conexión de Azure: {e}")

def test_full_upload_cycle(adapter):
    """
    NIVEL2: Prueba completa del ciclo de subida a Azure Blob Storage.
    Esta prueba verifica que un archivo se pueda subir correctamente a un contenedor específico y luego se pueda eliminar.
    Se crea un archivo temporal, se sube al contenedor, se verifica su existencia y finalmente se elimina para limpiar el entorno de pruebas.
    """
    container_name = "landing"
    blob_name = "tests/integration_test_file.txt"
    file_path = "data/temp/temp_test_file.txt"

    # Crear un archivo temporal para subir
    with open(file_path, "w") as f:
        f.write("Este es un archivo de prueba para Azure Blob Storage.")

    try:
        # Subir el archivo al contenedor
        adapter.upload_file_with_progress(container_name, blob_name, file_path)

        # Verificar que el blob existe después de la subida
        blob_client = adapter.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        assert blob_client.exists() is True
    finally:
        # Eliminar el archivo temporal creado para la prueba
        if os.path.exists(file_path):
            os.remove(file_path)