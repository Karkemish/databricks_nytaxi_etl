import pytest
import pandas as pd
import json
import os
from data.processed.local_transformer import LocalTransformer

def test_create_data_entry(tmp_path):
    """
    Prueba para verificar que el método create_data_entry crea un archivo CSV con la estructura y datos esperados.
    Se utiliza tmp_path para crear un archivo temporal durante la prueba, asegurando que no se afecte el sistema de archivos real.
    Se verifica que el archivo CSV se crea correctamente y contiene las columnas y filas esperadas.
    """
    output_path = tmp_path / "test_entry.csv"
    LocalTransformer.create_data_entry(str(output_path))

    assert os.path.exists(output_path), "El archivo CSV no fue creado."
    
    df = pd.read_csv(output_path)
    expected_columns = ["id", "group_code", "group_description", "code", "description"]
    assert list(df.columns) == expected_columns, f"Las columnas del CSV no coinciden con las esperadas. Se esperaban {expected_columns} pero se encontraron {list(df.columns)}."
    
    expected_rows = 14
    assert len(df) == expected_rows, f"El número de filas en el CSV no coincide con lo esperado. Se esperaban {expected_rows} filas pero se encontraron {len(df)}."

def test_csv_to_json_custom(tmp_path):
    """
    Prueba para verificar que el método csv_to_json_custom transforma un archivo CSV a JSON con la estructura esperada.
    Se utiliza tmp_path para crear archivos temporales durante la prueba, asegurando que no se afecte el sistema de archivos real.
    Se verifica que el archivo JSON se crea correctamente y contiene la estructura de datos esperada.
    """
    # Crear un archivo CSV de prueba
    input_csv = tmp_path / "input.csv"
    df = pd.DataFrame({
        "locationid": [1],
        "borough": ["Manhattan"],
        "zone": ["Central Park"],
        "service_zone": ["Yellow Zone"]
    })
    df.to_csv(input_csv, index=False)

    # Definir la ruta del archivo JSON de salida
    output_json = tmp_path / "output.json"

    # Transformar el CSV a JSON
    LocalTransformer.csv_to_json_custom(str(input_csv), str(output_json))

    # Verificar que el archivo JSON se creó correctamente
    assert os.path.exists(output_json), "El archivo JSON no fue creado."

    # Cargar el contenido del JSON y verificar su estructura
    with open(output_json, "r") as f:
        data = json.load(f)
    
    assert data[0]["location_id"] == 1
    assert data[0]["location"]["borough"] == "Manhattan"
    assert "zone" in data[0]["location"]
    assert data[0]["location"]["zone"] == "Central Park"