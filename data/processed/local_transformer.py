import pandas as pd
import json

class LocalTransformer:
    """
    Clase para manejar la transformación de datos en archivos locales.
    Proporciona métodos para leer, transformar y guardar archivos en el formato elegido.
    """
    @staticmethod
    def csv_to_json_custom(input_csv: str, output_json: str) -> None:
        """
        Lee un archivo CSV, lo transforma a formato JSON y lo guarda en el destino indicado.
        Arguments:
            input_csv (str): Ruta del archivo CSV de entrada.
            output_json (str): Ruta del archivo JSON de salida.
        """
        df = pd.read_csv(input_csv)
        records = [
            {
                "location_id": int(row["LocationID"]),
                "location": {
                    "borough": row["Borough"],
                    "zone": row["Zone"],
                    "service_zone": row["service_zone"]
                }
            } for _, row in df.iterrows()
        ]
        with open(output_json, "w") as f:
            json.dump(records, f)

    @staticmethod
    def create_data_entry(output_csv: str) -> None:
        """
        Crea una entrada de datos en formato CSV.
        Arguments:
            output_csv (str): Ruta del archivo CSV de salida.
        """
        columns=["id", "group_code", "group_description", "code", "description"]
        values = [
            [1, "001001", "rate_code_id", 1, "Standard rate"],
            [2, "001002", "rate_code_id", 2, "JFK"],
            [3, "001003", "rate_code_id", 3, "Newark"],
            [4, "001004", "rate_code_id", 4, "Nassau or Westchester"],
            [5, "001005", "rate_code_id", 5, "Negotiated fare"],
            [6, "001006", "rate_code_id", 6, "Group ride"],
            [7, "001007", "rate_code_id", 99, "Null/unknown"],
            [8, "002001", "payment_type", 0, "Flex Fare trip"],
            [9, "002002", "payment_type", 1, "Credit card"],
            [10, "002003", "payment_type", 2, "Cash"],
            [11, "002004", "payment_type", 3, "No charge"],
            [12, "002005", "payment_type", 4, "Dispute"],
            [13, "002006", "payment_type", 5, "Unknown"],
            [14, "002007", "payment_type", 6, "Voided trip"],
            [15, "003001", "vendor_id", 1, "Creative Mobile Technologies, LLC"],
            [16, "003002", "vendor_id", 2, "Curb Mobility, LLC"],
            [17, "003003", "vendor_id", 6, "Myle Technologies Inc"],
            [18, "003004", "vendor_id", 7, "Helix"]
        ]
        df = pd.DataFrame(columns=columns, data=values)
        df.to_csv(output_csv, index=False, sep=",")