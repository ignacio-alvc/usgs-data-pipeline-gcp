import os
import requests
import json
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

GCP_CREDENTIALS_PATH = os.path.join(SCRIPT_DIR, "../.secrets/portfolio-earthquake-analysis-6ff1f9179260.json")

GCS_BUCKET_NAME = "bkt-earthquakes-raw-ia"

# Endpoint de la API USGS para obtener datos de sismos de los últimos 30 días.
API_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_month.geojson"

BQ_PROJECT_ID = "portfolio-earthquake-analysis"
BQ_DATASET_ID = "earthquakes_dw"
BQ_TABLE_ID = "raw_usgs_earthquakes"

# Extrae datos de la API USGS
def extract_data(url: str) -> dict:
    print("Iniciando extracción de datos desde: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        print("Extracción exitosa.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error durante la extracción de datos: {e}")
        return None
    
# Carga los datos extraídos en formato JSON a Google Cloud Storage
def load_to_gcs(data: dict, bucket_name: str, credentials_path: str):
    if data is None:
        print("No hay datos para cargar. Abortando operación de carga.")
        return
    print(f"Iniciando carga a GCS bucket: {bucket_name}")
    try:
        today = datetime.now()
        destination_blob_name = f"raw_data/{today.year}/{today.month:02d}/{today.day:02d}/usgs_earthquakes.json"
        storage_client = storage.Client.from_service_account_json(credentials_path)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(
            data=json.dumps(data),
            content_type="application/json"
        )

        print(f"¡Éxito! Datos cargados a: gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"Error durante la carga a GCS: {e}")

# Carga los datos en big query
def load_to_bigquery(data: dict, credentials_path: str):
    if data is None:
        print("No hay datos para cargar a BigQuery. Abortando operación de carga.")
        return
    features_data = data.get('features')
    if not features_data:
        print("Error: No se encontró el array 'features' en los datos extraídos.")
        return
    
    load_timestamp = datetime.now(timezone.utc)
    processed_features = []
    for feature in features_data:
        record = feature.copy()
        record['_load_timestamp'] = load_timestamp
        processed_features.append(record)
    print(f"Iniciando carga a BigQuery en la tabla: {BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}")

    try:
        bq_client = bigquery.Client.from_service_account_json(credentials_path)
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        job = bq_client.load_table_from_json(
            processed_features,
            table_id,
            job_config=job_config
        )
        job.result()
        print(f"¡Éxito! Datos cargados a BigQuery en la tabla: {table_id}")
    except Exception as e:
        print(f"Error durante la carga a BigQuery: {e}")


# Punto de entrada del script
if __name__ == "__main__":
    print("Iniciando pipeline de extracción y carga (EL)...")

    # Extrae
    raw_data = extract_data(API_URL)

    # Carga
    load_to_gcs(
        data=raw_data,
        bucket_name=GCS_BUCKET_NAME,
        credentials_path=GCP_CREDENTIALS_PATH
    )

    load_to_bigquery(
        data=raw_data,
        credentials_path=GCP_CREDENTIALS_PATH
    )

    print("Pipeline (EXTRACT-LOAD) completado.")


