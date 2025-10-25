import os
import joblib
import pandas as pd
import functions_framework
from google.cloud import storage

BUCKET_NAME = "bkt-earthquake-models-ia"
SCALER_PATH = "earthquake_scaler.joblib"
MODEL_PATH = "earthquake_xgboost_model.joblib"

scaler = None
model = None

def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
    blob.download_to_filename(destination_file_name)
    print(f"Blob {source_blob_name} descargado a {destination_file_name}.")

def load_models():
    global scaler, model

    local_scaler_path = f"/tmp/{SCALER_PATH}"
    local_model_path = f"/tmp/{MODEL_PATH}"

    if not os.path.exists(local_scaler_path):
        download_blob(BUCKET_NAME, SCALER_PATH, local_scaler_path)
    if not os.path.exists(local_model_path):
        download_blob(BUCKET_NAME, MODEL_PATH, local_model_path)

    scaler = joblib.load(local_scaler_path)
    model = joblib.load(local_model_path)
    print("Scaler y modelo cargados.")

@functions_framework.http
def predict_magnitude(request):
    global scaler, model

    if scaler is None or model is None:
        load_models()
    if request.method != 'POST':
        return 'Metodo no permitido', 405
    
    request_json = request.get_json(silent=True)
    if not request_json:
        return 'Request JSON invalido o vacío', 400
    
    try:
        latitude = float(request_json['latitude'])
        longitude = float(request_json['longitude'])
        depth_km = float(request_json['depth_km'])
    except (KeyError, TypeError, ValueError) as e:
        return f'Datos de entrada inválidos o faltantas (latitude, longitude, depth_km): {e}', 400
    
    input_data = pd.DataFrame([[latitude, longitude, depth_km]],
                              columns=['latitude', 'longitude', 'depth_km'])
    
    input_data_scaled = scaler.transform(input_data)

    prediction = model.predict(input_data_scaled)

    response = {"predicted_magnitude": float(prediction[0])}
    return response, 200