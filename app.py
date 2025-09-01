from flask import Flask, jsonify
from flask_cors import CORS
import hashlib
import hmac
import time
import requests
import threading
from datetime import datetime, timedelta
import os

app = Flask(__name__)
CORS(app)

# Configuración Tuya
CLIENT_ID = "dhd4knqghttrtrx3n5vu"
ACCESS_SECRET = "d51e817b7fec4b6091b51a2cc3c323d5"
DEVICE_ID = "bf9b2ec293a9f9b528lkdl"

# Tokens
current_token = None
token_expires_at = None
token_lock = threading.Lock()

# Cache de sensores
last_sensor_data = None
last_sensor_fetch = None
CACHE_TTL = 3  # segundos

# -------------------
# Funciones Tuya
# -------------------
def get_tuya_token():
    timestamp = str(int(time.time() * 1000))
    method = "GET"
    url_path = "/v1.0/token?grant_type=1"
    body = ""
    content_sha256 = hashlib.sha256(body.encode()).hexdigest()
    string_to_sign = f"{method}\n{content_sha256}\n\n{url_path}"
    str_to_sign = CLIENT_ID + timestamp + string_to_sign
    signature = hmac.new(ACCESS_SECRET.encode(), str_to_sign.encode(), hashlib.sha256).hexdigest().upper()

    url = f"https://openapi.tuyaeu.com{url_path}"
    headers = {
        "client_id": CLIENT_ID,
        "sign": signature,
        "t": timestamp,
        "sign_method": "HMAC-SHA256",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        if data.get("success") and "result" in data:
            return {
                "token": data["result"]["access_token"],
                "expires_in": data["result"].get("expire_time", 7200)
            }
        else:
            return {"error": f"Error en respuesta Tuya: {data}"}
    except Exception as e:
        return {"error": f"Error al obtener token: {str(e)}"}

def ensure_valid_token():
    global current_token, token_expires_at
    with token_lock:
        now = datetime.now()
        if not current_token or not token_expires_at or now >= (token_expires_at - timedelta(minutes=5)):
            token_result = get_tuya_token()
            if "error" in token_result:
                return token_result
            current_token = token_result["token"]
            token_expires_at = now + timedelta(seconds=token_result["expires_in"])
        return {"token": current_token}

def calculate_tuya_signature(access_token, method="GET", url_path="/v1.0/devices", body=""):
    timestamp = str(int(time.time() * 1000))
    content_sha256 = hashlib.sha256(body.encode()).hexdigest()
    string_to_sign = f"{method}\n{content_sha256}\n\n{url_path}"
    str_to_sign = CLIENT_ID + access_token + timestamp + string_to_sign
    signature = hmac.new(ACCESS_SECRET.encode(), str_to_sign.encode(), hashlib.sha256).hexdigest().upper()
    return {
        "sign_method": "HMAC-SHA256",
        "client_id": CLIENT_ID,
        "t": timestamp,
        "Content-Type": "application/json",
        "access_token": access_token,
        "sign": signature
    }

def get_tuya_data():
    token_result = ensure_valid_token()
    if "error" in token_result:
        return token_result

    access_token = token_result["token"]
    url_path = f"/v1.0/devices/{DEVICE_ID}/status"
    headers = calculate_tuya_signature(access_token, "GET", url_path)
    url = f"https://openapi.tuyaeu.com{url_path}"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        if 'result' in data and data['result']:
            data['result'] = [item for item in data['result'] if item.get('code') != 'alarm_volume']
        return data
    except Exception as e:
        return {"error": f"Error al conectar con Tuya: {str(e)}", "success": False}

# -------------------
# Función de cache
# -------------------
def get_sensors_cached():
    global last_sensor_data, last_sensor_fetch
    now = datetime.now()
    if last_sensor_data and last_sensor_fetch and (now - last_sensor_fetch).total_seconds() < CACHE_TTL:
        return last_sensor_data
    data = get_tuya_data()
    last_sensor_data = data
    last_sensor_fetch = now
    return data

# -------------------
# Endpoints
# -------------------
@app.route('/api/sensors', methods=['GET'])
def api_sensors():
    return jsonify(get_sensors_cached())

@app.route('/api/sensors/formatted', methods=['GET'])
def api_sensors_formatted():
    data = get_sensors_cached()
    if 'error' in data:
        return jsonify(data)
    formatted_data = {"success": True, "timestamp": int(time.time()), "sensors": []}
    sensor_names = {
        'air_quality_index': 'Calidad del Aire',
        'temp_current': 'Temperatura',
        'humidity_value': 'Humedad',
        'co2_value': 'CO₂',
        'ch2o_value': 'Formaldehído',
        'pm25_value': 'PM2.5',
        'pm1': 'PM1.0',
        'pm10': 'PM10',
        'battery_percentage': 'Batería',
        'charge_state': 'Estado de Carga'
    }
    if 'result' in data and data['result']:
        for item in data['result']:
            formatted_data["sensors"].append({
                "code": item.get('code'),
                "name": sensor_names.get(item.get('code'), item.get('code', '').replace('_', ' ').title()),
                "value": item.get('value'),
                "type": type(item.get('value')).__name__
            })
    return jsonify(formatted_data)

@app.route('/api/health', methods=['GET'])
def health_check():
    token_status = "valid" if current_token and token_expires_at and datetime.now() < token_expires_at else "expired"
    return jsonify({
        "status": "healthy",
        "timestamp": int(time.time()),
        "service": "Tuya Sensors API",
        "token_status": token_status
    })

# -------------------
# Inicio
# -------------------
if __name__ == "__main__":
    print("🚀 Tuya Sensors API iniciando...")
    initial_token = ensure_valid_token()
    if "error" in initial_token:
        print(f"⚠️  Error al obtener token inicial: {initial_token['error']}")
    else:
        print("✅ Token inicial obtenido")
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
