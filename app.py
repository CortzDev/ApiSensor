from flask import Flask, jsonify
from flask_cors import CORS
import hashlib
import hmac
import time
import requests
import threading
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)  # Habilita CORS para permitir requests desde cualquier dominio

# Configuración de Tuya
CLIENT_ID = "dhd4knqghttrtrx3n5vu"
ACCESS_SECRET = "d51e817b7fec4b6091b51a2cc3c323d5"
DEVICE_ID = "bf9b2ec293a9f9b528lkdl"

# Variables globales para manejo de tokens
current_token = None
token_expires_at = None
token_lock = threading.Lock()

def get_tuya_token():
    """Obtiene un nuevo token de Tuya"""
    timestamp = str(int(time.time() * 1000))
    method = "GET"
    url_path = "/v1.0/token?grant_type=1"
    body = ""
    content_sha256 = hashlib.sha256(body.encode()).hexdigest()
    string_to_sign = f"{method}\n{content_sha256}\n\n{url_path}"
    str_to_sign = CLIENT_ID + timestamp + string_to_sign
    signature = hmac.new(
        ACCESS_SECRET.encode(),
        str_to_sign.encode(),
        hashlib.sha256
    ).hexdigest().upper()

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
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and "result" in data:
                return {
                    "token": data["result"]["access_token"],
                    "expires_in": data["result"].get("expire_time", 7200)  # Default 2 horas
                }
            else:
                return {"error": f"Error en respuesta de Tuya: {data}"}
        else:
            return {"error": f"HTTP {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": f"Error al obtener token: {str(e)}"}

def ensure_valid_token():
    """Asegura que tenemos un token válido, lo renueva si es necesario"""
    global current_token, token_expires_at
    
    with token_lock:
        now = datetime.now()
        
        # Si no hay token o está por expirar (renovar 5 minutos antes)
        if not current_token or not token_expires_at or now >= (token_expires_at - timedelta(minutes=5)):
            print("🔄 Renovando token de Tuya...")
            token_result = get_tuya_token()
            
            if "error" in token_result:
                return token_result
            
            current_token = token_result["token"]
            token_expires_at = now + timedelta(seconds=token_result["expires_in"])
            print(f"✅ Token renovado. Expira: {token_expires_at.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return {"token": current_token}

def calculate_tuya_signature(access_token, method="GET", url_path="/v1.0/devices", body=""):
    """Calcula la firma HMAC para requests a Tuya"""
    timestamp = str(int(time.time() * 1000))
    nonce = ""
    content_sha256 = hashlib.sha256(body.encode()).hexdigest()
    string_to_sign = f"{method}\n{content_sha256}\n\n{url_path}"
    str_to_sign = CLIENT_ID + access_token + timestamp + nonce + string_to_sign
    signature = hmac.new(
        ACCESS_SECRET.encode(),
        str_to_sign.encode(),
        hashlib.sha256
    ).hexdigest().upper()

    return {
        "sign_method": "HMAC-SHA256",
        "client_id": CLIENT_ID,
        "t": timestamp,
        "Content-Type": "application/json",
        "access_token": access_token,
        "sign": signature
    }

def get_tuya_data():
    """Obtiene datos del dispositivo Tuya con token dinámico"""
    # Asegurar que tenemos un token válido
    token_result = ensure_valid_token()
    if "error" in token_result:
        return token_result
    
    access_token = token_result["token"]
    url_path = f"/v1.0/devices/{DEVICE_ID}/status"
    
    # Calcular headers con firma
    headers = calculate_tuya_signature(access_token, "GET", url_path)
    
    url = f"https://openapi.tuyaeu.com{url_path}"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        
        # Filtrar sensores no deseados
        if 'result' in data and data['result']:
            data['result'] = [item for item in data['result'] if item.get('code') != 'alarm_volume']
        
        return data
    except Exception as e:
        return {"error": f"Error al conectar con Tuya: {str(e)}", "success": False}

@app.route('/api/sensors', methods=['GET'])
def get_sensors():
    """Endpoint principal para obtener datos de sensores"""
    return jsonify(get_tuya_data())

@app.route('/api/sensors/formatted', methods=['GET'])
def get_sensors_formatted():
    """Endpoint que devuelve datos formateados para el frontend"""
    data = get_tuya_data()
    
    if 'error' in data:
        return jsonify(data)
    
    # Formatear datos para el frontend
    formatted_data = {
        "success": True,
        "timestamp": int(time.time()),
        "sensors": []
    }
    
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
            sensor = {
                "code": item.get('code'),
                "name": sensor_names.get(item.get('code'), item.get('code', '').replace('_', ' ').title()),
                "value": item.get('value'),
                "type": type(item.get('value')).__name__
            }
            formatted_data["sensors"].append(sensor)
    
    return jsonify(formatted_data)

@app.route('/api/token', methods=['GET'])
def get_token_info():
    """Endpoint para obtener información del token actual"""
    global current_token, token_expires_at
    
    if not current_token or not token_expires_at:
        return jsonify({
            "status": "no_token",
            "message": "No hay token activo"
        })
    
    now = datetime.now()
    is_valid = now < token_expires_at
    time_remaining = (token_expires_at - now).total_seconds() if is_valid else 0
    
    return jsonify({
        "status": "active" if is_valid else "expired",
        "expires_at": token_expires_at.isoformat(),
        "time_remaining_seconds": int(time_remaining),
        "is_valid": is_valid
    })

@app.route('/api/token/refresh', methods=['POST'])
def refresh_token():
    """Endpoint para forzar la renovación del token"""
    global current_token, token_expires_at
    
    with token_lock:
        # Forzar renovación limpiando el token actual
        current_token = None
        token_expires_at = None
    
    # Obtener nuevo token
    token_result = ensure_valid_token()
    
    if "error" in token_result:
        return jsonify({
            "success": False,
            "error": token_result["error"]
        }), 400
    
    return jsonify({
        "success": True,
        "message": "Token renovado exitosamente",
        "expires_at": token_expires_at.isoformat()
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint para verificar el estado de la API"""
    global current_token, token_expires_at
    
    token_status = "valid"
    if not current_token:
        token_status = "no_token"
    elif not token_expires_at or datetime.now() >= token_expires_at:
        token_status = "expired"
    
    return jsonify({
        "status": "healthy",
        "timestamp": int(time.time()),
        "service": "Tuya Sensors API",
        "token_status": token_status
    })

@app.route('/', methods=['GET'])
def api_info():
    """Información sobre la API"""
    return jsonify({
        "name": "Tuya Sensors API",
        "version": "2.0",
        "features": [
            "Renovación automática de tokens",
            "Manejo thread-safe de tokens",
            "Endpoints para gestión de tokens"
        ],
        "endpoints": {
            "/api/sensors": "Obtiene datos raw de sensores",
            "/api/sensors/formatted": "Obtiene datos formateados de sensores",
            "/api/token": "Información del token actual",
            "/api/token/refresh": "Renueva el token manualmente",
            "/api/health": "Estado de la API y token"
        },
        "usage": {
            "base_url": "http://localhost:5000",
            "cors": "Habilitado para todos los dominios",
            "methods": ["GET", "POST"]
        }
    })

if __name__ == "__main__":
    print("🚀 Tuya Sensors API v2.0 iniciando...")
    print("🔑 Con gestión automática de tokens")
    print("📡 Endpoints disponibles:")
    print("   • /api/sensors")
    print("   • /api/sensors/formatted")
    print("   • /api/token")
    print("   • /api/token/refresh")
    print("   • /api/health")
    
    # Obtener token inicial
    initial_token = ensure_valid_token()
    if "error" in initial_token:
        print(f"⚠️  Error al obtener token inicial: {initial_token['error']}")
    else:
        print("✅ Token inicial obtenido correctamente")
    
    # Railway detecta automáticamente el puerto desde la variable PORT
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
