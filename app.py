
from flask import Flask, jsonify, request
from flask_cors import CORS
import hashlib
import hmac
import time
import requests
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
import psycopg2
import psycopg2.extras
import json
import traceback
import pandas as pd

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

# -------------------------
# CONFIG: IDs de Dispositivos
# -------------------------
ID_CARA_SUCIA = os.getenv("TUYA_DEVICE_ID", "bf9b2ec293a9f9b528lkdl")
ID_NAHUIZALCO = "bfbb9424274a58f7c805lh"
ID_JUAYUA     = "bfc04053ebf458efd9dil7"

# Mapeo para facilitar la lectura en la API
SENSORS_MAP = {
    ID_CARA_SUCIA: "Cara Sucia (Estudio Principal)",
    ID_NAHUIZALCO: "Nahuizalco (Tiempo Real)",
    ID_JUAYUA:     "Juayúa (Tiempo Real)"
}

# -------------------------
# CONFIG: Tuya Auth
# -------------------------
CLIENT_ID = os.getenv("TUYA_CLIENT_ID", "dhd4knqghttrtrx3n5vu")
ACCESS_SECRET = os.getenv("TUYA_ACCESS_SECRET", "d51e817b7fec4b6091b51a2cc3c323d5")

# -------------------------
# CONFIG: Database (PostgreSQL)
# -------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:VaLqxGBzdzZmBTddchzzryKgNeQmoPfI@switchback.proxy.rlwy.net:14573/railway?sslmode=require"
)

# -------------------------
# TOKEN MANAGEMENT (Tuya)
# -------------------------
current_token = None
token_expires_at = None
token_lock = threading.Lock()

def get_tuya_token():
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
        data = response.json() if response.text else {}
        if response.status_code == 200 and data.get("success"):
            return {
                "token": data["result"]["access_token"],
                "expires_in": data["result"].get("expire_time", 7200)
            }
        return {"error": f"Error Tuya: {data}"}
    except Exception as e:
        return {"error": str(e)}

def ensure_valid_token():
    global current_token, token_expires_at
    with token_lock:
        now = datetime.now(timezone.utc)
        if not current_token or not token_expires_at or now >= (token_expires_at - timedelta(minutes=5)):
            print("🔄 Renovando token Tuya...")
            token_result = get_tuya_token()
            if "error" in token_result:
                return token_result
            current_token = token_result["token"]
            token_expires_at = now + timedelta(seconds=token_result["expires_in"])
            print(f"✅ Token renovado. Expira: {token_expires_at.isoformat()}")
        return {"token": current_token}

def calculate_tuya_signature(access_token, method, url_path, body=""):
    timestamp = str(int(time.time() * 1000))
    content_sha256 = hashlib.sha256(body.encode()).hexdigest()
    string_to_sign = f"{method}\n{content_sha256}\n\n{url_path}"
    str_to_sign = CLIENT_ID + access_token + timestamp + "" + string_to_sign
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

# -------------------------
# FUNCIÓN CORE: Obtener datos de CUALQUIER sensor
# -------------------------
def get_tuya_data(device_id):
    token_result = ensure_valid_token()
    if "error" in token_result: return token_result
    
    access_token = token_result["token"]
    url_path = f"/v1.0/devices/{device_id}/status"
    headers = calculate_tuya_signature(access_token, "GET", url_path)
    url = f"https://openapi.tuyaeu.com{url_path}"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json() if response.text else {}
        if 'result' in data and data['result']:
            # Limpiar ruidos innecesarios como alarm_volume
            data['result'] = [item for item in data['result'] if item.get('code') != 'alarm_volume']
        return data
    except Exception as e:
        return {"error": str(e), "success": False}

# -------------------------
# DB Utilities
# -------------------------
def db_connect():
    return psycopg2.connect(DATABASE_URL)

def create_tables_if_not_exist():
    create_sql = """
    CREATE TABLE IF NOT EXISTS sensor_readings (id SERIAL PRIMARY KEY, device_id TEXT, recorded_at TIMESTAMP NOT NULL, raw JSONB NOT NULL);
    CREATE TABLE IF NOT EXISTS sensor_snapshot (device_id TEXT PRIMARY KEY, last_recorded_at TIMESTAMP NOT NULL, raw JSONB NOT NULL);
    CREATE TABLE IF NOT EXISTS sensor_metrics (
        id SERIAL PRIMARY KEY, device_id TEXT, recorded_at TIMESTAMP NOT NULL,
        air_quality_index TEXT, temp_current DOUBLE PRECISION, humidity_value DOUBLE PRECISION,
        co2_value DOUBLE PRECISION, ch2o_value DOUBLE PRECISION, pm25_value DOUBLE PRECISION,
        pm1 DOUBLE PRECISION, pm10 DOUBLE PRECISION, battery_percentage DOUBLE PRECISION,
        charge_state BOOLEAN, raw JSONB, CONSTRAINT uq_device_time UNIQUE(device_id, recorded_at)
    );
    CREATE INDEX IF NOT EXISTS idx_metrics_device_time ON sensor_metrics(device_id, recorded_at DESC);
    """
    conn = None
    try:
        conn = db_connect(); cur = conn.cursor()
        cur.execute(create_sql); conn.commit(); cur.close()
        print("✅ Base de datos verificada.")
    except Exception as e:
        print("⚠️ Error DB:", e)
    finally:
        if conn: conn.close()

CODE_TO_COLUMN = {
    "air_quality_index": "air_quality_index", "temp_current": "temp_current",
    "humidity_value": "humidity_value", "co2_value": "co2_value",
    "ch2o_value": "ch2o_value", "pm25_value": "pm25_value",
    "pm1": "pm1", "pm10": "pm10", "battery_percentage": "battery_percentage",
    "charge_state": "charge_state"
}

def save_full_reading(device_id, full_data):
    conn = None
    try:
        conn = db_connect(); cur = conn.cursor()
        
        # Procesar fecha
        ts_ms = full_data.get("t", time.time() * 1000)
        recorded_at = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(ZoneInfo("America/El_Salvador"))
        naive_dt = recorded_at.replace(tzinfo=None)
        raw_json = json.dumps(full_data, default=str)

        # 1. Guardar en sensor_readings
        cur.execute("INSERT INTO sensor_readings (device_id, recorded_at, raw) VALUES (%s, %s, %s::jsonb) RETURNING id;", (device_id, naive_dt, raw_json))
        reading_id = cur.fetchone()[0]

        # 2. Mapear columnas para sensor_metrics
        cols = {col: None for col in CODE_TO_COLUMN.values()}
        for it in (full_data.get("result") or []):
            code = it.get("code")
            if code in CODE_TO_COLUMN:
                val = it.get("value")
                cols[CODE_TO_COLUMN[code]] = float(val) if isinstance(val, (int, float)) else val

        cur.execute("""
            INSERT INTO sensor_metrics (device_id, recorded_at, air_quality_index, temp_current, humidity_value, co2_value, 
            ch2o_value, pm25_value, pm1, pm10, battery_percentage, charge_state, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
        """, (device_id, naive_dt, cols["air_quality_index"], cols["temp_current"], cols["humidity_value"], cols["co2_value"],
              cols["ch2o_value"], cols["pm25_value"], cols["pm1"], cols["pm10"], cols["battery_percentage"], cols["charge_state"], raw_json))
        metric_id = cur.fetchone()[0]

        # 3. Snapshot
        cur.execute("INSERT INTO sensor_snapshot (device_id, last_recorded_at, raw) VALUES (%s, %s, %s::jsonb) ON CONFLICT (device_id) DO UPDATE SET last_recorded_at = EXCLUDED.last_recorded_at, raw = EXCLUDED.raw;", (device_id, naive_dt, raw_json))

        conn.commit(); cur.close()
        return {"success": True, "reading_id": reading_id, "metric_id": metric_id, "recorded_at": naive_dt.isoformat()}
    except Exception as e:
        if conn: conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn: conn.close()

# -------------------------
# JOB PERIÓDICO: Solo Cara Sucia
# -------------------------
SAVE_INTERVAL_SECONDS = 10 * 60

def periodic_save_job():
    while True:
        try:
            print(f"⏱️ Job: Consultando estudio principal ({SENSORS_MAP[ID_CARA_SUCIA]})...")
            data = get_tuya_data(ID_CARA_SUCIA)
            if "error" not in data:
                res = save_full_reading(ID_CARA_SUCIA, data)
                print(f"✅ Guardado Cara Sucia: {res.get('metric_id')}" if res.get("success") else f"❌ Error: {res.get('error')}")
        except Exception as e:
            print("⚠️ Error en Job:", e)
        time.sleep(SAVE_INTERVAL_SECONDS)

# -------------------------
# ENDPOINTS
# -------------------------

@app.route('/api/sensors/realtime', methods=['GET'])
def get_all_realtime():
    """Muestra los 3 sensores en tiempo real sin guardar en base de datos."""
    results = []
    for dev_id, name in SENSORS_MAP.items():
        data = get_tuya_data(dev_id)
        results.append({
            "name": name,
            "device_id": dev_id,
            "success": "error" not in data,
            "data": data.get("result", []),
            "error": data.get("error") if "error" in data else None
        })
    return jsonify({"timestamp": int(time.time()), "devices": results})

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    """Obtiene históricos de Cara Sucia."""
    start_date = request.args.get('start_date')
    limit = request.args.get('limit', type=int)
    try:
        conn = db_connect()
        query = "SELECT recorded_at, temp_current, humidity_value, co2_value, pm25_value FROM sensor_metrics WHERE device_id = %s"
        params = [ID_CARA_SUCIA]
        if start_date:
            query += " AND recorded_at >= %s"; params.append(start_date)
        
        query += " ORDER BY recorded_at " + ("DESC LIMIT " + str(limit) if limit else "ASC")
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        
        if limit: df = df.sort_values(by='recorded_at', ascending=True)
        df['recorded_at'] = pd.to_datetime(df['recorded_at']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        return jsonify({"data": df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/save-now', methods=['POST'])
def save_now():
    """Fuerza guardado manual solo de Cara Sucia."""
    data = get_tuya_data(ID_CARA_SUCIA)
    if "error" in data: return jsonify(data), 500
    return jsonify(save_full_reading(ID_CARA_SUCIA, data))

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "study_device": ID_CARA_SUCIA, "monitoring": list(SENSORS_MAP.values())})

# -------------------------
# INICIO
# -------------------------
_initialized = False
def init_background():
    global _initialized
    if not _initialized:
        create_tables_if_not_exist()
        threading.Thread(target=periodic_save_job, daemon=True).start()
        _initialized = True

init_background()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)