# app.py
# API unificada: Tuya IoT + Background Jobs (Estudio Cara Sucia) + Real-time (Nahuizalco/Juayúa)
# Versión: 2.6 - Fix Parseo de Fechas para Frontend y Fix Boolean Cast para PostgreSQL

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

SENSORS_MAP = {
    ID_CARA_SUCIA: "Cara Sucia (Estudio Principal)",
    ID_NAHUIZALCO: "Nahuizalco (Tiempo Real)",
    ID_JUAYUA:     "Juayúa (Tiempo Real)"
}

# -------------------------
# CONFIG: Tuya Auth & Database
# -------------------------
CLIENT_ID = os.getenv("TUYA_CLIENT_ID", "dhd4knqghttrtrx3n5vu")
ACCESS_SECRET = os.getenv("TUYA_ACCESS_SECRET", "d51e817b7fec4b6091b51a2cc3c323d5")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:VaLqxGBzdzZmBTddchzzryKgNeQmoPfI@switchback.proxy.rlwy.net:14573/railway?sslmode=require")

# -------------------------
# TOKEN MANAGEMENT (Tuya)
# -------------------------
current_token = None
token_expires_at = None
token_lock = threading.Lock()

def get_tuya_token():
    timestamp = str(int(time.time() * 1000))
    url_path = "/v1.0/token?grant_type=1"
    content_sha256 = hashlib.sha256("".encode()).hexdigest()
    string_to_sign = f"GET\n{content_sha256}\n\n{url_path}"
    str_to_sign = CLIENT_ID + timestamp + string_to_sign
    signature = hmac.new(ACCESS_SECRET.encode(), str_to_sign.encode(), hashlib.sha256).hexdigest().upper()

    url = f"https://openapi.tuyaeu.com{url_path}"
    headers = {"client_id": CLIENT_ID, "sign": signature, "t": timestamp, "sign_method": "HMAC-SHA256"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        if response.status_code == 200 and data.get("success"):
            return {"token": data["result"]["access_token"], "expires_in": data["result"].get("expire_time", 7200)}
        return {"error": f"Error Tuya Token: {data}"}
    except Exception as e:
        return {"error": str(e)}

def ensure_valid_token():
    global current_token, token_expires_at
    with token_lock:
        now = datetime.now(timezone.utc)
        if not current_token or not token_expires_at or now >= (token_expires_at - timedelta(minutes=5)):
            token_result = get_tuya_token()
            if "error" in token_result: return token_result
            current_token = token_result["token"]
            token_expires_at = now + timedelta(seconds=token_result["expires_in"])
            print(f"✅ Token renovado. Expira: {token_expires_at}")
        return {"token": current_token}

def calculate_tuya_signature(access_token, method, url_path, body=""):
    timestamp = str(int(time.time() * 1000))
    content_sha256 = hashlib.sha256(body.encode()).hexdigest()
    string_to_sign = f"{method}\n{content_sha256}\n\n{url_path}"
    str_to_sign = CLIENT_ID + access_token + timestamp + "" + string_to_sign
    signature = hmac.new(ACCESS_SECRET.encode(), str_to_sign.encode(), hashlib.sha256).hexdigest().upper()
    return {"sign_method": "HMAC-SHA256", "client_id": CLIENT_ID, "t": timestamp, "access_token": access_token, "sign": signature, "Content-Type": "application/json"}

def get_tuya_data(device_id):
    token_result = ensure_valid_token()
    if "error" in token_result: return token_result
    url_path = f"/v1.0/devices/{device_id}/status"
    headers = calculate_tuya_signature(token_result["token"], "GET", url_path)
    try:
        response = requests.get(f"https://openapi.tuyaeu.com{url_path}", headers=headers, timeout=10)
        data = response.json()
        if 'result' in data and data['result']:
            data['result'] = [item for item in data['result'] if item.get('code') != 'alarm_volume']
        return data
    except Exception as e:
        return {"error": str(e), "success": False}

# -------------------------
# DB Utilities & Fix Boolean
# -------------------------
def db_connect():
    return psycopg2.connect(DATABASE_URL)

def create_tables_if_not_exist():
    sql = """
    CREATE TABLE IF NOT EXISTS sensor_readings (id SERIAL PRIMARY KEY, device_id TEXT, recorded_at TIMESTAMP NOT NULL, raw JSONB NOT NULL);
    CREATE TABLE IF NOT EXISTS sensor_snapshot (device_id TEXT PRIMARY KEY, last_recorded_at TIMESTAMP NOT NULL, raw JSONB NOT NULL);
    CREATE TABLE IF NOT EXISTS sensor_metrics (
        id SERIAL PRIMARY KEY, device_id TEXT, recorded_at TIMESTAMP NOT NULL,
        air_quality_index TEXT, temp_current DOUBLE PRECISION, humidity_value DOUBLE PRECISION,
        co2_value DOUBLE PRECISION, ch2o_value DOUBLE PRECISION, pm25_value DOUBLE PRECISION,
        pm1 DOUBLE PRECISION, pm10 DOUBLE PRECISION, battery_percentage DOUBLE PRECISION,
        charge_state BOOLEAN, raw JSONB, CONSTRAINT uq_device_time UNIQUE(device_id, recorded_at)
    );
    """
    conn = None
    try:
        conn = db_connect(); cur = conn.cursor()
        cur.execute(sql); conn.commit(); cur.close()
        print("✅ Estructura de tablas verificada.")
    except Exception as e: print(f"⚠️ Error DB Setup: {e}")
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
        ts_ms = full_data.get("t", time.time() * 1000)
        recorded_at = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(ZoneInfo("America/El_Salvador"))
        naive_dt = recorded_at.replace(tzinfo=None)
        raw_json = json.dumps(full_data, default=str)

        # Mapeo de columnas con FIX para booleano
        cols = {col: None for col in CODE_TO_COLUMN.values()}
        for it in (full_data.get("result") or []):
            code = it.get("code")
            if code in CODE_TO_COLUMN:
                val = it.get("value")
                if code == "charge_state":
                    cols[CODE_TO_COLUMN[code]] = bool(val) if not isinstance(val, str) else val.lower() == "true"
                elif code == "air_quality_index":
                    cols[CODE_TO_COLUMN[code]] = str(val) if val is not None else None
                else:
                    try:
                        cols[CODE_TO_COLUMN[code]] = float(val) if val is not None and str(val).strip() != "" else None
                    except (ValueError, TypeError):
                        cols[CODE_TO_COLUMN[code]] = None

        cur.execute("INSERT INTO sensor_readings (device_id, recorded_at, raw) VALUES (%s, %s, %s::jsonb) RETURNING id;", (device_id, naive_dt, raw_json))
        r_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO sensor_metrics (device_id, recorded_at, air_quality_index, temp_current, humidity_value, co2_value, 
            ch2o_value, pm25_value, pm1, pm10, battery_percentage, charge_state, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
        """, (device_id, naive_dt, cols["air_quality_index"], cols["temp_current"], cols["humidity_value"], cols["co2_value"],
              cols["ch2o_value"], cols["pm25_value"], cols["pm1"], cols["pm10"], cols["battery_percentage"], cols["charge_state"], raw_json))
        m_id = cur.fetchone()[0]

        cur.execute("INSERT INTO sensor_snapshot (device_id, last_recorded_at, raw) VALUES (%s, %s, %s::jsonb) ON CONFLICT (device_id) DO UPDATE SET last_recorded_at = EXCLUDED.last_recorded_at, raw = EXCLUDED.raw;", (device_id, naive_dt, raw_json))
        
        conn.commit(); cur.close()
        return {"success": True, "reading_id": r_id, "metric_id": m_id}
    except Exception as e:
        if conn: conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        if conn: conn.close()

# -------------------------
# JOB: Solo Cara Sucia
# -------------------------
def periodic_save_job():
    while True:
        try:
            print(f"⏱️ Guardando datos de estudio: {SENSORS_MAP[ID_CARA_SUCIA]}")
            data = get_tuya_data(ID_CARA_SUCIA)
            if "error" not in data:
                res = save_full_reading(ID_CARA_SUCIA, data)
                if res.get("success"): print(f"✅ Metric ID: {res['metric_id']}")
                else: print(f"❌ Error: {res['error']}")
            else:
                print(f"❌ Error Tuya: {data.get('error')}")
        except Exception as e: print(f"⚠️ Job Error: {e}")
        time.sleep(10 * 60)

# -------------------------
# ENDPOINTS
# -------------------------
@app.route('/api/sensors/realtime', methods=['GET'])
def get_all_realtime():
    results = []
    for dev_id, name in SENSORS_MAP.items():
        data = get_tuya_data(dev_id)
        results.append({"name": name, "device_id": dev_id, "success": "error" not in data, "data": data.get("result", []), "error": data.get("error", None)})
    return jsonify({"timestamp": int(time.time()), "devices": results})

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    limit = request.args.get('limit', type=int)
    from_date_str = request.args.get('start_date')
    to_date_str = request.args.get('end_date')
    device_id = request.args.get('device_id', ID_CARA_SUCIA)

    from_date = None
    to_date = None

    # Parseo robusto de fechas adaptado para el input datetime-local de React
    try:
        if from_date_str and from_date_str.strip():
            dt = pd.to_datetime(from_date_str)
            if pd.notna(dt):
                if dt.tzinfo is None:
                    dt = dt.tz_localize('America/El_Salvador')
                else:
                    dt = dt.tz_convert('America/El_Salvador')
                from_date = dt.tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
                
        if to_date_str and to_date_str.strip():
            dt = pd.to_datetime(to_date_str)
            if pd.notna(dt):
                if dt.tzinfo is None:
                    dt = dt.tz_localize('America/El_Salvador')
                else:
                    dt = dt.tz_convert('America/El_Salvador')
                to_date = dt.tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        return jsonify({"error": f"Formato de fecha inválido: {e}"}), 400

    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = """
        SELECT recorded_at, temp_current, humidity_value, co2_value, 
               ch2o_value, pm25_value, pm1, pm10, battery_percentage
        FROM sensor_metrics
        WHERE device_id = %s
        """
        params = [device_id]

        # Filtro por fechas
        if from_date:
            query += " AND recorded_at >= %s"
            params.append(from_date)

        if to_date:
            query += " AND recorded_at <= %s"
            params.append(to_date)

        query += " ORDER BY recorded_at DESC"

        if limit:
            query += " LIMIT %s"
            params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if rows:
            df = pd.DataFrame([dict(row) for row in rows])
            df = df.sort_values(by='recorded_at', ascending=True)
            df['recorded_at'] = pd.to_datetime(df['recorded_at']).dt.strftime('%Y-%m-%dT%H:%M:%S')
            # Limpiar valores NaN que hacen crashear a Flask jsonify
            data = [{k: (v if pd.notna(v) else None) for k, v in row.items()} for row in df.to_dict(orient='records')]
        else:
            data = []

        return jsonify({"data": data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/sensors', methods=['GET'])
def get_sensors():
    # Permite pasar el ID del dispositivo por URL, por defecto usa Cara Sucia
    device_id = request.args.get("device_id", ID_CARA_SUCIA)
    return jsonify(get_tuya_data(device_id))

@app.route('/api/sensors/formatted', methods=['GET'])
def get_sensors_formatted():
    device_id = request.args.get("device_id", ID_CARA_SUCIA)
    data = get_tuya_data(device_id)
    if 'error' in data:
        return jsonify(data)

    formatted_data = {"success": True, "timestamp": int(time.time()), "sensors": []}
    sensor_names = {
        'air_quality_index': 'Calidad del Aire', 'temp_current': 'Temperatura',
        'humidity_value': 'Humedad', 'co2_value': 'CO₂', 'ch2o_value': 'Formaldehído',
        'pm25_value': 'PM2.5', 'pm1': 'PM1.0', 'pm10': 'PM10',
        'battery_percentage': 'Batería', 'charge_state': 'Estado de Carga'
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
    global current_token, token_expires_at
    if not current_token or not token_expires_at:
        return jsonify({"status": "no_token", "message": "No hay token activo"})

    now = datetime.now(timezone.utc)
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
    global current_token, token_expires_at
    with token_lock:
        current_token = None
        token_expires_at = None

    token_result = ensure_valid_token()
    if "error" in token_result:
        return jsonify({"success": False, "error": token_result["error"]}), 400

    return jsonify({"success": True, "message": "Token renovado exitosamente", "expires_at": token_expires_at.isoformat()})

@app.route('/api/health', methods=['GET'])
def health_check():
    global current_token, token_expires_at
    token_status = "valid"
    if not current_token:
        token_status = "no_token"
    elif not token_expires_at or datetime.now(timezone.utc) >= token_expires_at:
        token_status = "expired"

    return jsonify({
        "status": "healthy", "timestamp": int(time.time()),
        "service": "Tuya Sensors API Unified", "token_status": token_status
    })

@app.route('/api/save-now', methods=['POST', 'GET'])
def save_now():
    device_id = request.args.get("device_id", ID_CARA_SUCIA)
    data = get_tuya_data(device_id)
    if "error" in data:
        return jsonify({"success": False, "error": data.get("error")}), 500
    result = save_full_reading(device_id, data)
    return jsonify(result), (200 if result.get("success") else 500)

@app.route('/api/latest-metrics', methods=['GET'])
def latest_metrics():
    device_id = request.args.get("device_id", ID_CARA_SUCIA)
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM sensor_metrics WHERE device_id = %s ORDER BY recorded_at DESC LIMIT 1;", (device_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row: return jsonify({"success": True, "device_id": device_id, "metrics": None})
        if row.get("recorded_at"): row["recorded_at"] = row["recorded_at"].strftime("%Y-%m-%d %H:%M:%S")
        return jsonify({"success": True, "device_id": device_id, "metrics": row})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/snapshots', methods=['GET'])
def snapshots():
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT device_id, last_recorded_at, raw FROM sensor_snapshot;")
        rows = cur.fetchall()
        cur.close(); conn.close()
        for r in rows:
            if r.get("last_recorded_at"): r["last_recorded_at"] = r["last_recorded_at"].strftime("%Y-%m-%d %H:%M:%S")
        return jsonify({"success": True, "snapshots": rows})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/', methods=['GET'])
def api_info():
    return jsonify({
        "name": "Tuya Sensors API Unified",
        "version": "2.6",
        "endpoints": {
            "/api/metrics": "Métricas históricas (soporta start_date y end_date)",
            "/api/sensors/realtime": "Estado en tiempo real de todos los dispositivos",
            "/api/sensors": "Obtiene datos raw de sensores",
            "/api/sensors/formatted": "Obtiene datos formateados de sensores",
            "/api/token": "Información del token actual",
            "/api/token/refresh": "Renueva el token manualmente",
            "/api/health": "Estado de la API y token",
            "/api/save-now": "Fuerza un guardado inmediato en BD",
            "/api/latest-metrics": "Último registro columnar",
            "/api/snapshots": "Último raw por dispositivo"
        }
    })

# -------------------------
# INICIO
# -------------------------
if __name__ == "__main__":
    create_tables_if_not_exist()
    threading.Thread(target=periodic_save_job, daemon=True).start()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)