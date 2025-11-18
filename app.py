# app_full_sensors_columns.py
# API completa: mantiene tus endpoints originales + guardado columnar + job periódico cada 10min
from flask import Flask, jsonify, request
from flask_cors import CORS
import hashlib
import hmac
import time
import requests
import threading
from datetime import datetime, timedelta, timezone
import os
import psycopg2
import psycopg2.extras
import json
import traceback

app = Flask(__name__)
CORS(app)

# -------------------------
# CONFIG: Tuya
# -------------------------
CLIENT_ID = "dhd4knqghttrtrx3n5vu"
ACCESS_SECRET = "d51e817b7fec4b6091b51a2cc3c323d5"
DEVICE_ID = "bf9b2ec293a9f9b528lkdl"

# -------------------------
# CONFIG: Database (tu URL)
# -------------------------
DATABASE_URL = "postgresql://postgres:VaLqxGBzdzZmBTddchzzryKgNeQmoPfI@switchback.proxy.rlwy.net:14573/railway?sslmode=require"

# -------------------------
# TOKEN MANAGEMENT (Tuya) - thread-safe
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
        if response.status_code == 200:
            if data.get("success") and "result" in data:
                return {
                    "token": data["result"]["access_token"],
                    "expires_in": data["result"].get("expire_time", 7200)
                }
            else:
                return {"error": f"Error en respuesta de Tuya: {data}"}
        else:
            return {"error": f"HTTP {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": f"Error al obtener token: {str(e)}"}

def ensure_valid_token():
    global current_token, token_expires_at
    with token_lock:
        now = datetime.now(timezone.utc)
        if not current_token or not token_expires_at or now >= (token_expires_at - timedelta(minutes=5)):
            print("🔄 Renovando token Tuya...")
            token_result = get_tuya_token()
            if "error" in token_result:
                print("⚠️ Error renovando token:", token_result["error"])
                return token_result
            current_token = token_result["token"]
            token_expires_at = now + timedelta(seconds=token_result["expires_in"])
            print("✅ Token renovado. Expira:", token_expires_at.isoformat())
        return {"token": current_token}

def calculate_tuya_signature(access_token, method="GET", url_path="/v1.0/devices", body=""):
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

# -------------------------
# TUYA: obtener estado del dispositivo
# -------------------------
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
        data = response.json() if response.text else {}
        # opcional: filtrar códigos no deseados
        if 'result' in data and data['result']:
            data['result'] = [item for item in data['result'] if item.get('code') != 'alarm_volume']
        return data
    except Exception as e:
        return {"error": f"Error al conectar con Tuya: {str(e)}", "success": False}

# -------------------------
# DB utilities
# -------------------------
def db_connect():
    # psycopg2 acepta la URL directamente
    return psycopg2.connect(DATABASE_URL)

def create_tables_if_not_exist():
    """
    Crea:
      - sensor_readings (raw)
      - sensor_snapshot (opcional para últimas raw)
      - sensor_metrics (columnar según confirmación)
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS sensor_readings (
        id SERIAL PRIMARY KEY,
        device_id TEXT,
        recorded_at TIMESTAMPTZ NOT NULL,
        raw JSONB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sensor_snapshot (
        device_id TEXT PRIMARY KEY,
        last_recorded_at TIMESTAMPTZ NOT NULL,
        raw JSONB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sensor_metrics (
        id SERIAL PRIMARY KEY,
        device_id TEXT,
        recorded_at TIMESTAMPTZ NOT NULL,
        air_quality_index TEXT,
        temp_current DOUBLE PRECISION,
        humidity_value DOUBLE PRECISION,
        co2_value DOUBLE PRECISION,
        ch2o_value DOUBLE PRECISION,
        pm25_value DOUBLE PRECISION,
        pm1 DOUBLE PRECISION,
        pm10 DOUBLE PRECISION,
        battery_percentage DOUBLE PRECISION,
        charge_state BOOLEAN,
        raw JSONB,
        CONSTRAINT uq_device_time UNIQUE(device_id, recorded_at)
    );

    CREATE INDEX IF NOT EXISTS idx_metrics_device_time ON sensor_metrics(device_id, recorded_at DESC);
    """
    conn = None
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        cur.close()
        print("✅ Tablas verificadas/creadas.")
    except Exception as e:
        print("⚠️ Error creando tablas:", e)
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

# -------------------------
# Helpers: timestamp
# -------------------------
def parse_recorded_at_from_response(resp):
    ts_ms = resp.get("t")
    try:
        if isinstance(ts_ms, (int, float)):
            return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    except Exception:
        pass
    return datetime.now(timezone.utc)

# -------------------------
# Mapeo code -> columna
# -------------------------
CODE_TO_COLUMN = {
    "air_quality_index": "air_quality_index",
    "temp_current": "temp_current",
    "humidity_value": "humidity_value",
    "co2_value": "co2_value",
    "ch2o_value": "ch2o_value",
    "pm25_value": "pm25_value",
    "pm1": "pm1",
    "pm10": "pm10",
    "battery_percentage": "battery_percentage",
    "charge_state": "charge_state"
}

# -------------------------
# Lógica de guardado: raw + columnar
# -------------------------
def save_full_reading(device_id, full_data):
    """
    Guarda en sensor_readings (raw), sensor_metrics (columnar) y actualiza sensor_snapshot.
    Retorna dict con success y detalles.
    """
    conn = None
    try:
        conn = db_connect()
        cur = conn.cursor()
        recorded_at = parse_recorded_at_from_response(full_data)
        raw_json = json.dumps(full_data, default=str)

        # 1) Insert raw reading
        cur.execute(
            "INSERT INTO sensor_readings (device_id, recorded_at, raw) VALUES (%s, %s, %s::jsonb) RETURNING id;",
            (device_id, recorded_at, raw_json)
        )
        reading_id = cur.fetchone()[0]

        # 2) Preparar columnas para sensor_metrics
        cols = {col: None for col in CODE_TO_COLUMN.values()}
        items = full_data.get("result") or []
        for it in items:
            code = it.get("code")
            if code not in CODE_TO_COLUMN:
                continue
            col = CODE_TO_COLUMN[code]
            val = it.get("value")
            if isinstance(val, bool):
                cols[col] = val
            elif isinstance(val, (int, float)):
                cols[col] = float(val)
            elif isinstance(val, str):
                s = val.strip()
                # detectar booleans en string
                if s.lower() in ("true", "false"):
                    cols[col] = (s.lower() == "true")
                else:
                    # intentar parsear número
                    try:
                        num = float(s)
                        cols[col] = num
                    except:
                        cols[col] = s
            else:
                cols[col] = None

        # 3) Insertar fila columnar
        cur.execute(
            """
            INSERT INTO sensor_metrics
              (device_id, recorded_at,
               air_quality_index, temp_current, humidity_value, co2_value,
               ch2o_value, pm25_value, pm1, pm10,
               battery_percentage, charge_state, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                device_id, recorded_at,
                cols["air_quality_index"], cols["temp_current"], cols["humidity_value"], cols["co2_value"],
                cols["ch2o_value"], cols["pm25_value"], cols["pm1"], cols["pm10"],
                cols["battery_percentage"], cols["charge_state"], raw_json
            )
        )
        metric_id = cur.fetchone()[0]

        # 4) Upsert snapshot (raw)
        cur.execute(
            """
            INSERT INTO sensor_snapshot (device_id, last_recorded_at, raw)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (device_id) DO UPDATE
              SET last_recorded_at = EXCLUDED.last_recorded_at,
                  raw = EXCLUDED.raw;
            """,
            (device_id, recorded_at, raw_json)
        )

        conn.commit()
        cur.close()
        return {"success": True, "reading_id": reading_id, "metric_id": metric_id, "recorded_at": recorded_at.isoformat()}
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        traceback.print_exc()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            conn.close()

# -------------------------
# JOB periódico (cada 10 minutos)
# -------------------------
SAVE_INTERVAL_SECONDS = 10 * 60  # 10 minutos

def periodic_save_job():
    while True:
        try:
            print("⏱️ Guardado periódico: obteniendo datos Tuya...")
            data = get_tuya_data()
            if "error" in data:
                print("⚠️ Error al obtener datos Tuya:", data.get("error"))
            else:
                result = save_full_reading(DEVICE_ID, data)
                if result.get("success"):
                    print(f"✅ Guardado periódico: reading_id={result['reading_id']} metric_id={result['metric_id']} at {result['recorded_at']}")
                else:
                    print("⚠️ Error guardando (periódico):", result.get("error"))
        except Exception as e:
            print("⚠️ Excepción en periodic_save_job:", e)
            traceback.print_exc()
        time.sleep(SAVE_INTERVAL_SECONDS)

# -------------------------
# Endpoints originales (sin cambios funcionales)
# -------------------------
@app.route('/api/sensors', methods=['GET'])
def get_sensors():
    """Obtiene datos raw de sensores (tal cual desde Tuya)"""
    return jsonify(get_tuya_data())

@app.route('/api/sensors/formatted', methods=['GET'])
def get_sensors_formatted():
    data = get_tuya_data()
    if 'error' in data:
        return jsonify(data)

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
    global current_token, token_expires_at

    if not current_token or not token_expires_at:
        return jsonify({
            "status": "no_token",
            "message": "No hay token activo"
        })

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
        "status": "healthy",
        "timestamp": int(time.time()),
        "service": "Tuya Sensors API",
        "token_status": token_status
    })

# -------------------------
# Nuevos endpoints para guardado y consulta columnar
# -------------------------
@app.route('/api/save-now', methods=['POST', 'GET'])
def save_now():
    data = get_tuya_data()
    if "error" in data:
        return jsonify({"success": False, "error": data.get("error")}), 500
    result = save_full_reading(DEVICE_ID, data)
    if result.get("success"):
        return jsonify(result)
    else:
        return jsonify(result), 500

@app.route('/api/latest-metrics', methods=['GET'])
def latest_metrics():
    device_id = request.args.get("device_id", DEVICE_ID)
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT * FROM sensor_metrics
            WHERE device_id = %s
            ORDER BY recorded_at DESC
            LIMIT 1;
            """, (device_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return jsonify({"success": True, "device_id": device_id, "metrics": None})
        if row.get("recorded_at"):
            row["recorded_at"] = row["recorded_at"].isoformat()
        return jsonify({"success": True, "device_id": device_id, "metrics": row})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/snapshots', methods=['GET'])
def snapshots():
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT device_id, last_recorded_at, raw FROM sensor_snapshot;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"success": True, "snapshots": rows})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

# -------------------------
# Root: mantener JSON de info original (con endpoints y features)
# -------------------------
@app.route('/', methods=['GET'])
def api_info():
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

# -------------------------
# Startup: crear tablas, obtener token y lanzar job periódico
# -------------------------
if __name__ == "__main__":
    print("🚀 Tuya Sensors API (columnar) iniciando...")
    create_tables_if_not_exist()

    # token inicial (no crítico)
    initial = ensure_valid_token()
    if "error" in initial:
        print("⚠️ No se pudo obtener token inicial:", initial["error"])
    else:
        print("✅ Token inicial obtenido")

    # lanzar job periódico en background (daemon)
    t = threading.Thread(target=periodic_save_job, daemon=True)
    t.start()
    print(f"⏱️ Hilo de guardado periódico iniciado (cada {SAVE_INTERVAL_SECONDS} segundos)")

    port = int(os.environ.get("PORT", 5000))
    print(f"Escuchando en puerto {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
