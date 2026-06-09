"""
Monitt.io — MQTT Message Processor
Subscribes to the MQTT broker, parses incoming TRB256 messages,
and writes sensor readings to TimescaleDB.

Topic structure:
  monitt/{customer_id}/{building_id}/{device_id}/telemetry
"""

import os
import json
import logging
import struct
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────

MQTT_HOST     = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT     = int(os.getenv("MQTT_PORT", 1883))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "monitt")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TOPIC    = os.getenv("MQTT_TOPIC", "monitt/#")

DB_URL = os.getenv("DATABASE_URL")  # Railway injects this automatically

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# Database
# ─────────────────────────────────────────

def get_db():
    """Return a psycopg2 connection. Retries on failure."""
    while True:
        try:
            conn = psycopg2.connect(DB_URL)
            log.info("Connected to database")
            return conn
        except Exception as e:
            log.error(f"DB connection failed: {e} — retrying in 5s")
            time.sleep(5)


def lookup_device(conn, device_serial: str):
    """
    Look up device, building, and customer IDs by device serial number.
    Returns a dict or None if not found.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT d.id as device_id, d.building_id, b.customer_id
            FROM devices d
            JOIN buildings b ON b.id = d.building_id
            WHERE d.serial_number = %s
        """, (device_serial,))
        return cur.fetchone()


def insert_reading(conn, reading: dict):
    """Insert a single sensor reading into TimescaleDB."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO readings (
                time, device_id, building_id, customer_id,
                engine_state, engine_rpm, coolant_temp_c, oil_pressure_kpa,
                fuel_level_pct, battery_voltage_mv,
                gen_frequency_hz, gen_l1_voltage_mv, gen_l1_current_ma, gen_l1_watts,
                common_alarm, common_warning
            ) VALUES (
                %(time)s, %(device_id)s, %(building_id)s, %(customer_id)s,
                %(engine_state)s, %(engine_rpm)s, %(coolant_temp_c)s, %(oil_pressure_kpa)s,
                %(fuel_level_pct)s, %(battery_voltage_mv)s,
                %(gen_frequency_hz)s, %(gen_l1_voltage_mv)s, %(gen_l1_current_ma)s, %(gen_l1_watts)s,
                %(common_alarm)s, %(common_warning)s
            )
        """, reading)
    conn.commit()


# ─────────────────────────────────────────
# Modbus response parser
# ─────────────────────────────────────────

def parse_dse_modbus_response(payload: bytes) -> dict | None:
    """
    Parse a Modbus RTU response from a DSE controller.

    The TRB256 MQTT Gateway returns raw Modbus RTU bytes.
    DSE GenComm registers (starting at 0x4000):
      Offset 0:  Engine state         (1 = stopped, 2 = cranking, 3 = running, etc.)
      Offset 1:  Oil pressure (kPa)
      Offset 2:  Coolant temp (°C)
      Offset 3:  Fuel level (%)
      Offset 5:  Battery voltage (mV / 10 → multiply by 10)
      Offset 8:  Engine RPM
      Offset 14: Gen L1 voltage (V * 10)
      Offset 15: Gen frequency (Hz * 10)
      Offset 18: Gen L1 current (A * 10)
      Offset 22: Gen L1 power (kW * 10)
      Offset 30: Common alarm (bit 0)
      Offset 31: Common warning (bit 0)

    NOTE: This parser will be refined once we see real device output.
    The register map is based on DSE GenComm v4 documentation.
    """
    try:
        # Modbus RTU read holding registers response:
        # [device_addr][0x03][byte_count][data...][CRC_lo][CRC_hi]
        if len(payload) < 5:
            return None

        device_addr = payload[0]
        func_code   = payload[1]
        byte_count  = payload[2]

        if func_code != 0x03:
            log.warning(f"Unexpected Modbus function code: {func_code}")
            return None

        data = payload[3:3 + byte_count]

        def reg(offset):
            """Read a 16-bit register at word offset."""
            idx = offset * 2
            if idx + 2 > len(data):
                return None
            return struct.unpack(">H", data[idx:idx+2])[0]

        def sreg(offset):
            """Read a signed 16-bit register."""
            idx = offset * 2
            if idx + 2 > len(data):
                return None
            return struct.unpack(">h", data[idx:idx+2])[0]

        return {
            "engine_state":       reg(0),
            "oil_pressure_kpa":   reg(1),
            "coolant_temp_c":     sreg(2),
            "fuel_level_pct":     reg(3),
            "battery_voltage_mv": (reg(5) or 0) * 100,   # DSE returns V*100
            "engine_rpm":         reg(8),
            "gen_l1_voltage_mv":  (reg(14) or 0) * 100,  # DSE returns V*10, we store mV
            "gen_frequency_hz":   reg(15),                # Hz*10
            "gen_l1_current_ma":  (reg(18) or 0) * 100,  # DSE returns A*10, we store mA
            "gen_l1_watts":       (reg(22) or 0) * 100,  # DSE returns kW*10, we store W
            "common_alarm":       bool(reg(30) & 0x01) if reg(30) is not None else False,
            "common_warning":     bool(reg(31) & 0x01) if reg(31) is not None else False,
        }

    except Exception as e:
        log.error(f"Failed to parse Modbus response: {e}")
        return None


def parse_json_payload(payload: bytes) -> dict | None:
    """
    Some TRB256 configurations send JSON instead of raw Modbus bytes.
    Handle both formats.
    """
    try:
        data = json.loads(payload.decode("utf-8"))
        return {
            "engine_state":       data.get("engine_state"),
            "oil_pressure_kpa":   data.get("oil_pressure_kpa"),
            "coolant_temp_c":     data.get("coolant_temp_c"),
            "fuel_level_pct":     data.get("fuel_level_pct"),
            "battery_voltage_mv": data.get("battery_voltage_mv"),
            "engine_rpm":         data.get("engine_rpm"),
            "gen_frequency_hz":   data.get("gen_frequency_hz"),
            "gen_l1_voltage_mv":  data.get("gen_l1_voltage_mv"),
            "gen_l1_current_ma":  data.get("gen_l1_current_ma"),
            "gen_l1_watts":       data.get("gen_l1_watts"),
            "common_alarm":       data.get("common_alarm", False),
            "common_warning":     data.get("common_warning", False),
        }
    except Exception:
        return None


# ─────────────────────────────────────────
# MQTT callbacks
# ─────────────────────────────────────────

def make_on_message(conn):
    def on_message(client, userdata, msg):
        topic = msg.topic
        payload = msg.payload

        log.info(f"Message on {topic} ({len(payload)} bytes)")

        # Parse topic: monitt/{customer_id}/{building_id}/{device_serial}/telemetry
        parts = topic.split("/")
        if len(parts) < 5 or parts[0] != "monitt":
            log.warning(f"Unexpected topic format: {topic}")
            return

        device_serial = parts[3]
        msg_type      = parts[4]  # telemetry, status, etc.

        if msg_type != "telemetry":
            return

        # Look up device in DB
        device = lookup_device(conn, device_serial)
        if not device:
            log.warning(f"Unknown device serial: {device_serial} — register it in the database first")
            return

        # Try JSON first, fall back to Modbus binary
        sensor_data = parse_json_payload(payload) or parse_dse_modbus_response(payload)

        if not sensor_data:
            log.error(f"Could not parse payload from {device_serial}: {payload.hex()}")
            return

        reading = {
            "time":        datetime.now(timezone.utc),
            "device_id":   str(device["device_id"]),
            "building_id": str(device["building_id"]),
            "customer_id": str(device["customer_id"]),
            **sensor_data,
        }

        try:
            insert_reading(conn, reading)
            log.info(f"Saved reading from {device_serial} — RPM:{sensor_data.get('engine_rpm')} fuel:{sensor_data.get('fuel_level_pct')}%")
        except Exception as e:
            log.error(f"DB insert failed: {e}")
            conn.rollback()

    return on_message


def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        log.info(f"Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC, qos=1)
        log.info(f"Subscribed to {MQTT_TOPIC}")
    else:
        log.error(f"MQTT connection failed with reason code {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties=None):
    log.warning(f"Disconnected from MQTT broker (code {reason_code}) — will reconnect")


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────

def main():
    log.info("Starting Monitt.io MQTT processor")

    conn = get_db()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="monitt-processor")
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = make_on_message(conn)

    # Reconnect automatically
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            log.error(f"MQTT error: {e} — retrying in 10s")
            time.sleep(10)


if __name__ == "__main__":
    main()
