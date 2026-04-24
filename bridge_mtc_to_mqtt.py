import os, json, time, signal, logging
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import requests
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

AGENT_URL = os.environ.get("AGENT_URL", "http://10.0.0.64:5001").rstrip("/")
MQTT_HOST = os.environ.get("MQTT_HOST", "10.0.0.180")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "harvester")
MQTT_PASS = os.environ.get("MQTT_PASS", "letmein")
SITE      = os.environ.get("SITE", "foxwood")
DEVICE_ID = os.environ.get("DEVICE_ID", "MAZAK-SN328525")

POLL_INTERVAL_SEC      = int(os.environ.get("POLL_INTERVAL_SEC", "2"))
TELEMETRY_INTERVAL_SEC = int(os.environ.get("TELEMETRY_INTERVAL_SEC", "1800"))
HEARTBEAT_INTERVAL_SEC = int(os.environ.get("HEARTBEAT_INTERVAL_SEC", "30"))

TOPIC_ROOT      = f"{SITE}/mazak"
TOPIC_STATUS    = f"{TOPIC_ROOT}/status/{DEVICE_ID}"
TOPIC_HEARTBEAT = f"{TOPIC_STATUS}/hb"
TOPIC_STATE     = f"{TOPIC_ROOT}/state"
TOPIC_PROGRAM   = f"{TOPIC_ROOT}/program"
TOPIC_TELEMETRY = f"{TOPIC_ROOT}/telemetry"

TRACKED = {
    "avail", "execution", "program", "subprogram", "program_cmt", "partname",
    "PartCountAct", "ProgramTimeStudy",
    "AutoOperationTime", "AutoCuttingTime", "LaserTotalTime", "CNCPweronTime",
    "Material", "SheetThickness",
    "Laser_power", "Laser_duty", "Laser_frequency",
    "NCmode", "Feedratefrt", "estop",
}
STATE_FIELDS = ("execution", "avail", "program", "NCmode", "estop")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mazak-bridge")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def coerce_num(v):
    if v in (None, "UNAVAILABLE", ""):
        return None
    try:
        return float(v) if "." in v else int(v)
    except (TypeError, ValueError):
        return None


def coerce_str(v):
    if v in (None, "UNAVAILABLE", ""):
        return None
    return v


def fetch_current() -> dict:
    name_filter = " or ".join(f'@name="{n}"' for n in TRACKED)
    url = f'{AGENT_URL}/current?path=//DataItem[{name_filter}]'
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    out = {}
    for el in root.iter():
        n = el.get("name")
        if n in TRACKED:
            out[n] = el.text.strip() if el.text else None
    return out


def telemetry_payload(state: dict) -> dict:
    return {
        "ts": utcnow_iso(),
        "device_id": DEVICE_ID,
        "execution":            coerce_str(state.get("execution")),
        "availability":         coerce_str(state.get("avail")),
        "mode":                 coerce_str(state.get("NCmode")),
        "program":              coerce_str(state.get("program")),
        "subprogram":           coerce_str(state.get("subprogram")),
        "part_count":           coerce_num(state.get("PartCountAct")),
        "auto_operation_time_s":coerce_num(state.get("AutoOperationTime")),
        "auto_cutting_time_s":  coerce_num(state.get("AutoCuttingTime")),
        "laser_total_time_s":   coerce_num(state.get("LaserTotalTime")),
        "cnc_poweron_time_s":   coerce_num(state.get("CNCPweronTime")),
        "laser_power_w":        coerce_num(state.get("Laser_power")),
        "laser_duty_pct":       coerce_num(state.get("Laser_duty")),
        "laser_frequency_hz":   coerce_num(state.get("Laser_frequency")),
        "feedrate":             coerce_num(state.get("Feedratefrt")),
        "material":             coerce_str(state.get("Material")),
        "sheet_thickness":      coerce_str(state.get("SheetThickness")),
    }


def detect_state_changes(prev: dict, curr: dict) -> list:
    out = []
    for f in STATE_FIELDS:
        if prev.get(f) != curr.get(f):
            out.append({
                "ts": utcnow_iso(),
                "device_id": DEVICE_ID,
                "field": f,
                "from_value": coerce_str(prev.get(f)),
                "to_value":   coerce_str(curr.get(f)),
            })
    return out


def detect_program_completion(prev: dict, curr: dict):
    if prev.get("execution") == "ACTIVE" and curr.get("execution") not in (None, "ACTIVE"):
        return {
            "ts": utcnow_iso(),
            "device_id": DEVICE_ID,
            "program":         coerce_str(prev.get("program")),
            "comment":         coerce_str(prev.get("program_cmt")),
            "part_name":       coerce_str(prev.get("partname")),
            "ended_state":     curr.get("execution"),
            "runtime_seconds": coerce_num(prev.get("ProgramTimeStudy")),
            "auto_cutting_time_s_end":   coerce_num(prev.get("AutoCuttingTime")),
            "auto_operation_time_s_end": coerce_num(prev.get("AutoOperationTime")),
            "part_count_end":  coerce_num(prev.get("PartCountAct")),
            "material":        coerce_str(prev.get("Material")),
            "sheet_thickness": coerce_str(prev.get("SheetThickness")),
        }
    return None


def publish(client, topic, payload, retain=False, qos=1):
    if not isinstance(payload, str):
        payload = json.dumps(payload, separators=(",", ":"))
    client.publish(topic, payload, qos=qos, retain=retain)


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"mazak-bridge-{DEVICE_ID}")
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.will_set(TOPIC_STATUS, payload="offline", qos=1, retain=True)
    client.reconnect_delay_set(min_delay=1, max_delay=60)

    log.info(f"connecting mqtt {MQTT_HOST}:{MQTT_PORT}")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    publish(client, TOPIC_STATUS, "online", retain=True, qos=1)
    log.info(f"published online -> {TOPIC_STATUS}")

    prev = {}
    last_telemetry = 0.0
    last_heartbeat = 0.0

    stop = False
    def shutdown(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    while not stop:
        loop_start = time.time()
        try:
            curr = fetch_current()

            if prev:
                for ev in detect_state_changes(prev, curr):
                    publish(client, TOPIC_STATE, ev)
                    log.info(f"state {ev['field']}: {ev['from_value']} -> {ev['to_value']}")

                done = detect_program_completion(prev, curr)
                if done and done["program"]:
                    publish(client, TOPIC_PROGRAM, done)
                    log.info(f"program done: {done['program']} runtime={done['runtime_seconds']}s parts_end={done['part_count_end']}")

            now = time.time()
            if now - last_telemetry >= TELEMETRY_INTERVAL_SEC:
                snap = telemetry_payload(curr)
                publish(client, TOPIC_TELEMETRY, snap)
                last_telemetry = now
                log.info(f"telemetry exec={snap['execution']} program={snap['program']}")

            if now - last_heartbeat >= HEARTBEAT_INTERVAL_SEC:
                publish(client, TOPIC_HEARTBEAT,
                        {"fw": "mtc-agent-1.4.0.10",
                         "execution": coerce_str(curr.get("execution"))},
                        qos=0)
                last_heartbeat = now

            prev = curr

        except requests.RequestException as e:
            log.error(f"agent fetch failed: {e}")
        except ET.ParseError as e:
            log.error(f"xml parse failed: {e}")
        except Exception as e:
            log.exception(f"unexpected error: {e}")

        elapsed = time.time() - loop_start
        time.sleep(max(0.0, POLL_INTERVAL_SEC - elapsed))

    log.info("shutdown")
    publish(client, TOPIC_STATUS, "offline", retain=True, qos=1)
    time.sleep(0.5)
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
