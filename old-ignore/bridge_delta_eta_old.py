import json
import threading
import time
import queue
import datetime
import operator
from functools import reduce
from kafka import KafkaConsumer, KafkaProducer
from pyais import decode as ais_decode
import os
import requests
from datetime import datetime as dt, timedelta
from config_loader import load_kafka_config_from_dashboard

# ----------------------------------------------------
# CONFIG
# ----------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

MAIN_TOPIC = "ais.raw"
SIM_TOPIC = "ais_simulation.raw"
ALL_TOPICS = [MAIN_TOPIC, SIM_TOPIC]

ANALYTICS_TOPIC = "analytics_ais.raw"

API_BASE = "http://87.26.178.190:25080"

message_queue = queue.Queue(maxsize=10000)
ships_db = {}
multipart_buffer = {}
simulation_state = {}   # <-- stato simulazioni
last_cleanup = time.time()

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ----------------------------------------------------
# LOGGING
# ----------------------------------------------------
def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ----------------------------------------------------
# NORMALIZZAZIONE INPUT
# ----------------------------------------------------
def normalize_nmea(raw_value):
    try:
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8", errors="ignore")

        raw_value = raw_value.strip()

        if raw_value.startswith("{"):
            try:
                data = json.loads(raw_value)
                if "fields" in data and "value" in data["fields"]:
                    return data["fields"]["value"].encode("ascii", "ignore").decode("ascii")
            except:
                pass

        if raw_value.startswith("!AIVDM"):
            return raw_value

        if "!" in raw_value:
            return raw_value[raw_value.find("!") :]

        return None
    except:
        return None

# ----------------------------------------------------
# CHECKSUM
# ----------------------------------------------------
def compute_checksum(nmea_str_no_checksum):
    content = nmea_str_no_checksum
    if content.startswith("!"):
        content = content[1:]
    return f"{reduce(operator.xor, (ord(c) for c in content), 0):02X}"

# ----------------------------------------------------
# ETA parsing
# ----------------------------------------------------
def calculate_eta_timestamp(decoded):
    try:
        def get(key, d=0):
            v = decoded.get(key, d)
            try:
                return int(v)
            except:
                return d

        month = get("eta_month")
        day = get("eta_day")
        hour = get("eta_hour", 24)
        minute = get("eta_minute", 60)

        if month == 0:
            month = get("month")
        if day == 0:
            day = get("day")
        if hour == 24:
            hour = get("hour", 24)
        if minute == 60:
            minute = get("minute", 60)

        if not (1 <= month <= 12 and 1 <= day <= 31):
            return None

        if hour >= 24:
            hour = 0
        if minute >= 60:
            minute = 0

        now = datetime.datetime.now()
        year = now.year
        if now.month == 12 and month == 1:
            year += 1

        dt_eta = datetime.datetime(year, month, day, hour, minute)
        return dt_eta.timestamp()
    except:
        return None

# ----------------------------------------------------
# ETA ATTESA DA API (REALE)
# ----------------------------------------------------
def get_expected_eta_from_api(mmsi):
    try:
        url = f"{API_BASE}/vascello/{mmsi}/percorso_attivo"
        r = requests.get(url, timeout=3)

        if r.status_code != 200:
            return None

        data = r.json()
        percorso = data.get("percorso")
        if not percorso:
            return None

        partenza = percorso.get("orario_partenza_schedulato")
        durata_min = percorso.get("tempo_percorrenza")

        if not partenza or durata_min is None:
            return None

        dt_partenza = dt.fromisoformat(partenza)
        eta_prevista = dt_partenza + timedelta(minutes=float(durata_min))

        return eta_prevista.timestamp()

    except Exception as e:
        log(f"[API ERROR] {e}")
        return None

# ----------------------------------------------------
# ETA ATTESA SIMULAZIONE
# ----------------------------------------------------
def get_simulation_expected_eta(mmsi, start_ts):
    try:
        url = f"{API_BASE}/vascello/{mmsi}/percorso_attivo"
        r = requests.get(url, timeout=3)

        if r.status_code != 200:
            return None

        data = r.json()
        percorso = data.get("percorso")
        if not percorso:
            return None

        durata_min = percorso.get("tempo_percorrenza")
        if durata_min is None:
            return None
            
        print("durata min", durata_min)
        print("start timestamp", start_ts)

        return start_ts + float(durata_min) * 60

    except Exception as e:
        log(f"[SIM API ERROR] {e}")
        return None

# ----------------------------------------------------
# MULTIPART AIS HANDLER
# ----------------------------------------------------
def handle_multipart(topic, parts):
    try:
        total = int(parts[1])
        index = int(parts[2])
        seq = parts[3] or "0"
        chan = parts[4]
        payload = parts[5]

        key = (topic, chan, seq)

        entry = multipart_buffer.setdefault(
            key, {"total": total, "parts": {}, "ts": time.time()}
        )

        entry["parts"][index] = payload
        entry["ts"] = time.time()

        if len(entry["parts"]) == total:
            full = "".join(entry["parts"][i] for i in range(1, total + 1))
            del multipart_buffer[key]

            body = f"AIVDM,1,1,,{chan},{full},0"
            chk = compute_checksum(body)
            return f"!{body}*{chk}"

    except Exception as e:
        log(f"[MULTIPART ERROR] {e}")

    return None

# ----------------------------------------------------
# MAIN PROCESSING LOOP
# ----------------------------------------------------
def processing_loop():
    global last_cleanup
    log("Worker analytics avviato")

    while True:
        # cleanup multipart buffer
        if time.time() - last_cleanup > 10:
            now = time.time()
            for k, v in list(multipart_buffer.items()):
                if now - v["ts"] > 5:
                    del multipart_buffer[k]
            last_cleanup = now

        try:
            msg = message_queue.get(timeout=1)
        except queue.Empty:
            continue

        topic = msg.topic
        nmea = normalize_nmea(msg.value)

        if not nmea or not nmea.startswith("!"):
            message_queue.task_done()
            continue

        final = None
        parts = nmea.split(",")

        try:
            if len(parts) > 5:
                total = int(parts[1])
                if total > 1:
                    final = handle_multipart(topic, parts)
                else:
                    final = nmea
        except:
            pass

        if not final:
            message_queue.task_done()
            continue

        try:
            decoded = ais_decode(final)
            data = decoded.asdict() if hasattr(decoded, "asdict") else dict(decoded)

            mmsi = str(data.get("mmsi"))
            if not mmsi:
                message_queue.task_done()
                continue

            # ghost logic invariata
            if mmsi.startswith("50"):
                real_mmsi = mmsi[2:]
                is_ghost = True
            else:
                real_mmsi = mmsi
                is_ghost = False

            ship = ships_db.setdefault(mmsi, {"mmsi": mmsi})
            ship.update(data)
            ship["is_ghost"] = is_ghost
            ship["real_mmsi"] = real_mmsi

            # ETA AIS
            eta = calculate_eta_timestamp(data)
            if eta is not None:
                ship["eta"] = eta

            if ship["is_ghost"] or eta is None:
                message_queue.task_done()
                continue

            # --------------------------------------------------
            # DIFFERENZIAZIONE PER TOPIC
            # --------------------------------------------------
            if topic == MAIN_TOPIC:
                expected_eta = get_expected_eta_from_api(mmsi)

            elif topic == SIM_TOPIC:
                sim = simulation_state.get(mmsi)

                if not sim:
                    start_ts = time.time()
                    expected_eta = get_simulation_expected_eta(mmsi, start_ts)
                    print("expected_eta",expected_eta)
                    if expected_eta is None:
                        message_queue.task_done()
                        continue

                    simulation_state[mmsi] = {
                        "start_ts": start_ts,
                        "expected_eta": expected_eta,
                    }
                else:
                    expected_eta = sim["expected_eta"]
            else:
                message_queue.task_done()
                continue

            if expected_eta is None:
                message_queue.task_done()
                continue

            delta_min = (eta - expected_eta) / 60

            producer.send(
                ANALYTICS_TOPIC,
                {
                    "type": "delta_eta",
                    "mmsi": mmsi,
                    "delta_min": delta_min,
                    "destination": ship.get("destination", "UNKNOWN"),
                    "eta": eta,
                    "eta_expected": expected_eta,
                    "timestamp": time.time(),
                },
            )

        except Exception as e:
            log(f"Decode error: {e}")

        message_queue.task_done()

# ----------------------------------------------------
# KAFKA WORKER
# ----------------------------------------------------
def kafka_worker():
    log(f"Connessione Kafka: {BOOTSTRAP_SERVERS}")
    consumer = None

    while not consumer:
        try:
            consumer = KafkaConsumer(
                *ALL_TOPICS,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: x,
                auto_offset_reset="latest",
            )
            log("Kafka connesso")
        except:
            time.sleep(2)

    for msg in consumer:
        message_queue.put(msg)

# ----------------------------------------------------
# RUN
# ----------------------------------------------------
if __name__ == "__main__":
    threading.Thread(target=kafka_worker, daemon=True).start()
    threading.Thread(target=processing_loop, daemon=True).start()

    log("Analytics engine running with real + simulation AIS")
    while True:
        time.sleep(1)
