import json
import threading
import time
import queue
import datetime
import operator
from functools import reduce
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from pyais import decode as ais_decode
import os
from config_loader import load_kafka_config_from_dashboard

config = load_kafka_config_from_dashboard()
WINDOW_FUTURE_MIN = config["window_future"]
PUBLISH_INTERVAL = config["publish_interval"]
CONFIG_LAST_UPDATE = config.get("last_update", time.time())
KAFKA_TOPIC = config["kafka_topic"]
# Il thread di polling parte automaticamente!
# ----------------------------------------------------
# CONFIG
# ----------------------------------------------------

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

MAIN_TOPIC = KAFKA_TOPIC
ALL_TOPICS = [MAIN_TOPIC]

ANALYTICS_TOPIC = "analytics_ais.raw"

# WINDOW_FUTURE_MIN = 30
# PUBLISH_INTERVAL = 30  # seconds

# ----------------------------------------------------
# STATE
# ----------------------------------------------------
message_queue = queue.Queue(maxsize=10000)
multipart_buffer = {}
last_cleanup = time.time()

ships_db = {}                      # mmsi -> last decoded state
berths = defaultdict(dict)         # destination -> { mmsi -> eta }

# ----------------------------------------------------
# KAFKA PRODUCER
# ----------------------------------------------------
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
# MULTIPART HANDLER
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
# ETA PARSER
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

        if month == 0: month = get("month")
        if day == 0: day = get("day")
        if hour == 24: hour = get("hour", 24)
        if minute == 60: minute = get("minute", 60)

        if not (1 <= month <= 12 and 1 <= day <= 31):
            return None

        if hour >= 24: hour = 0
        if minute >= 60: minute = 0

        now = datetime.datetime.now()
        year = now.year
        if now.month == 12 and month == 1:
            year += 1

        dt = datetime.datetime(year, month, day, hour, minute)
        return dt.timestamp()
    except:
        return None

# ----------------------------------------------------
# PROCESSING LOOP
# ----------------------------------------------------
def processing_loop():
    global last_cleanup
    log("Berth incoming analytics worker started")

    while True:
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

        nmea = normalize_nmea(msg.value)
        if not nmea or not nmea.startswith("!"):
            message_queue.task_done()
            continue

        final = None
        parts = nmea.split(",")

        try:
            if len(parts) > 5 and int(parts[1]) > 1:
                final = handle_multipart(msg.topic, parts)
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

            mmsi = str(data.get("mmsi") or "")
            if not mmsi or mmsi.startswith("50"):
                message_queue.task_done()
                continue

            ship = ships_db.setdefault(mmsi, {"mmsi": mmsi})
            ship.update(data)

            destination = ship.get("destination")
            if destination:
                destination = " ".join(destination.strip().upper().split())

            eta = calculate_eta_timestamp(data)
            if eta is not None:
                ship["eta"] = eta

            if destination and ship.get("eta") is not None:
                berths[destination][mmsi] = ship["eta"]

        except Exception as e:
            log(f"Decode error: {e}")

        message_queue.task_done()

# ----------------------------------------------------
# PUBLISHER LOOP
# ----------------------------------------------------
def publisher_loop():
    log("Berth incoming publisher started")

    while True:
        now = time.time()
        horizon = now + WINDOW_FUTURE_MIN * 60

        for destination, ships in list(berths.items()):
            incoming = [
                {"mmsi": mmsi, "eta": eta}
                for mmsi, eta in ships.items()
                if now < eta <= horizon
            ]

            incoming.sort(key=lambda x: x["eta"])

            event = {
                "type": "berth_incoming",
                "destination": destination,
                "window_future_min": WINDOW_FUTURE_MIN,
                "incoming_vessels": len(incoming),
                "incoming": incoming,
                "timestamp": now
            }

            producer.send(ANALYTICS_TOPIC, event)

        time.sleep(PUBLISH_INTERVAL)

# ----------------------------------------------------
# KAFKA WORKER
# ----------------------------------------------------
def kafka_worker():
    log(f"Connessione Kafka: {BOOTSTRAP_SERVERS}")
    consumer = None

    while not consumer:
        MAIN_TOPIC = KAFKA_TOPIC
        ALL_TOPICS = [MAIN_TOPIC]
        print(ALL_TOPICS)
        try:
            consumer = KafkaConsumer(
                *ALL_TOPICS,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: x,
                auto_offset_reset="latest"
            )
            log("Kafka connesso")
        except:
            time.sleep(2)

    for msg in consumer:
        message_queue.put(msg)
        
        
# ----------------------------------------------------
# AUTO-RELOAD CONFIGURAZIONI
# ----------------------------------------------------      
        
        
        
def periodic_config_check():
    global WINDOW_FUTURE_MIN, PUBLISH_INTERVAL, CONFIG_LAST_UPDATE, KAFKA_TOPIC

    new_config = load_kafka_config_from_dashboard()
    print(new_config.get("last_update"))

    if new_config.get("last_update", 0) > CONFIG_LAST_UPDATE:
        log(
            f"[CONFIG] update: "
            f"WINDOW_FUTURE_MIN {WINDOW_FUTURE_MIN} -> {new_config['window_future']}, "
            f"PUBLISH_INTERVAL {PUBLISH_INTERVAL} -> {new_config['publish_interval']}"
        )

        WINDOW_FUTURE_MIN = new_config["window_future"]
        PUBLISH_INTERVAL = new_config["publish_interval"]
        CONFIG_LAST_UPDATE = new_config["last_update"]
        KAFKA_TOPIC = new_config["kafka_topic"]

        
def config_update_thread():
    """
    Thread che controlla la configurazione ogni x secondi e aggiorna automaticamente
    le variabili globali se sono cambiate.
    
    Questo è il CORE del sistema di AUTO-UPDATE!
    Non è necessario riavviare il servizio Docker quando modifichi i parametri.
    """
    while True:
        time.sleep(120)  # Controlla ogni x secondi -> 2 minuti per ora
        try:
            periodic_config_check()
        except Exception as e:
            print(f"[CONFIG] Errore nel check periodico: {e}")



# ----------------------------------------------------
# RUN
# ----------------------------------------------------
if __name__ == "__main__":
    threading.Thread(target=kafka_worker, daemon=True).start()
    threading.Thread(target=processing_loop, daemon=True).start()
    threading.Thread(target=publisher_loop, daemon=True).start()
    threading.Thread(target=config_update_thread, daemon=True).start()

    log("Berth incoming bridge running (with ordered incoming vessels)")
    while True:
        time.sleep(1)
