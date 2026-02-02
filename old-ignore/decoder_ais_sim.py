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

# ----------------------------------------------------
# CONFIG
# ----------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")

INPUT_TOPIC = "ais_simulation.raw"
OUTPUT_TOPIC = "ais_decoded_simulation.raw"

# ----------------------------------------------------
# STATE
# ----------------------------------------------------
message_queue = queue.Queue(maxsize=10000)
multipart_buffer = {}
last_cleanup = time.time()

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
# NORMALIZZAZIONE INPUT (IDENTICA AGLI ALTRI)
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
def handle_multipart(parts):
    try:
        total = int(parts[1])
        index = int(parts[2])
        seq = parts[3] or "0"
        chan = parts[4]
        payload = parts[5]

        key = (chan, seq)

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
# PROCESSING LOOP
# ----------------------------------------------------
def processing_loop():
    global last_cleanup
    log("AIS decoded bridge started")

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

        nmea = normalize_nmea(msg.value)

        if not nmea or not nmea.startswith("!"):
            message_queue.task_done()
            continue

        final = None
        parts = nmea.split(",")

        try:
            if len(parts) > 5 and int(parts[1]) > 1:
                final = handle_multipart(parts)
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

            event = {
                "type": "ais_decoded",
                "msg_type": data.get("msg_type"),
                "mmsi": data.get("mmsi"),
                "payload": data,
                "timestamp": time.time()
            }
            #print(event)
            producer.send(OUTPUT_TOPIC, event)

        except Exception as e:
            log(f"[DECODE ERROR] {e}")

        message_queue.task_done()

# ----------------------------------------------------
# KAFKA WORKER
# ----------------------------------------------------
def kafka_worker():
    log(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}")
    consumer = None

    while not consumer:
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="latest",
                value_deserializer=lambda x: x
            )
            log("Kafka connected")
        except:
            time.sleep(2)

    for msg in consumer:
        log(f"[RECEIVED] {msg.value[:100] if msg.value else 'empty'}...")
        message_queue.put(msg)

# ----------------------------------------------------
# RUN
# ----------------------------------------------------
if __name__ == "__main__":
    threading.Thread(target=kafka_worker, daemon=True).start()
    threading.Thread(target=processing_loop, daemon=True).start()

    log("AIS decoded bridge running")
    while True:
        time.sleep(1)
