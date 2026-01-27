import asyncio
import datetime
import json
import operator
import os
import time
from collections import defaultdict
from functools import reduce
from typing import Optional, Dict, List, Any

from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from pyais import decode as ais_decode
from pydantic import BaseModel, Field

from config_loader import load_kafka_config_from_dashboard
import logging

logging.getLogger("faststream").setLevel(logging.WARNING)


# ====================================================
# CONFIG INIZIALE
# ====================================================

config = load_kafka_config_from_dashboard()

WINDOW_FUTURE_MIN: int = int(config["window_future"])
PUBLISH_INTERVAL: int = int(config["publish_interval"])
CONFIG_LAST_UPDATE: float = float(config.get("last_update", time.time()))
KAFKA_TOPIC: str = str(config["kafka_topic"])

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "87.26.178.190:29092")
ANALYTICS_TOPIC = "analytics_ais.raw"

MAIN_TOPIC = "ais.raw"
SIM_TOPIC = "ais_simulation.raw"

# ====================================================
# FASTSTREAM
# ====================================================

broker = KafkaBroker(BOOTSTRAP_SERVERS)
app = FastStream(broker)

# ====================================================
# MODELLI (AsyncAPI)
# ====================================================

class IncomingVessel(BaseModel):
    mmsi: str = Field(..., description="MMSI nave")
    eta: float = Field(..., description="ETA Unix timestamp")
    source: str = Field(..., description="Data source topic (ais.raw | ais_simulation.raw)")

class BerthIncomingEvent(BaseModel):
    type: str = Field("berth_incoming", description="Tipo evento")
    destination: str = Field(..., description="Destinazione / banchina")
    window_future_min: int = Field(..., description="Finestra temporale (min)")
    incoming_vessels: int = Field(..., description="Numero navi in arrivo")
    incoming: List[IncomingVessel] = Field(..., description="Lista navi in arrivo")
    timestamp: float = Field(..., description="Timestamp evento")

@broker.publisher(ANALYTICS_TOPIC)
async def _doc_berth_incoming() -> BerthIncomingEvent:
    ...

# ====================================================
# STATE
# ====================================================

state_lock = asyncio.Lock()

ships_db: Dict[str, dict] = {}
# berths[destination][mmsi] -> {"eta": float, "source": str}
berths: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)

multipart_buffer: Dict[tuple, dict] = {}
last_cleanup = time.time()


# ====================================================
# UTILS
# ====================================================

def log(msg: str) -> None:
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def normalize_nmea(raw_value) -> Optional[str]:
    try:
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8", errors="ignore")

        raw_value = raw_value.strip()

        if raw_value.startswith("{"):
            data = json.loads(raw_value)
            if "fields" in data and "value" in data["fields"]:
                return data["fields"]["value"]

        if raw_value.startswith("!AIVDM"):
            return raw_value

        if "!" in raw_value:
            return raw_value[raw_value.find("!") :]

        return None
    except Exception:
        return None


def compute_checksum(body: str) -> str:
    content = body[1:] if body.startswith("!") else body
    return f"{reduce(operator.xor, (ord(c) for c in content), 0):02X}"


def handle_multipart(topic: str, parts) -> Optional[str]:
    try:
        total = int(parts[1])
        index = int(parts[2])
        seq = parts[3] or "0"
        chan = parts[4]
        payload = parts[5]

        key = (topic, chan, seq)
        entry = multipart_buffer.setdefault(key, {"total": total, "parts": {}, "ts": time.time()})

        entry["parts"][index] = payload
        entry["ts"] = time.time()

        if len(entry["parts"]) == total:
            full = "".join(entry["parts"][i] for i in range(1, total + 1))
            del multipart_buffer[key]

            body = f"AIVDM,1,1,,{chan},{full},0"
            return f"!{body}*{compute_checksum(body)}"
    except Exception:
        return None

    return None


def calculate_eta_timestamp(decoded: dict) -> Optional[float]:
    try:
        def get_int(key, default=0):
            try:
                return int(decoded.get(key, default))
            except Exception:
                return default

        month = get_int("eta_month") or get_int("month")
        day = get_int("eta_day") or get_int("day")
        hour = get_int("eta_hour", 24)
        minute = get_int("eta_minute", 60)

        if not (1 <= month <= 12 and 1 <= day <= 31):
            return None

        hour = hour if hour < 24 else 0
        minute = minute if minute < 60 else 0

        now = datetime.datetime.now()
        year = now.year + (1 if now.month == 12 and month == 1 else 0)

        return datetime.datetime(year, month, day, hour, minute).timestamp()
    except Exception:
        return None


def cleanup_multipart() -> None:
    global last_cleanup
    now = time.time()
    if now - last_cleanup < 10:
        return

    for k, v in list(multipart_buffer.items()):
        if now - v["ts"] > 5:
            del multipart_buffer[k]

    last_cleanup = now

# ====================================================
# CORE PROCESSOR
# ====================================================

async def process_ais(topic: str, raw_bytes: bytes) -> None:
    cleanup_multipart()

    nmea = normalize_nmea(raw_bytes)
    if not nmea or not nmea.startswith("!"):
        return

    parts = nmea.split(",")
    final = nmea

    if len(parts) > 5 and int(parts[1]) > 1:
        final = handle_multipart(topic, parts)

    if not final:
        return

    decoded = ais_decode(final)
    data = decoded.asdict() if hasattr(decoded, "asdict") else dict(decoded)

    mmsi = str(data.get("mmsi") or "")
    if not mmsi or mmsi.startswith("50"):
        return

    destination = data.get("destination")
    if destination:
        destination = " ".join(destination.strip().upper().split())

    eta = calculate_eta_timestamp(data)

    async with state_lock:
        ship = ships_db.setdefault(mmsi, {})
        ship.update(data)

        if eta is not None:
            ship["eta"] = eta

            if destination and ship.get("eta") is not None:
                berths[destination][mmsi] = {"eta": ship["eta"], "source": topic}

# ====================================================
# SUBSCRIBERS (STATICI, CORRETTI)
# ====================================================

@broker.subscriber(MAIN_TOPIC)
async def consume_main(msg: KafkaMessage):
    await process_ais(MAIN_TOPIC, msg.body)
    await msg.ack()


@broker.subscriber(SIM_TOPIC)
async def consume_sim(msg: KafkaMessage):
    await process_ais(SIM_TOPIC, msg.body)
    await msg.ack()

# ====================================================
# PUBLISHER LOOP
# ====================================================

async def publisher_loop():
    log("Berth incoming publisher started")

    while True:
        now = time.time()
        horizon = now + WINDOW_FUTURE_MIN * 60

        async with state_lock:
            snapshot = {d: dict(v) for d, v in berths.items()}

        for destination, ships in snapshot.items():
            incoming = [
                IncomingVessel(mmsi=mmsi, eta=info.get("eta"), source=info.get("source", MAIN_TOPIC))
                for mmsi, info in ships.items()
                if now < info.get("eta", 0) <= horizon
            ]

            incoming.sort(key=lambda x: x.eta)

            event = BerthIncomingEvent(
                destination=destination,
                window_future_min=WINDOW_FUTURE_MIN,
                incoming_vessels=len(incoming),
                incoming=incoming,
                timestamp=now,
            )

            await broker.publish(event, topic=ANALYTICS_TOPIC)

        await asyncio.sleep(PUBLISH_INTERVAL)

# ====================================================
# CONFIG WATCHER
# ====================================================

async def config_watcher():
    global WINDOW_FUTURE_MIN, PUBLISH_INTERVAL, CONFIG_LAST_UPDATE

    while True:
        await asyncio.sleep(120)

        new = load_kafka_config_from_dashboard()
        last = float(new.get("last_update", 0))

        if last > CONFIG_LAST_UPDATE:
            log("[CONFIG] reload")

            WINDOW_FUTURE_MIN = int(new["window_future"])
            PUBLISH_INTERVAL = int(new["publish_interval"])
            CONFIG_LAST_UPDATE = last

            # kafka_topic setting is ignored because both topics are consumed concurrently

# ====================================================
# STARTUP
# ====================================================

@app.on_startup
async def startup():
    log(f"Connessione Kafka: {BOOTSTRAP_SERVERS}")
    log("Berth incoming analytics worker started (FastStream)")

    asyncio.create_task(publisher_loop())
    asyncio.create_task(config_watcher())
