import asyncio
import json
import os
import time
from typing import Dict, Literal, Optional

from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from pydantic import BaseModel, Field
from pyais import decode as ais_decode

from config_loader import load_kafka_config_from_dashboard
import logging

logging.getLogger("faststream").setLevel(logging.WARNING)


# ====================================================
# CONFIG INIZIALE
# ====================================================

config = load_kafka_config_from_dashboard()

PUBLISH_INTERVAL_SEC: int = int(config["publish_interval"])
CONFIG_LAST_UPDATE: float = float(config.get("last_update", time.time()))

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "87.26.178.190:29092")

MAIN_TOPIC = "ais.raw"
SIM_TOPIC = "ais_simulation.raw"

INPUT_TOPIC: str = str(config.get("kafka_topic", MAIN_TOPIC))
OUTPUT_TOPIC = "analytics_ais.raw"

COMPONENTS = ["engine_main", "generator", "gearbox"]

# ====================================================
# FASTSTREAM
# ====================================================

broker = KafkaBroker(BOOTSTRAP_SERVERS)
app = FastStream(broker)

# ====================================================
# STATE
# ====================================================

state_lock = asyncio.Lock()
ships: Dict[str, dict] = {}

# topic attivo (LOGICO, NON dinamico)
_active_topic: str = INPUT_TOPIC

# ====================================================
# ASYNCAPI SCHEMA
# ====================================================

class ComponentUsageEvent(BaseModel):
    type: Literal["component_usage"] = Field("component_usage", description="Tipo evento")
    mmsi: str = Field(..., description="MMSI nave")
    component: str = Field(..., description="Nome componente")
    usage_seconds_total: int = Field(..., description="Secondi totali di utilizzo")
    active: bool = Field(..., description="Componente attivo")
    timestamp: float = Field(..., description="Timestamp evento")

@broker.publisher(OUTPUT_TOPIC)
async def _doc_component_usage() -> ComponentUsageEvent:
    ...

# ====================================================
# DOMAIN LOGIC
# ====================================================

def is_component_active(component: str, ais_state: dict) -> bool:
    sog = ais_state.get("speed")
    return sog is not None and sog > 0.1


def update_component_usage(ship: dict, now: float) -> None:
    last_ts = ship["last_update_ts"]
    dt = now - last_ts
    ship["last_update_ts"] = now

    if dt <= 0:
        return

    for component, state in ship["components"].items():
        active = is_component_active(component, ship["ais"])
        if active:
            state["usage_total"] += dt
        state["active"] = active


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

# ====================================================
# CORE AIS PROCESSOR
# ====================================================

async def process_ais_message(raw_bytes: bytes) -> None:
    raw = normalize_nmea(raw_bytes)
    if not raw:
        return

    decoded = ais_decode(raw)
    data = decoded.asdict()

    if "speed" not in data:
        return

    mmsi = str(data.get("mmsi") or "")
    if not mmsi:
        return

    now = time.time()

    async with state_lock:
        ship = ships.setdefault(
            mmsi,
            {
                "ais": {},
                "last_update_ts": now,
                "components": {
                    c: {"usage_total": 0.0, "active": False}
                    for c in COMPONENTS
                },
            },
        )

        ship["ais"] = data
        update_component_usage(ship, now)

# ====================================================
# SUBSCRIBERS STATICI (FASTSTREAM-CORRETTI)
# ====================================================

@broker.subscriber(MAIN_TOPIC)
async def consume_main(msg: KafkaMessage):
    if _active_topic != MAIN_TOPIC:
        await msg.ack()
        return

    await process_ais_message(msg.body)
    await msg.ack()


@broker.subscriber(SIM_TOPIC)
async def consume_sim(msg: KafkaMessage):
    if _active_topic != SIM_TOPIC:
        await msg.ack()
        return

    await process_ais_message(msg.body)
    await msg.ack()

# ====================================================
# PERIODIC PUBLISHER
# ====================================================

async def publish_loop():
    while True:
        await asyncio.sleep(PUBLISH_INTERVAL_SEC)
        now = time.time()

        async with state_lock:
            snapshot = {
                mmsi: {
                    c: dict(state)
                    for c, state in ship["components"].items()
                }
                for mmsi, ship in ships.items()
            }

        for mmsi, components in snapshot.items():
            for component, state in components.items():
                event = ComponentUsageEvent(
                    mmsi=mmsi,
                    component=component,
                    usage_seconds_total=int(state["usage_total"]),
                    active=state["active"],
                    timestamp=now,
                )

                await broker.publish(event, topic=OUTPUT_TOPIC)

# ====================================================
# CONFIG WATCHER (CAMBIO TOPIC LOGICO)
# ====================================================

async def config_watcher():
    global PUBLISH_INTERVAL_SEC, CONFIG_LAST_UPDATE, _active_topic

    while True:
        await asyncio.sleep(120)

        new_config = load_kafka_config_from_dashboard()
        last_update = float(new_config.get("last_update", 0))

        if last_update > CONFIG_LAST_UPDATE:
            print(
                f"[CONFIG] update: "
                f"PUBLISH_INTERVAL {PUBLISH_INTERVAL_SEC} -> {new_config['publish_interval']}, "
                f"TOPIC {_active_topic} -> {new_config['kafka_topic']}"
            )

            PUBLISH_INTERVAL_SEC = int(new_config["publish_interval"])
            CONFIG_LAST_UPDATE = last_update

            new_topic = str(new_config["kafka_topic"])
            if new_topic != _active_topic:
                print(f"[KAFKA] active topic {_active_topic} -> {new_topic}")
                _active_topic = new_topic

# ====================================================
# STARTUP
# ====================================================

@app.on_startup
async def startup():
    print("[component-usage] FastStream worker started (static subscribers)")
    asyncio.create_task(publish_loop())
    asyncio.create_task(config_watcher())
