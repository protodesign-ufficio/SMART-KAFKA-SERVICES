"""
bridge_components.py
--------------------

FastStream worker che valuta l'utilizzo dei componenti macchina (engine, generator, gearbox)
partendo dai messaggi AIS decodificati.

Questo modulo espone Pydantic models usati per generare AsyncAPI:
- `ComponentUsageEvent` -> payload pubblicato su `analytics_ais.raw`

Le funzioni `@broker.publisher` sono stub per la generazione della documentazione
AsyncAPI e non contengono logica di pubblicazione aggiuntiva.
"""

import asyncio
import json
import os
import time
from typing import Dict, Literal, Optional, Tuple

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

# CHIAVE STATO: (source_topic, mmsi)
ShipKey = Tuple[str, str]
ships: Dict[ShipKey, dict] = {}

# ====================================================
# ASYNCAPI SCHEMA
# ====================================================

class ComponentUsageEvent(BaseModel):
    type: Literal["component_usage"] = Field("component_usage", description="Tipo evento")
    mmsi: str = Field(..., description="MMSI nave")
    component: str = Field(..., description="Nome componente")
    usage_seconds_total: int = Field(..., description="Secondi totali di utilizzo")
    active: bool = Field(..., description="Componente attivo")
    source: str = Field(..., description="Data source topic (ais.raw | ais_simulation.raw)")
    timestamp: float = Field(..., description="Timestamp evento")
    """Esempio payload per `analytics_ais.raw`:
    {
        "type": "component_usage",
        "mmsi": "123456789",
        "component": "engine_main",
        "usage_seconds_total": 120,
        "active": true,
        "source": "ais.raw",
        "timestamp": 1670000100.0
    }
    """

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

async def process_ais_message(topic: str, raw_bytes: bytes) -> None:
    """Processa un messaggio AIS grezzo.

    DIFFERENZA CHIAVE: lo stato è indicizzato da (topic_sorgente, mmsi)
    così reale e simulato con stesso MMSI restano separati.
    """
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

    key: ShipKey = (topic, mmsi)

    async with state_lock:
        ship = ships.setdefault(
            key,
            {
                "ais": {},
                "last_update_ts": now,
                "components": {c: {"usage_total": 0.0, "active": False} for c in COMPONENTS},
                "source": topic,  # ora è stabile, perché la key include il topic
                "mmsi": mmsi,
            },
        )

        ship["ais"] = data
        update_component_usage(ship, now)

# ====================================================
# SUBSCRIBERS STATICI (FASTSTREAM-CORRETTI)
# ====================================================

@broker.subscriber(MAIN_TOPIC)
async def consume_main(msg: KafkaMessage):
    await process_ais_message(MAIN_TOPIC, msg.body)
    await msg.ack()


@broker.subscriber(SIM_TOPIC)
async def consume_sim(msg: KafkaMessage):
    await process_ais_message(SIM_TOPIC, msg.body)
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
                key: {
                    "mmsi": ship.get("mmsi"),
                    "components": {c: dict(state) for c, state in ship["components"].items()},
                    "source": ship.get("source"),
                }
                for key, ship in ships.items()
            }

        for key, data in snapshot.items():
            mmsi = data["mmsi"]
            components = data["components"]
            source = data.get("source") or key[0]

            for component, state in components.items():
                event = ComponentUsageEvent(
                    mmsi=mmsi,
                    component=component,
                    usage_seconds_total=int(state["usage_total"]),
                    active=state["active"],
                    source=source,
                    timestamp=now,
                )

                await broker.publish(event, topic=OUTPUT_TOPIC)

# ====================================================
# CONFIG WATCHER (CAMBIO TOPIC LOGICO)
# ====================================================

async def config_watcher():
    global PUBLISH_INTERVAL_SEC, CONFIG_LAST_UPDATE

    while True:
        await asyncio.sleep(120)

        new_config = load_kafka_config_from_dashboard()
        last_update = float(new_config.get("last_update", 0))

        if last_update > CONFIG_LAST_UPDATE:
            print(
                f"[CONFIG] update: "
                f"PUBLISH_INTERVAL {PUBLISH_INTERVAL_SEC} -> {new_config['publish_interval']}, "
            )

            PUBLISH_INTERVAL_SEC = int(new_config["publish_interval"])
            CONFIG_LAST_UPDATE = last_update

# ====================================================
# STARTUP
# ====================================================

@app.on_startup
async def startup():
    print("[component-usage] FastStream worker started (static subscribers)")
    asyncio.create_task(publish_loop())
    asyncio.create_task(config_watcher())
