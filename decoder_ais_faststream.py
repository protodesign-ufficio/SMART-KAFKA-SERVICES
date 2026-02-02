"""
decoder_ais_faststream.py
-------------------------

Worker FastStream che consuma i topic AIS grezzi e pubblica
i messaggi decodificati sui rispettivi topic di output.

Topic gestiti:
- ais.raw -> ais_decoded.raw
- ais_simulation.raw -> ais_decoded_simulation.raw
"""

import asyncio
import datetime
import json
import operator
import os
import time
from functools import reduce
from typing import Optional, Dict, Any

from faststream import FastStream
from faststream.kafka import KafkaBroker
from pyais import decode as ais_decode
from pydantic import BaseModel, Field
import logging

logging.getLogger("faststream").setLevel(logging.WARNING)

# ====================================================
# CONFIG
# ====================================================

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")

# Topic di input/output
MAIN_INPUT_TOPIC = "ais.raw"
MAIN_OUTPUT_TOPIC = "ais_decoded.raw"

SIM_INPUT_TOPIC = "ais_simulation.raw"
SIM_OUTPUT_TOPIC = "ais_decoded_simulation.raw"

# ====================================================
# FASTSTREAM
# ====================================================

broker = KafkaBroker(BOOTSTRAP_SERVERS)
app = FastStream(broker)

# ====================================================
# MODELLI (AsyncAPI)
# ====================================================

class AisDecodedPayload(BaseModel):
    """Payload AIS decodificato con tutti i campi disponibili."""
    msg_type: Optional[int] = Field(None, description="Tipo messaggio AIS")
    mmsi: Optional[str] = Field(None, description="MMSI nave")
    # Altri campi dinamici nel payload

class AisDecodedEvent(BaseModel):
    """Evento AIS decodificato pubblicato sui topic di output."""
    type: str = Field("ais_decoded", description="Tipo evento")
    msg_type: Optional[int] = Field(None, description="Tipo messaggio AIS")
    mmsi: Optional[str] = Field(None, description="MMSI nave")
    payload: Dict[str, Any] = Field(..., description="Payload AIS decodificato completo")
    timestamp: float = Field(..., description="Timestamp evento Unix")
    source: str = Field(..., description="Topic sorgente (ais.raw | ais_simulation.raw)")
    """Esempio:
    {
        "type": "ais_decoded",
        "msg_type": 1,
        "mmsi": "123456789",
        "payload": {
            "msg_type": 1,
            "mmsi": "123456789",
            "status": 0,
            "turn": 0,
            "speed": 10.5,
            "accuracy": true,
            "lon": 9.123456,
            "lat": 44.123456,
            "course": 180.0,
            "heading": 180,
            "second": 30,
            "maneuver": 0,
            "raim": false,
            "radio": 0
        },
        "timestamp": 1670000000.0,
        "source": "ais.raw"
    }
    """

# Publisher stubs per AsyncAPI docs
@broker.publisher(MAIN_OUTPUT_TOPIC)
async def _doc_ais_decoded_main() -> AisDecodedEvent:
    ...

@broker.publisher(SIM_OUTPUT_TOPIC)
async def _doc_ais_decoded_sim() -> AisDecodedEvent:
    ...

# ====================================================
# STATE
# ====================================================

state_lock = asyncio.Lock()

# Buffer separati per messaggi multipart per ogni topic
multipart_buffer: Dict[tuple, dict] = {}
last_cleanup = time.time()

# ====================================================
# UTILS
# ====================================================

def log(msg: str) -> None:
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def normalize_nmea(raw_value) -> Optional[str]:
    """Normalizza il messaggio NMEA grezzo in formato standard."""
    try:
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8", errors="ignore")

        raw_value = raw_value.strip()

        # JSON wrapper (es. da Kafka Connect)
        if raw_value.startswith("{"):
            try:
                data = json.loads(raw_value)
                if "fields" in data and "value" in data["fields"]:
                    return data["fields"]["value"].encode("ascii", "ignore").decode("ascii")
            except:
                pass

        # Messaggio AIVDM diretto
        if raw_value.startswith("!AIVDM"):
            return raw_value

        # Cerca il marker '!' nel messaggio
        if "!" in raw_value:
            return raw_value[raw_value.find("!"):]

        return None
    except Exception:
        return None


def compute_checksum(body: str) -> str:
    """Calcola il checksum NMEA."""
    content = body[1:] if body.startswith("!") else body
    return f"{reduce(operator.xor, (ord(c) for c in content), 0):02X}"


def handle_multipart(topic: str, parts: list) -> Optional[str]:
    """Gestisce i messaggi AIS multipart."""
    global last_cleanup
    
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
            return f"!{body}*{compute_checksum(body)}"

    except Exception as e:
        log(f"[MULTIPART ERROR] {e}")

    return None


async def cleanup_multipart_buffer():
    """Pulizia periodica del buffer multipart."""
    global last_cleanup
    
    now = time.time()
    if now - last_cleanup > 10:
        async with state_lock:
            for k, v in list(multipart_buffer.items()):
                if now - v["ts"] > 5:
                    del multipart_buffer[k]
        last_cleanup = now


async def decode_and_publish(raw_value, source_topic: str, output_topic: str) -> None:
    """Decodifica il messaggio AIS e pubblica sul topic di output."""
    
    await cleanup_multipart_buffer()
    
    nmea = normalize_nmea(raw_value)
    
    if not nmea or not nmea.startswith("!"):
        return

    final = None
    parts = nmea.split(",")

    try:
        # Verifica se è un messaggio multipart
        if len(parts) > 5 and int(parts[1]) > 1:
            async with state_lock:
                final = handle_multipart(source_topic, parts)
        else:
            final = nmea
    except Exception:
        pass

    if not final:
        return

    try:
        decoded = ais_decode(final)
        data = decoded.asdict() if hasattr(decoded, "asdict") else dict(decoded)

        event = {
            "type": "ais_decoded",
            "msg_type": data.get("msg_type"),
            "mmsi": data.get("mmsi"),
            "payload": data,
            "timestamp": time.time(),
            "source": source_topic
        }

        await broker.publish(event, topic=output_topic)

    except Exception as e:
        log(f"[DECODE ERROR] {e}")


# ====================================================
# SUBSCRIBERS
# ====================================================

@broker.subscriber(MAIN_INPUT_TOPIC)
async def handle_main_ais(msg):
    """
    Consuma messaggi AIS dal topic principale `ais.raw`
    e pubblica i messaggi decodificati su `ais_decoded.raw`.
    """
    await decode_and_publish(msg, MAIN_INPUT_TOPIC, MAIN_OUTPUT_TOPIC)


@broker.subscriber(SIM_INPUT_TOPIC)
async def handle_sim_ais(msg):
    """
    Consuma messaggi AIS dal topic simulazione `ais_simulation.raw`
    e pubblica i messaggi decodificati su `ais_decoded_simulation.raw`.
    """
    await decode_and_publish(msg, SIM_INPUT_TOPIC, SIM_OUTPUT_TOPIC)


# ====================================================
# STARTUP / SHUTDOWN
# ====================================================

@app.on_startup
async def on_startup():
    log("AIS Decoder FastStream starting...")
    log(f"Kafka: {BOOTSTRAP_SERVERS}")
    log(f"Input topics: {MAIN_INPUT_TOPIC}, {SIM_INPUT_TOPIC}")
    log(f"Output topics: {MAIN_OUTPUT_TOPIC}, {SIM_OUTPUT_TOPIC}")


@app.after_startup
async def after_startup():
    log("AIS Decoder FastStream ready!")


@app.on_shutdown
async def on_shutdown():
    log("AIS Decoder FastStream shutting down...")


# ====================================================
# RUN
# ====================================================

if __name__ == "__main__":
    asyncio.run(app.run())
