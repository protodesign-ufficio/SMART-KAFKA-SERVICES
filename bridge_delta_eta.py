"""
bridge_delta_eta.py
-------------------

FastStream worker che calcola la differenza tra ETA osservata via AIS e l'ETA
attesa (delta in minuti). Pubblica eventi `delta_eta` su `analytics_ais.raw`.

Le annotazioni Pydantic servono esclusivamente a rendere la documentazione
AsyncAPI leggibile; la logica runtime non viene alterata.
"""

import asyncio
import json
import os
import time
import datetime
import operator
from functools import reduce
from typing import Dict, Literal, Optional, Tuple

import requests
from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from pydantic import BaseModel, Field
from pyais import decode as ais_decode
from datetime import datetime as dt, timedelta
import logging

logging.getLogger("faststream").setLevel(logging.WARNING)


# ====================================================
# CONFIG
# ====================================================

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

MAIN_TOPIC = "ais.raw"
SIM_TOPIC = "ais_simulation.raw"

ANALYTICS_TOPIC = "analytics_ais.raw"
API_BASE = "http://87.26.178.190:15080"

# ====================================================
# FASTSTREAM
# ====================================================

broker = KafkaBroker(BOOTSTRAP_SERVERS)
app = FastStream(broker)

# ====================================================
# STATE
# ====================================================

state_lock = asyncio.Lock()

# CHIAVE STATO: (topic_sorgente, mmsi)
ShipKey = Tuple[str, str]

ships_db: Dict[ShipKey, dict] = {}
multipart_buffer: Dict[tuple, dict] = {}

# anche la simulation_state diventa per-(topic,mmsi) (così è coerente e future-proof)
simulation_state: Dict[ShipKey, dict] = {}

last_cleanup = time.time()

# ====================================================
# ASYNCAPI SCHEMA (documentazione leggibile)
# ====================================================

class DeltaEtaEvent(BaseModel):
    type: Literal["delta_eta"] = Field("delta_eta", description="Tipo evento")
    mmsi: str = Field(..., description="MMSI nave (come ricevuto da AIS)")
    delta_min: float = Field(..., description="Delta ETA = ETA AIS - ETA attesa (minuti)")
    destination: str = Field(..., description="Destinazione AIS (normalizzata o UNKNOWN)")
    eta: float = Field(..., description="ETA AIS in Unix timestamp (secondi)")
    eta_expected: float = Field(..., description="ETA attesa in Unix timestamp (secondi)")
    source: Literal["real", "simulation"] = Field(..., description="Origine del messaggio AIS")
    timestamp: float = Field(..., description="Timestamp evento (Unix time, secondi)")
    """Esempio:
    {
        "type": "delta_eta",
        "mmsi": "123456789",
        "delta_min": -5.0,
        "destination": "PORTO X",
        "eta": 1670000000.0,
        "eta_expected": 1670000300.0,
        "source": "real",
        "timestamp": 1670000100.0
    }
    """

@broker.publisher(ANALYTICS_TOPIC)
async def _doc_delta_eta() -> DeltaEtaEvent:
    ...

# ====================================================
# UTILS
# ====================================================

def normalize_nmea(raw_value) -> Optional[str]:
    try:
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8", errors="ignore")

        raw_value = raw_value.strip()

        if raw_value.startswith("{"):
            try:
                data = json.loads(raw_value)
                if "fields" in data and "value" in data["fields"]:
                    return data["fields"]["value"]
            except Exception:
                pass

        if raw_value.startswith("!AIVDM"):
            return raw_value

        if "!" in raw_value:
            return raw_value[raw_value.find("!") :]

        return None
    except Exception:
        return None


def compute_checksum(nmea_str_no_checksum: str) -> str:
    content = nmea_str_no_checksum[1:] if nmea_str_no_checksum.startswith("!") else nmea_str_no_checksum
    return f"{reduce(operator.xor, (ord(c) for c in content), 0):02X}"


def handle_multipart(topic: str, parts) -> Optional[str]:
    """
    Ricompone messaggi AIS multipart (AIVDM n>1) usando una bufferizzazione per (topic, channel, seq).
    """
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
            chk = compute_checksum(body)
            return f"!{body}*{chk}"
    except Exception:
        return None

    return None


def calculate_eta_timestamp(decoded: dict) -> Optional[float]:
    """
    Estrae ETA da campi AIS (eta_month/day/hour/minute) con fallback a month/day/hour/minute.
    Ritorna Unix timestamp (secondi) oppure None se non disponibile/valida.
    """
    try:
        def get_int_or_none(key: str) -> Optional[int]:
            if key not in decoded:
                return None
            v = decoded.get(key)
            try:
                return int(v)
            except Exception:
                return None

        month = get_int_or_none("eta_month")
        if month is None:
            month = get_int_or_none("month")

        day = get_int_or_none("eta_day")
        if day is None:
            day = get_int_or_none("day")

        hour = get_int_or_none("eta_hour")
        if hour is None:
            hour = get_int_or_none("hour")

        minute = get_int_or_none("eta_minute")
        if minute is None:
            minute = get_int_or_none("minute")

        # Require explicit values; no silent fallbacks to default values.
        if month is None or day is None or hour is None or minute is None:
            return None

        # Validate ranges strictly
        if not (1 <= month <= 12 and 1 <= day <= 31):
            return None
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None

        now = datetime.datetime.now()
        year = now.year + (1 if now.month == 12 and month == 1 else 0)

        return datetime.datetime(year, month, day, hour, minute).timestamp()
    except Exception:
        return None


def get_expected_eta_from_api(mmsi: str) -> Optional[float]:
    """
    ETA attesa "reale": ricavata da API con partenza schedulata + tempo_percorrenza.
    Seleziona il percorso con virtuale=false.
    """
    try:
        r = requests.get(f"{API_BASE}/vascello/{mmsi}/percorso_attivo", timeout=3)
        if r.status_code != 200:
            return None

        percorsi = r.json().get("percorsi", [])
        if not percorsi:
            return None

        # Cerca il percorso con virtuale=false (nave reale)
        percorso = None
        for p in percorsi:
            if p.get("assegnazione", {}).get("virtuale") is False:
                percorso = p.get("percorso")
                break

        if not percorso:
            return None

        partenza = percorso.get("orario_partenza_schedulato")
        durata_min = percorso.get("tempo_percorrenza")

        if not partenza or durata_min is None:
            return None

        return (dt.fromisoformat(partenza) + timedelta(minutes=float(durata_min))).timestamp()
    except Exception:
        return None


def get_simulation_expected_eta(mmsi: str, start_ts: float) -> Optional[float]:
    """
    ETA attesa "simulazione": start_ts + tempo_percorrenza (API) in minuti.
    Seleziona il percorso con virtuale=true.
    """
    try:
        r = requests.get(f"{API_BASE}/vascello/{mmsi}/percorso_attivo", timeout=3)
        if r.status_code != 200:
            return None

        percorsi = r.json().get("percorsi", [])
        if not percorsi:
            return None

        # Cerca il percorso con virtuale=true (simulazione)
        percorso = None
        for p in percorsi:
            if p.get("assegnazione", {}).get("virtuale") is True:
                percorso = p.get("percorso")
                break

        if not percorso:
            return None

        durata_min = percorso.get("tempo_percorrenza")
        if durata_min is None:
            return None

        return start_ts + float(durata_min) * 60
    except Exception:
        return None


def cleanup_multipart_buffer() -> None:
    """
    Elimina ricomposizioni multipart stale per evitare crescita indefinita.
    """
    global last_cleanup
    now = time.time()
    if now - last_cleanup <= 10:
        return

    for k, v in list(multipart_buffer.items()):
        if now - v["ts"] > 5:
            del multipart_buffer[k]
    last_cleanup = now


# ====================================================
# CORE PROCESSOR
# ====================================================

async def process_ais_message(msg: KafkaMessage, source: Literal["real", "simulation"]) -> None:
    cleanup_multipart_buffer()

    raw = normalize_nmea(msg.body)
    if not raw or not raw.startswith("!"):
        await msg.ack()
        return

    topic = getattr(msg, "topic", MAIN_TOPIC)

    parts = raw.split(",")
    final = raw

    # multipart
    if len(parts) > 5:
        try:
            total = int(parts[1])
            if total > 1:
                final = handle_multipart(topic, parts)
        except Exception:
            final = None

    if not final:
        await msg.ack()
        return

    try:
        decoded = ais_decode(final)
        data = decoded.asdict() if hasattr(decoded, "asdict") else dict(decoded)

        mmsi = str(data.get("mmsi") or "")
        if not mmsi:
            await msg.ack()
            return

        # ghost logic invariata
        if mmsi.startswith("50"):
            await msg.ack()
            return

        eta = calculate_eta_timestamp(data)
        print(f"[DELTA ETA] MMSI={mmsi} ETA={eta} SOURCE={source} TOPIC={topic}")
        if eta is None:
            await msg.ack()
            return
        print(f"[DELTA ETA] {data}")

        key: ShipKey = (topic, mmsi)

        # variabili locali da usare fuori lock (evitiamo di leggere ship dopo che altri thread lo mutano)
        destination_norm = "UNKNOWN"
        expected_eta: Optional[float] = None

        async with state_lock:
            ship = ships_db.setdefault(key, {"mmsi": mmsi, "topic": topic})
            ship.update(data)

            destination = ship.get("destination") or "UNKNOWN"
            if isinstance(destination, str):
                destination = " ".join(destination.strip().upper().split()) or "UNKNOWN"
            ship["destination"] = destination
            destination_norm = destination

            # calcolo expected ETA in base alla sorgente
            if source == "real":
                expected_eta = get_expected_eta_from_api(mmsi)
            else:
                sim = simulation_state.get(key)
                if not sim:
                    start_ts = time.time()
                    expected_eta = get_simulation_expected_eta(mmsi, start_ts)
                    if expected_eta is None:
                        await msg.ack()
                        return
                    simulation_state[key] = {"start_ts": start_ts, "expected_eta": expected_eta}
                else:
                    expected_eta = sim["expected_eta"]

        if expected_eta is None:
            await msg.ack()
            return

        delta_min = (eta - expected_eta) / 60.0

        event = DeltaEtaEvent(
            mmsi=mmsi,
            delta_min=delta_min,
            destination=destination_norm,
            eta=eta,
            eta_expected=expected_eta,
            source=source,
            timestamp=time.time(),
        )
        #print(f"[DELTA ETA] {event.json()}")

        await broker.publish(event, topic=ANALYTICS_TOPIC)
        await msg.ack()

    except Exception as e:
        print("[DELTA ETA ERROR]", e)
        await msg.nack()


# ====================================================
# SUBSCRIBERS
# ====================================================

@broker.subscriber(MAIN_TOPIC)
async def ais_consumer_real(msg: KafkaMessage):
    await process_ais_message(msg, source="real")


@broker.subscriber(SIM_TOPIC)
async def ais_consumer_sim(msg: KafkaMessage):
    await process_ais_message(msg, source="simulation")


# ====================================================
# STARTUP
# ====================================================

@app.on_startup
async def startup():
    print("[delta-eta] FastStream worker started (real + simulation)")
