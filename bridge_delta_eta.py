"""
Bridge Delta ETA - Worker Analytics Scostamento Orario
=======================================================

Descrizione
-----------
Questo modulo implementa un worker FastStream che calcola la differenza
(delta) tra l'ETA osservata nei messaggi AIS e l'ETA attesa/schedulata
recuperata dal sistema di backend.

Funzionalità Principale
-----------------------
Confronta l'ETA dichiarata dalla nave (campo AIS) con l'ETA attesa basata
sugli orari schedulati e il tempo di percorrenza previsto. Pubblica eventi
``delta_eta`` che indicano se la nave è in anticipo, puntuale o in ritardo.

Architettura del Flusso Dati
----------------------------
::

    ┌─────────────────────┐     ┌──────────────────┐
    │     ais.raw         │ ──► │                  │
    │  (NMEA grezzo)      │     │  Bridge Delta    │     ┌─────────────────────────┐
    └─────────────────────┘     │  ETA             │ ──► │   analytics_ais.raw     │
                                │                  │     │                         │
    ┌─────────────────────┐     │  - Parsing ETA   │     │  Eventi:                │
    │  ais_simulation.raw │ ──► │  - Query API     │     │  - delta_eta            │
    │  (NMEA grezzo)      │     │  - Calcolo delta │     │                         │
    └─────────────────────┘     └──────────────────┘     └─────────────────────────┘
                                        │
                                        ▼
                                ┌──────────────────┐
                                │  Backend API     │
                                │  /vascello/{mmsi}│
                                │  /percorso_attivo│
                                └──────────────────┘

Topic Kafka
-----------
**Input (Subscription):**
    - ``ais.raw``: Messaggi AIS reali in formato NMEA
    - ``ais_simulation.raw``: Messaggi AIS simulati in formato NMEA

**Output (Publishing):**
    - ``analytics_ais.raw``: Eventi analytics (tipo ``delta_eta``)

Calcolo Delta ETA
-----------------
Il delta è calcolato come::

    delta_min = (ETA_AIS - ETA_attesa) / 60

Dove:
- **ETA_AIS**: ETA dichiarata dalla nave nel messaggio AIS (tipo 5)
- **ETA_attesa**: ETA calcolata come ``orario_partenza_schedulato + tempo_percorrenza``

Interpretazione del delta:
- ``delta_min < 0``: Nave in anticipo (arriva prima del previsto)
- ``delta_min = 0``: Nave puntuale
- ``delta_min > 0``: Nave in ritardo (arriva dopo il previsto)

Gestione Real vs Simulation
---------------------------
Il sistema distingue tra navi reali e simulate:

**Navi Reali (source="real"):**
    - L'ETA attesa viene recuperata dal percorso con ``virtuale=false``
    - Basata su ``orario_partenza_schedulato + tempo_percorrenza``

**Navi Simulate (source="simulation"):**
    - L'ETA attesa viene calcolata al primo messaggio ricevuto
    - Formula: ``timestamp_primo_messaggio + (tempo_percorrenza / sim_speed_factor)``
    - Il percorso deve avere ``virtuale=true``
    - Il ``tempo_percorrenza`` viene scalato per ``SIM_SPEED_FACTOR`` (configurabile
      da backend) per tenere conto della velocità accelerata della simulazione

Cleanup Automatico
------------------
Le navi vengono rimosse dalla memoria quando non ricevono dati per
un tempo pari a 1/5 del tempo di percorrenza del loro percorso attivo.
Questo evita accumulo di memoria per navi che hanno terminato la navigazione.

Dipendenze
----------
- ``faststream``: Framework per streaming Kafka
- ``pyais``: Libreria per decodifica messaggi AIS
- ``pydantic``: Validazione e serializzazione dati
- ``requests``: Client HTTP per query API backend

Autore: Team AIS Analytics
Versione: 2.0.0
"""

# =============================================================================
# IMPORTS
# =============================================================================

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

# Riduce la verbosità dei log FastStream
logging.getLogger("faststream").setLevel(logging.WARNING)

# Import configurazione dinamica
from config_loader import load_kafka_config_from_dashboard


# =============================================================================
# CONFIGURAZIONE
# =============================================================================

def _parse_sim_speed_factor(value) -> float:
    """
    Converte in modo sicuro il valore sim_speed_factor.
    
    Gestisce None, 0, valori negativi e stringhe non valide,
    restituendo sempre un valore valido (>= 1.0 come fallback).
    """
    try:
        f = float(value) if value is not None else 1.0
        if f <= 0:
            print(f"[DELTA ETA CONFIG] ATTENZIONE: sim_speed_factor={f} non valido (<= 0), uso default 1.0")
            return 1.0
        return f
    except (TypeError, ValueError) as e:
        print(f"[DELTA ETA CONFIG] ATTENZIONE: sim_speed_factor='{value}' non parsabile ({e}), uso default 1.0")
        return 1.0


# Carica configurazione dal backend
_config = load_kafka_config_from_dashboard()
print(f"[DELTA ETA CONFIG] Configurazione raw ricevuta: sim_speed_factor={_config.get('sim_speed_factor', '<ASSENTE>')}")
SIM_SPEED_FACTOR = _parse_sim_speed_factor(_config.get("sim_speed_factor", 1.0))
"""float: Fattore di velocità simulazione (il tempo_percorrenza viene diviso per questo valore)"""

CONFIG_LAST_UPDATE: float = float(_config.get("last_update", time.time()))
"""float: Timestamp ultimo aggiornamento configurazione"""

print(f"[DELTA ETA CONFIG] SIM_SPEED_FACTOR caricato all'avvio: {SIM_SPEED_FACTOR}")

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
"""str: Indirizzo cluster Kafka"""

MAIN_TOPIC = "ais.raw"
"""str: Topic input per messaggi AIS reali"""

SIM_TOPIC = "ais_simulation.raw"
"""str: Topic input per messaggi AIS simulati"""

ANALYTICS_TOPIC = "analytics_ais.raw"
"""str: Topic output per eventi analytics"""

API_BASE = "http://87.26.178.190:25080"
"""str: URL base dell'API backend per recupero dati percorso"""


# =============================================================================
# INIZIALIZZAZIONE FASTSTREAM
# =============================================================================

broker = KafkaBroker(BOOTSTRAP_SERVERS)
"""KafkaBroker: Istanza del broker Kafka"""

app = FastStream(broker)
"""FastStream: Applicazione principale FastStream"""


# =============================================================================
# STATO GLOBALE
# =============================================================================

state_lock = asyncio.Lock()
"""asyncio.Lock: Lock per accesso thread-safe allo stato"""

ShipKey = Tuple[str, str]
"""Type alias: Chiave univoca nave = (topic_sorgente, mmsi)"""

ships_db: Dict[ShipKey, dict] = {}
"""
Dict[ShipKey, dict]: Database in-memory delle navi.

Struttura valore: dati AIS + campi calcolati (last_seen, tempo_percorrenza, etc.)
"""

multipart_buffer: Dict[tuple, dict] = {}
"""Dict[tuple, dict]: Buffer per ricomposizione messaggi NMEA multipart"""

simulation_state: Dict[ShipKey, dict] = {}
"""
Dict[ShipKey, dict]: Stato simulazioni (ETA attesa calcolata al primo messaggio).

Struttura valore:
{
    "start_ts": float,       # Timestamp primo messaggio ricevuto
    "expected_eta": float    # ETA attesa calcolata = start_ts + tempo_percorrenza
}
"""

last_cleanup = time.time()
"""float: Timestamp ultima pulizia buffer multipart"""

# =============================================================================
# MODELLI PYDANTIC (Schema AsyncAPI)
# =============================================================================

class DeltaEtaEvent(BaseModel):
    """
    Evento di scostamento ETA pubblicato su analytics_ais.raw.
    
    Rappresenta la differenza tra l'ETA dichiarata dalla nave (AIS)
    e l'ETA attesa basata sugli orari schedulati.

    Attributes
    ----------
    * `type` : Literal["delta_eta"] - Tipo evento, sempre "delta_eta"
    * `mmsi` : str - MMSI della nave
    * `delta_min` : float - Scostamento in minuti (< 0 anticipo, = 0 puntuale, > 0 ritardo)
    * `destination` : str - Destinazione AIS normalizzata (uppercase) o "UNKNOWN"
    * `eta` : float - ETA dichiarata dalla nave (Unix timestamp)
    * `eta_expected` : float - ETA attesa/schedulata (Unix timestamp)
    * `source` : Literal["real", "simulation"] - Origine del messaggio ("real" o "simulation")
    * `timestamp` : float - Timestamp Unix della generazione evento
    
    Examples
    --------
    Nave in anticipo di 5 minuti::
    
        {
            "type": "delta_eta",
            "mmsi": "123456789",
            "delta_min": -5.0,
            "destination": "SALERNO",
            "eta": 1670000000.0,
            "eta_expected": 1670000300.0,
            "source": "real",
            "sim_speed_factor": null,
            "timestamp": 1670000100.0
        }
    
    Nave simulata in ritardo di 10 minuti::
    
        {
            "type": "delta_eta",
            "mmsi": "987654321",
            "delta_min": 10.0,
            "destination": "POSITANO",
            "eta": 1670001200.0,
            "eta_expected": 1670000600.0,
            "source": "simulation",
            "sim_speed_factor": 2.0,
            "timestamp": 1670000500.0
        }
    """
    type: Literal["delta_eta"] = Field("delta_eta", description="Tipo evento")
    mmsi: str = Field(..., description="MMSI nave (9 cifre)")
    delta_min: float = Field(..., description="Delta = ETA_AIS - ETA_attesa (minuti)")
    destination: str = Field(..., description="Destinazione AIS (uppercase) o 'UNKNOWN'")
    eta: float = Field(..., description="ETA AIS (Unix timestamp)")
    eta_expected: float = Field(..., description="ETA attesa (Unix timestamp)")
    source: Literal["real", "simulation"] = Field(..., description="Origine: 'real' | 'simulation'")
    sim_speed_factor: Optional[float] = Field(None, description="Fattore velocità simulazione usato (None per navi reali)")
    timestamp: float = Field(..., description="Timestamp evento (Unix seconds)")


# -----------------------------------------------------------------------------
# Publisher Stub per documentazione AsyncAPI
# -----------------------------------------------------------------------------

@broker.publisher(ANALYTICS_TOPIC)
async def _doc_delta_eta() -> DeltaEtaEvent:
    """Publisher stub per documentazione AsyncAPI."""
    ...

# =============================================================================
# FUNZIONI UTILITY
# =============================================================================

def log(msg: str) -> None:
    """Log con timestamp e flush esplicito per evitare buffering nei container."""
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def normalize_nmea(raw_value) -> Optional[str]:
    """
    Normalizza un messaggio NMEA grezzo in formato standard AIVDM.
    
    Parameters
    ----------
    raw_value : bytes | str
        Messaggio grezzo
    
    Returns
    -------
    str | None
        Messaggio normalizzato o None se parsing fallisce
    """
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
    """
    Calcola il checksum NMEA per un messaggio.
    
    Parameters
    ----------
    nmea_str_no_checksum : str
        Corpo del messaggio NMEA (senza checksum)
    
    Returns
    -------
    str
        Checksum esadecimale a 2 cifre
    """
    content = nmea_str_no_checksum[1:] if nmea_str_no_checksum.startswith("!") else nmea_str_no_checksum
    return f"{reduce(operator.xor, (ord(c) for c in content), 0):02X}"


def handle_multipart(topic: str, parts) -> Optional[str]:
    """
    Ricompone messaggi AIS multipart (AIVDM con n>1 frammenti).
    
    Utilizza un buffer indicizzato per (topic, canale, sequenza) per
    raccogliere i frammenti e ricomporli quando sono tutti disponibili.
    
    Parameters
    ----------
    topic : str
        Nome del topic Kafka sorgente
    parts : list
        Campi del messaggio NMEA (split per ",")
    
    Returns
    -------
    str | None
        Messaggio ricomposto se completo, altrimenti None
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
    Calcola l'ETA Unix timestamp dai campi AIS.
    
    Estrae eta_month, eta_day, eta_hour, eta_minute dal messaggio AIS
    decodificato e li converte in Unix timestamp.
    
    Parameters
    ----------
    decoded : dict
        Dizionario con i campi AIS decodificati
    
    Returns
    -------
    float | None
        ETA in Unix timestamp o None se non disponibile/valida
    
    Notes
    -----
    - Richiede tutti e 4 i campi per essere valida
    - Gestisce automaticamente il passaggio anno (dic->gen = anno+1)
    """
    try:
        def get_int_or_none(key: str) -> Optional[int]:
            """Helper per estrarre un intero o None."""
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

        # Tutti i campi sono richiesti
        if month is None or day is None or hour is None or minute is None:
            return None

        # Validazione range
        if not (1 <= month <= 12 and 1 <= day <= 31):
            return None
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None

        # Gestione cambio anno
        now = datetime.datetime.now()
        year = now.year + (1 if now.month == 12 and month == 1 else 0)

        return datetime.datetime(year, month, day, hour, minute).timestamp()
    except Exception:
        return None


async def _fetch_active_percorsi(mmsi: str) -> Optional[list]:
    """Recupera in modo non bloccante i percorsi attivi del vascello."""
    try:
        def _request() -> Optional[list]:
            r = requests.get(f"{API_BASE}/vascello/{mmsi}/percorso_attivo", timeout=30)
            if r.status_code != 200:
                return None
            return r.json().get("percorsi", [])

        return await asyncio.to_thread(_request)
    except Exception:
        return None


async def get_expected_eta_from_api(mmsi: str) -> Optional[float]:
    """
    Recupera l'ETA attesa per una nave reale dall'API backend.
    
    Interroga l'endpoint ``/vascello/{mmsi}/percorso_attivo`` e calcola
    l'ETA attesa come ``orario_partenza_schedulato + tempo_percorrenza``.
    
    Seleziona il percorso con ``virtuale=false`` (nave reale).
    
    Parameters
    ----------
    mmsi : str
        MMSI della nave
    
    Returns
    -------
    float | None
        ETA attesa in Unix timestamp o None se non disponibile
    
    Notes
    -----
    Il timeout della richiesta HTTP è di 30 secondi per evitare
    blocchi prolungati in caso di backend lento.
    """
    try:
        percorsi = await _fetch_active_percorsi(mmsi)
        if not percorsi:
            return None

        # Cerca il percorso reale (virtuale=false)
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


async def get_simulation_expected_eta(mmsi: str, start_ts: float) -> Optional[float]:
    """
    Calcola l'ETA attesa per una nave simulata.
    
    Formula: ``start_ts + (tempo_percorrenza / sim_speed_factor)`` dove start_ts 
    è il timestamp del primo messaggio ricevuto dalla simulazione.
    
    Il tempo_percorrenza viene scalato per SIM_SPEED_FACTOR per tenere conto
    della velocità accelerata della simulazione.
    
    Seleziona il percorso con ``virtuale=true`` (simulazione).
    
    Parameters
    ----------
    mmsi : str
        MMSI della nave simulata
    start_ts : float
        Timestamp Unix del primo messaggio ricevuto
    
    Returns
    -------
    float | None
        ETA attesa in Unix timestamp o None se non disponibile
    """
    try:
        percorsi = await _fetch_active_percorsi(mmsi)
        if not percorsi:
            return None

        # Cerca il percorso simulato (virtuale=true)
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

        # Scala il tempo_percorrenza per il fattore di velocità simulazione
        durata_min_scalata = float(durata_min) / SIM_SPEED_FACTOR
        expected = start_ts + durata_min_scalata * 60
        print(f"[DELTA ETA SIM] MMSI={mmsi} durata_min={durata_min} SIM_SPEED_FACTOR={SIM_SPEED_FACTOR} "
              f"durata_scalata={durata_min_scalata:.2f}min expected_eta={expected}")
        return expected
    except Exception as e:
        print(f"[DELTA ETA SIM ERROR] Errore calcolo ETA simulata MMSI={mmsi}: {e}")
        return None


def cleanup_multipart_buffer() -> None:
    """
    Pulizia periodica del buffer multipart.
    
    Rimuove le ricomposizioni stale (>5 secondi) per evitare
    crescita indefinita della memoria.
    """
    global last_cleanup
    now = time.time()
    if now - last_cleanup <= 10:
        return

    for k, v in list(multipart_buffer.items()):
        if now - v["ts"] > 5:
            del multipart_buffer[k]
    last_cleanup = now


async def cleanup_inactive_ships() -> None:
    """
    Rimuove dalla memoria le navi inattive.
    
    Una nave viene rimossa quando non riceve dati per un tempo pari
    a 1/10 del tempo di percorrenza del suo percorso attivo.
    
    Questo evita accumulo di memoria per navi che hanno completato
    la navigazione o sono uscite dall'area di copertura.
    """
    now = time.time()
    async with state_lock:
        for key in list(ships_db.keys()):
            ship = ships_db[key]
            last_seen = ship.get("last_seen")
            tempo_percorrenza = ship.get("tempo_percorrenza")

            if last_seen is None or tempo_percorrenza is None:
                continue

            # Timeout = 1/10 del tempo di percorrenza (in secondi)
            timeout_sec = (tempo_percorrenza * 60) / 10

            if now - last_seen > timeout_sec:
                mmsi = ship.get("mmsi", "?")
                print(f"[CLEANUP] Rimozione nave MMSI={mmsi} (inattiva per >{timeout_sec:.0f}s)")
                del ships_db[key]
                # Rimuovi anche dallo stato simulazione se presente
                if key in simulation_state:
                    del simulation_state[key]


# =============================================================================
# CORE PROCESSOR - Elaborazione Messaggi AIS
# =============================================================================

async def process_ais_message(msg: KafkaMessage, source: Literal["real", "simulation"]) -> None:
    """
    Processa un messaggio AIS e pubblica eventi delta_eta.
    
    Pipeline di elaborazione:
    1. Pulizia buffer e navi inattive
    2. Normalizzazione e decodifica NMEA
    3. Estrazione ETA dal messaggio AIS
    4. Recupero ETA attesa (da API per real, calcolata per simulation)
    5. Calcolo delta e pubblicazione evento
    
    Parameters
    ----------
    msg : KafkaMessage
        Messaggio Kafka contenente dati AIS grezzi
    source : Literal["real", "simulation"]
        Tipo sorgente per distinguere navi reali e simulate
    
    Notes
    -----
    - I messaggi senza ETA valida vengono ignorati (after ack)
    - I messaggi "ghost" (MMSI che inizia con "50") vengono ignorati
    - Lo stato è indicizzato per (topic, mmsi) per separare real/simulation
    - Per le simulazioni, l'ETA attesa viene calcolata al primo messaggio
      e memorizzata in simulation_state
    """
    cleanup_multipart_buffer()
    await cleanup_inactive_ships()

    raw = normalize_nmea(msg.body)
    if not raw or not raw.startswith("!"):
        await msg.ack()
        return

    topic = getattr(msg, "topic", MAIN_TOPIC)

    parts = raw.split(",")
    final = raw

    # Gestione messaggi multipart
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

        # Ignora messaggi "ghost" (MMSI che inizia con 50)
        if mmsi.startswith("50"):
            await msg.ack()
            return

        # Estrai ETA dal messaggio AIS
        eta = calculate_eta_timestamp(data)
        print(f"[DELTA ETA] MMSI={mmsi} ETA={eta} SOURCE={source} TOPIC={topic}")
        if eta is None:
            await msg.ack()
            return
        print(f"[DELTA ETA] {data}")

        key: ShipKey = (topic, mmsi)

        # Variabili da usare fuori dal lock
        destination_norm = "UNKNOWN"
        expected_eta: Optional[float] = None

        # Recupero tempo_percorrenza dall'API per il cleanup (non bloccante)
        tempo_percorrenza: Optional[float] = None
        percorsi: Optional[list] = None
        try:
            percorsi = await _fetch_active_percorsi(mmsi)
            if percorsi:
                virtuale_target = (source == "simulation")
                for p in percorsi:
                    if p.get("assegnazione", {}).get("virtuale") is virtuale_target:
                        tempo_percorrenza = p.get("percorso", {}).get("tempo_percorrenza")
                        break
        except Exception:
            pass

        # Pre-calcolo ETA attesa fuori dal lock per evitare attese bloccanti
        precomputed_expected_eta: Optional[float] = None
        precomputed_start_ts: Optional[float] = None
        if source == "real":
            precomputed_expected_eta = await get_expected_eta_from_api(mmsi)
        else:
            if simulation_state.get(key) is None:
                # Usa timestamp Kafka se disponibile, fallback wall clock
                msg_ts = getattr(msg, "timestamp", None)
                if isinstance(msg_ts, (int, float)):
                    precomputed_start_ts = float(msg_ts) / 1000.0
                else:
                    precomputed_start_ts = time.time()
                if precomputed_start_ts is not None:
                    precomputed_expected_eta = await get_simulation_expected_eta(mmsi, precomputed_start_ts)

        async with state_lock:
            # Aggiorna database navi
            ship = ships_db.setdefault(key, {"mmsi": mmsi, "topic": topic})
            ship.update(data)
            ship["last_seen"] = time.time()
            if tempo_percorrenza is not None:
                ship["tempo_percorrenza"] = tempo_percorrenza

            # Normalizza destinazione
            destination = ship.get("destination") or "UNKNOWN"
            if isinstance(destination, str):
                destination = " ".join(destination.strip().upper().split()) or "UNKNOWN"
            ship["destination"] = destination
            destination_norm = destination

            # Calcola ETA attesa in base alla sorgente senza I/O sotto lock
            if source == "real":
                expected_eta = precomputed_expected_eta
            else:
                sim = simulation_state.get(key)
                if not sim and precomputed_expected_eta is not None and precomputed_start_ts is not None:
                    simulation_state[key] = {
                        "start_ts": precomputed_start_ts,
                        "expected_eta": precomputed_expected_eta,
                        "tempo_percorrenza": tempo_percorrenza,
                    }
                    expected_eta = precomputed_expected_eta
                elif sim:
                    expected_eta = sim.get("expected_eta")

        if expected_eta is None:
            await msg.ack()
            return

        # Calcola delta in minuti
        delta_min = (eta - expected_eta) / 60.0

        # Costruisci e pubblica evento
        event = DeltaEtaEvent(
            mmsi=mmsi,
            delta_min=delta_min,
            destination=destination_norm,
            eta=eta,
            eta_expected=expected_eta,
            source=source,
            sim_speed_factor=SIM_SPEED_FACTOR if source == "simulation" else None,
            timestamp=time.time(),
        )

        await broker.publish(event, topic=ANALYTICS_TOPIC)
        await msg.ack()

    except Exception as e:
        print(f"[DELTA ETA ERROR] {e}")
        await msg.nack()


# =============================================================================
# SUBSCRIBER KAFKA
# =============================================================================

@broker.subscriber(MAIN_TOPIC)
async def ais_consumer_real(msg: KafkaMessage):
    """
    Subscriber per il topic AIS principale (dati reali).
    
    Processa messaggi con source="real" per calcolare delta ETA
    basato su orari schedulati dal backend.
    
    Parameters
    ----------
    msg : KafkaMessage
        Messaggio Kafka contenente dati AIS reali
    """
    await process_ais_message(msg, source="real")


@broker.subscriber(SIM_TOPIC)
async def ais_consumer_sim(msg: KafkaMessage):
    """
    Subscriber per il topic AIS simulazione.
    
    Processa messaggi con source="simulation" per calcolare delta ETA
    basato sul timestamp di inizio simulazione + tempo percorrenza.
    
    Parameters
    ----------
    msg : KafkaMessage
        Messaggio Kafka contenente dati AIS simulati
    """
    await process_ais_message(msg, source="simulation")


# =============================================================================
# CONFIG WATCHER - Ricaricamento Configurazione
# =============================================================================

async def refresh_active_simulations_expected_eta() -> None:
    """
    Ricalcola l'ETA attesa per tutte le simulazioni attive.

    Viene invocata quando cambia SIM_SPEED_FACTOR per applicare
    subito il nuovo fattore anche alle simulazioni già in corso.
    Evita inconsistenze tra sim_speed_factor pubblicato e delta_min calcolato.
    """
    async with state_lock:
        snapshot = [
            (key, dict(sim))
            for key, sim in simulation_state.items()
        ]

    if not snapshot:
        return

    updated = 0
    skipped = 0

    for key, sim_snapshot in snapshot:
        start_ts = sim_snapshot.get("start_ts")
        old_expected_eta = sim_snapshot.get("expected_eta")
        durata_min = sim_snapshot.get("tempo_percorrenza")

        if start_ts is None:
            skipped += 1
            continue

        _, mmsi = key

        new_expected_eta: Optional[float] = None
        try:
            if durata_min is not None:
                new_expected_eta = float(start_ts) + (float(durata_min) / SIM_SPEED_FACTOR) * 60.0
            else:
                new_expected_eta = await get_simulation_expected_eta(mmsi, float(start_ts))
        except Exception:
            new_expected_eta = None

        if new_expected_eta is None:
            skipped += 1
            continue

        async with state_lock:
            sim = simulation_state.get(key)
            if sim is None:
                continue

            sim["expected_eta"] = new_expected_eta

        updated += 1
        print(
            f"[DELTA ETA CONFIG] Recompute simulation MMSI={mmsi} "
            f"expected_eta: {old_expected_eta} -> {new_expected_eta}"
        )

    print(
        f"[DELTA ETA CONFIG] Recompute simulazioni attive completato: "
        f"aggiornate={updated}, saltate={skipped}"
    )


async def periodic_cleanup_task():
    """
    Task periodico per pulizia navi inattive.
    
    Esegue cleanup ogni 5 minuti anche quando non arrivano messaggi,
    evitando accumulo di memoria in caso di idle prolungato del sistema.
    """
    while True:
        await asyncio.sleep(300)  # Check ogni 5 minuti
        await cleanup_inactive_ships()
        print("[CLEANUP] Pulizia periodica completata")


async def config_watcher():
    """
    Task asincrono per ricaricamento automatico della configurazione.
    
    Ogni 2 minuti controlla il backend per aggiornamenti alla configurazione.
    Se il timestamp last_update è più recente, ricarica i parametri.
    
    Parametri aggiornati:
    - SIM_SPEED_FACTOR: fattore velocità simulazione
    
    Notes
    -----
    Questo permette di modificare la configurazione dalla dashboard
    senza riavviare il container Docker.
    """
    global SIM_SPEED_FACTOR, CONFIG_LAST_UPDATE

    while True:
        await asyncio.sleep(120)  # Check ogni 2 minuti

        log("[DELTA ETA CONFIG] Watcher tick (120s)")

        try:
            new_config = load_kafka_config_from_dashboard()
            #last_update = float(new_config.get("last_update", 0))
            log(f"[DELTA ETA CONFIG] Configurazione ricevuta: sim_speed_factor={new_config.get('sim_speed_factor', '<ASSENTE>')}")
            log(f"")

            if SIM_SPEED_FACTOR != new_config.get("sim_speed_factor", 1.0):
                old_sim_speed = SIM_SPEED_FACTOR
                new_sim_speed = new_config.get("sim_speed_factor", 1.0)

                log("[DELTA ETA CONFIG] Ricaricamento configurazione...")
                log(f"[DELTA ETA CONFIG] SIM_SPEED_FACTOR: {old_sim_speed} -> {new_sim_speed}")

                SIM_SPEED_FACTOR = new_sim_speed
                #CONFIG_LAST_UPDATE = last_update

                # Applica il nuovo fattore anche alle simulazioni già in corso
                if new_sim_speed != old_sim_speed:
                    await refresh_active_simulations_expected_eta()
        except Exception as e:
            log(f"[DELTA ETA CONFIG ERROR] watcher crash avoided: {e}")


def _log_task_failure(task: asyncio.Task) -> None:
    """Logga eventuali crash dei task in background."""
    try:
        exc = task.exception()
        if exc is not None:
            log(f"[DELTA ETA TASK ERROR] Task '{task.get_name()}' terminato con errore: {exc}")
    except asyncio.CancelledError:
        log(f"[DELTA ETA TASK INFO] Task '{task.get_name()}' cancellato")


# =============================================================================
# LIFECYCLE HOOKS
# =============================================================================

@app.on_startup
async def startup():
    """
    Hook eseguito all'avvio dell'applicazione FastStream.
    
    Inizializza i task asincroni per:
    - Watcher configurazione (ricarica SIM_SPEED_FACTOR ogni 2 minuti)
    - Cleanup periodico navi inattive (ogni 5 minuti)
    """
    log("=" * 60)
    log("BRIDGE DELTA ETA - Analytics Worker")
    log("=" * 60)
    log(f"Kafka Bootstrap: {BOOTSTRAP_SERVERS}")
    log(f"Topic Input:     {MAIN_TOPIC}, {SIM_TOPIC}")
    log(f"Topic Output:    {ANALYTICS_TOPIC}")
    log(f"API Backend:     {API_BASE}")
    log(f"Sim Speed Factor: {SIM_SPEED_FACTOR}")
    log("=" * 60)
    log("Worker avviato (real + simulation)")
    log("=" * 60)

    config_task = asyncio.create_task(config_watcher(), name="deltaeta_config_watcher")
    cleanup_task = asyncio.create_task(periodic_cleanup_task(), name="deltaeta_cleanup")
    config_task.add_done_callback(_log_task_failure)
    cleanup_task.add_done_callback(_log_task_failure)
