"""
Bridge Arrivo - Worker Rilevamento Arrivo Navi (Geofencing)
============================================================

Descrizione
-----------
Questo modulo implementa un worker FastStream che rileva l'arrivo delle navi
a destinazione utilizzando il geofencing basato sulle coordinate della rotta
recuperata dal backend.

Funzionalità Principale
-----------------------
Monitora la posizione delle navi tramite messaggi AIS (tipo 1-3) e confronta
le coordinate con il punto finale della rotta attiva (``geom_rotta``).
Quando la nave entra nel raggio del geofence di destinazione, aggiorna lo
stato dell'assegnazione a ``COMPLETATA`` tramite l'API backend:
``PATCH /assegnazione/{assegnazione_id}/stato``

Architettura del Flusso Dati
----------------------------
::

    ┌─────────────────────┐                                    
    │     ais.raw         │ ──┐                                
    │  (NMEA grezzo)      │   │    ┌────────────────────────┐
    └─────────────────────┘   ├──► │  Bridge Arrivo         │
                              │    │                        │
    ┌─────────────────────┐   │    │  - Parsing posizione   │
    │  ais_simulation.raw │ ──┘    │  - Geofencing          │
    │  (NMEA grezzo)      │        │  - State tracking      │
    └─────────────────────┘        └────────────────────────┘
                                        │
                                        ▼
                                ┌──────────────────────────────┐
                                │  Backend API                 │
                                │  GET  /vascello/{mmsi}/      │
                                │       percorso_attivo        │
                                │  GET  /percorso/{id}         │
                                │  PATCH /assegnazione/{id}/   │
                                │       stato                  │
                                └──────────────────────────────┘

Topic Kafka
-----------
**Input (Subscription):**
    - ``ais.raw``: Messaggi AIS reali in formato NMEA
    - ``ais_simulation.raw``: Messaggi AIS simulati in formato NMEA

**Output:**
    Nessun topic di output. All'arrivo viene invocato l'endpoint REST:
    ``PATCH /assegnazione/{assegnazione_id}/stato`` con body
    ``{"stato_esecuzione": "COMPLETATA"}``

Logica di Rilevamento Arrivo
-----------------------------
La nave è considerata arrivata quando TUTTE le condizioni sono soddisfatte:

1. **Geofence**: La distanza (haversine) tra la posizione AIS e l'ultimo
   punto della ``geom_rotta`` è inferiore al raggio configurato (default 500m)
2. **Persistenza**: La condizione deve persistere per un numero minimo
   di messaggi consecutivi (default 3) per evitare falsi positivi

Gestione Cache Rotte
--------------------
Le coordinate di destinazione e l'ID assegnazione vengono recuperati
dall'API backend e cachati per ogni nave. La cache viene aggiornata
ogni N minuti (configurabile) per gestire cambi di rotta.

Pipeline API:
1. ``GET /vascello/{mmsi}/percorso_attivo`` → ottiene percorso_id e assegnazione_id
2. ``GET /percorso/{percorso_id}`` → ottiene ``geom_rotta``
3. Estrae ultimo punto della geometria come coordinate destinazione
4. All'arrivo: ``PATCH /assegnazione/{assegnazione_id}/stato``
   con body ``{"stato_esecuzione": "COMPLETATA"}``

Dipendenze
----------
- ``faststream``: Framework per streaming Kafka
- ``pyais``: Libreria per decodifica messaggi AIS
- ``pydantic``: Validazione e serializzazione dati
- ``requests``: Client HTTP per query API backend
- ``config_loader``: Modulo per caricamento configurazione da dashboard

Autore: Team AIS Analytics
Versione: 1.1.0
"""

# =============================================================================
# IMPORTS
# =============================================================================

import asyncio
import datetime
import json
import math
import operator
import os
import time
from functools import reduce
from typing import Dict, Literal, Optional, Tuple

import requests
from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from pydantic import BaseModel, Field
from pyais import decode as ais_decode

from config_loader import load_kafka_config_from_dashboard
import logging

# Riduce la verbosità dei log FastStream
logging.getLogger("faststream").setLevel(logging.WARNING)


# =============================================================================
# CONFIGURAZIONE
# =============================================================================

config = load_kafka_config_from_dashboard()

PUBLISH_INTERVAL: int = int(config["publish_interval"])
"""int: Intervallo pubblicazione eventi periodici in secondi"""

CONFIG_LAST_UPDATE: float = float(config.get("last_update", time.time()))
"""float: Timestamp ultimo aggiornamento configurazione"""

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "87.26.178.190:29092")
"""str: Indirizzo cluster Kafka"""

MAIN_TOPIC = "ais.raw"
"""str: Topic input per messaggi AIS reali"""

SIM_TOPIC = "ais_simulation.raw"
"""str: Topic input per messaggi AIS simulati"""

API_BASE = os.getenv("API_BASE", "http://87.26.178.190:25080")
"""str: URL base dell'API backend"""

# --- Parametri Geofencing ---
GEOFENCE_RADIUS_M: float = float(os.getenv("GEOFENCE_RADIUS_M", "500"))
"""float: Raggio del geofence di destinazione in metri (default: 500m)"""

ARRIVAL_CONFIRM_COUNT: int = int(os.getenv("ARRIVAL_CONFIRM_COUNT", "3"))
"""int: Numero di messaggi consecutivi in geofence + bassa velocità per confermare l'arrivo"""

ROUTE_CACHE_TTL_SEC: float = float(os.getenv("ROUTE_CACHE_TTL_SEC", "600"))
"""float: Tempo di vita della cache rotte in secondi (default: 10 minuti)"""

SHIP_INACTIVE_TIMEOUT_SEC: float = float(os.getenv("SHIP_INACTIVE_TIMEOUT_SEC", "3600"))
"""float: Timeout in secondi per rimuovere navi inattive dalla memoria (default: 1 ora)"""


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

ships: Dict[ShipKey, dict] = {}
"""
Dict[ShipKey, dict]: Database in-memory delle navi.

Struttura valore:
{
    "mmsi": str,                    # MMSI nave
    "topic": str,                   # Topic sorgente
    "lat": float,                   # Ultima latitudine nota
    "lon": float,                   # Ultima longitudine nota
    "speed": float,                 # Ultima velocità SOG (nodi) [informativo]
    "status": int,                  # Stato navigazione AIS [informativo]
    "destination": str,             # Destinazione AIS dichiarata
    "last_seen": float,             # Timestamp ultimo messaggio
    "arrival_state": str,           # "navigating" | "arriving" | "arrived"
    "confirm_count": int,           # Contatore conferme arrivo consecutive
    "dest_lat": float | None,      # Lat destinazione (da geom_rotta)
    "dest_lon": float | None,      # Lon destinazione (da geom_rotta)
    "route_cache_ts": float,        # Timestamp cache rotta
    "percorso_id": str | None,      # ID percorso attivo
    "assegnazione_id": str | None,   # ID assegnazione (per PUT stato)
    "arrival_completed": bool,       # True se PUT COMPLETATA già effettuato
}
"""

multipart_buffer: Dict[tuple, dict] = {}
"""Dict[tuple, dict]: Buffer per ricomposizione messaggi NMEA multipart"""

last_cleanup = time.time()
"""float: Timestamp ultima pulizia buffer multipart"""


# =============================================================================
# MODELLI PYDANTIC (Schema AsyncAPI)
# =============================================================================

# (Nessun modello Pydantic di output: il servizio non pubblica su topic Kafka,
#  ma effettua una chiamata PUT all'API backend all'arrivo della nave)


# =============================================================================
# FUNZIONI UTILITY
# =============================================================================

def log(msg: str) -> None:
    """
    Stampa un messaggio di log con timestamp formattato.

    Parameters
    ----------
    msg : str
        Messaggio da loggare
    """
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
            return raw_value[raw_value.find("!"):]

        return None
    except Exception:
        return None


def compute_checksum(body: str) -> str:
    """
    Calcola il checksum NMEA per un messaggio.

    Parameters
    ----------
    body : str
        Corpo del messaggio NMEA (senza checksum)

    Returns
    -------
    str
        Checksum esadecimale a 2 cifre
    """
    content = body[1:] if body.startswith("!") else body
    return f"{reduce(operator.xor, (ord(c) for c in content), 0):02X}"


def handle_multipart(topic: str, parts) -> Optional[str]:
    """
    Ricompone messaggi AIS multipart (AIVDM con n>1 frammenti).

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


def cleanup_multipart_buffer() -> None:
    """
    Pulizia periodica del buffer multipart.
    Rimuove le ricomposizioni stale (>5 secondi).
    """
    global last_cleanup
    now = time.time()
    if now - last_cleanup <= 10:
        return

    for k, v in list(multipart_buffer.items()):
        if now - v["ts"] > 5:
            del multipart_buffer[k]
    last_cleanup = now


# =============================================================================
# FUNZIONI GEOFENCING
# =============================================================================

def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calcola la distanza in metri tra due coordinate usando la formula di Haversine.

    Parameters
    ----------
    lat1, lon1 : float
        Coordinate del primo punto (gradi decimali)
    lat2, lon2 : float
        Coordinate del secondo punto (gradi decimali)

    Returns
    -------
    float
        Distanza in metri
    """
    R = 6_371_000  # Raggio terrestre medio in metri

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def extract_destination_coords_from_geom(geom_rotta) -> Optional[Tuple[float, float]]:
    """
    Estrae le coordinate di destinazione dall'ultimo punto della geometria rotta.

    Supporta diversi formati di ``geom_rotta``:
    - GeoJSON LineString: ``{"type": "LineString", "coordinates": [[lon, lat], ...]}``
    - GeoJSON MultiLineString: ``{"type": "MultiLineString", "coordinates": [[[lon, lat], ...], ...]}``
    - Lista di coordinate: ``[[lon, lat], ...]``
    - Stringa GeoJSON (viene parsata automaticamente)

    Parameters
    ----------
    geom_rotta : dict | list | str
        Geometria della rotta in uno dei formati supportati

    Returns
    -------
    tuple[float, float] | None
        Tupla (latitudine, longitudine) dell'ultimo punto, o None se non estraibile.
        Nota: GeoJSON usa ordine [lon, lat], questa funzione restituisce (lat, lon).
    """
    try:
        # Se è una stringa, prova a parsarla come JSON
        if isinstance(geom_rotta, str):
            geom_rotta = json.loads(geom_rotta)

        coords = None

        if isinstance(geom_rotta, dict):
            geom_type = geom_rotta.get("type", "")

            if geom_type == "LineString":
                coords = geom_rotta.get("coordinates", [])
            elif geom_type == "MultiLineString":
                all_lines = geom_rotta.get("coordinates", [])
                if all_lines:
                    coords = all_lines[-1]  # Ultima linea
            elif "coordinates" in geom_rotta:
                coords = geom_rotta["coordinates"]
                # Se è una lista di liste di liste (MultiLineString senza type)
                if coords and isinstance(coords[0], list) and isinstance(coords[0][0], list):
                    coords = coords[-1]
        elif isinstance(geom_rotta, list):
            coords = geom_rotta

        if not coords or len(coords) == 0:
            return None

        # Ultimo punto della geometria = destinazione
        last_point = coords[-1]

        if isinstance(last_point, (list, tuple)) and len(last_point) >= 2:
            lon, lat = float(last_point[0]), float(last_point[1])
            # Validazione coordinate base
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return (lat, lon)

        return None
    except Exception as e:
        log(f"[GEOM] Errore parsing geom_rotta: {e}")
        return None


def get_route_info(mmsi: str, is_simulation: bool) -> Optional[Tuple[float, float, str, str]]:
    """
    Recupera le coordinate di destinazione e l'ID assegnazione dall'API backend.

    Pipeline:
    1. ``GET /vascello/{mmsi}/percorso_attivo`` → ottiene percorso_id e assegnazione_id
    2. ``GET /percorso/{percorso_id}`` → ottiene ``geom_rotta``
    3. Estrae ultimo punto della geometria

    Parameters
    ----------
    mmsi : str
        MMSI della nave
    is_simulation : bool
        True per navi simulate (cerca percorso con virtuale=true)

    Returns
    -------
    tuple[float, float, str, str] | None
        Tupla (latitudine, longitudine, percorso_id, assegnazione_id)
        o None se non disponibile
    """
    try:
        # Step 1: Recupera percorso attivo
        r = requests.get(f"{API_BASE}/vascello/{mmsi}/percorso_attivo", timeout=30)
        if r.status_code != 200:
            return None

        percorsi = r.json().get("percorsi", [])
        if not percorsi:
            return None

        # Cerca il percorso corrispondente (reale o virtuale)
        percorso_data = None
        percorso_id = None
        assegnazione_id = None
        for p in percorsi:
            virtuale = p.get("assegnazione", {}).get("virtuale")
            if virtuale is is_simulation:
                assegnazione_id = p.get("assegnazione", {}).get("id")
                percorso_data = p.get("percorso", {})
                percorso_id = percorso_data.get("id") or percorso_data.get("_id")
                break

        if not percorso_data or not percorso_id or not assegnazione_id:
            return None

        # Step 2: Recupera dettagli percorso con geom_rotta
        # Prima controlla se geom_rotta è già incluso nel percorso attivo
        geom_rotta = percorso_data.get("geom_rotta")

        if not geom_rotta:
            # Se non c'è, facciamo la chiamata dedicata
            r2 = requests.get(f"{API_BASE}/percorso/{percorso_id}", timeout=3)
            if r2.status_code != 200:
                return None

            percorso_detail = r2.json()
            geom_rotta = percorso_detail.get("geom_rotta")

        if not geom_rotta:
            return None

        # Step 3: Estrai ultimo punto
        dest = extract_destination_coords_from_geom(geom_rotta)
        if dest is None:
            return None

        return (dest[0], dest[1], str(percorso_id), str(assegnazione_id))

    except Exception as e:
        log(f"[API] Errore recupero info rotta per MMSI={mmsi}: {e}")
        return None


def mark_assegnazione_completata(assegnazione_id: str, mmsi: str) -> bool:
    """
    Aggiorna lo stato dell'assegnazione a COMPLETATA tramite API backend.

    Effettua una chiamata:
    ``PATCH /assegnazione/{assegnazione_id}/stato``
    con body ``{"stato_esecuzione": "COMPLETATA"}``

    Parameters
    ----------
    assegnazione_id : str
        ID dell'assegnazione da aggiornare
    mmsi : str
        MMSI della nave (usato solo per logging)

    Returns
    -------
    bool
        True se la chiamata è andata a buon fine, False altrimenti
    """
    try:
        url = f"{API_BASE}/assegnazione/{assegnazione_id}/stato"
        payload = {"stato_esecuzione": "COMPLETATA"}

        r = requests.patch(url, json=payload, timeout=30)

        if r.status_code in (200, 201, 204):
            log(f"[API] ✓ Assegnazione {assegnazione_id} marcata COMPLETATA "
                f"(MMSI={mmsi}, HTTP {r.status_code})")
            return True
        else:
            log(f"[API] ✗ Errore aggiornamento assegnazione {assegnazione_id}: "
                f"HTTP {r.status_code} - {r.text}")
            return False

    except Exception as e:
        log(f"[API] ✗ Errore chiamata PUT assegnazione {assegnazione_id}: {e}")
        return False


# =============================================================================
# CLEANUP
# =============================================================================

async def cleanup_inactive_ships() -> None:
    """
    Rimuove dalla memoria le navi inattive.

    Una nave viene rimossa quando non riceve dati per un tempo
    superiore a SHIP_INACTIVE_TIMEOUT_SEC (default: 1 ora).
    """
    now = time.time()
    async with state_lock:
        for key in list(ships.keys()):
            ship = ships[key]
            last_seen = ship.get("last_seen", 0)

            if now - last_seen > SHIP_INACTIVE_TIMEOUT_SEC:
                mmsi = ship.get("mmsi", "?")
                log(f"[CLEANUP] Rimozione nave MMSI={mmsi} (inattiva per >{SHIP_INACTIVE_TIMEOUT_SEC:.0f}s)")
                del ships[key]


# =============================================================================
# CORE PROCESSOR - Elaborazione Messaggi AIS
# =============================================================================

async def process_ais_message(msg: KafkaMessage, source: Literal["real", "simulation"]) -> None:
    """
    Processa un messaggio AIS e verifica l'arrivo a destinazione.

    Pipeline di elaborazione:
    1. Pulizia buffer e navi inattive
    2. Normalizzazione e decodifica NMEA
    3. Estrazione posizione (lat/lon), velocità (SOG), stato navigazione
    4. Recupero coordinate destinazione e assegnazione_id (da cache o API)
    5. Calcolo distanza (haversine) dalla destinazione
    6. Logica di stato: navigating → arriving → arrived
    7. All'arrivo: PATCH /assegnazione/{assegnazione_id}/stato → COMPLETATA

    Parameters
    ----------
    msg : KafkaMessage
        Messaggio Kafka contenente dati AIS grezzi
    source : Literal["real", "simulation"]
        Tipo sorgente per distinguere navi reali e simulate
    """
    cleanup_multipart_buffer()

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

        # Estrai posizione - richiede lat e lon (msg tipo 1-3)
        lat = data.get("lat")
        lon = data.get("lon")
        speed = data.get("speed")
        nav_status = data.get("status")

        # Estrai destinazione (msg tipo 5)
        destination = data.get("destination")
        if destination and isinstance(destination, str):
            destination = " ".join(destination.strip().upper().split()) or None

        is_simulation = (source == "simulation")
        key: ShipKey = (topic, mmsi)
        now = time.time()

        # Variabili per decisioni fuori dal lock
        should_mark_completed = False
        assegnazione_id_to_complete = None

        async with state_lock:
            # Inizializza o aggiorna stato nave
            ship = ships.setdefault(key, {
                "mmsi": mmsi,
                "topic": topic,
                "lat": None,
                "lon": None,
                "speed": None,
                "status": None,
                "destination": None,
                "last_seen": now,
                "arrival_state": "navigating",  # navigating | arriving | arrived
                "confirm_count": 0,
                "dest_lat": None,
                "dest_lon": None,
                "route_cache_ts": 0,
                "percorso_id": None,
                "assegnazione_id": None,
                "arrival_completed": False,
            })

            ship["last_seen"] = now

            # Aggiorna posizione se disponibile
            if lat is not None and lon is not None:
                # Filtra coordinate invalide (0,0 o fuori range)
                if -90 <= lat <= 90 and -180 <= lon <= 180 and not (lat == 0 and lon == 0):
                    ship["lat"] = lat
                    ship["lon"] = lon

            if speed is not None:
                ship["speed"] = speed

            if nav_status is not None:
                ship["status"] = nav_status

            if destination:
                ship["destination"] = destination

            # ---- LOGICA GEOFENCING ----

            # Se non abbiamo posizione, non possiamo verificare arrivo
            if ship["lat"] is None or ship["lon"] is None:
                await msg.ack()
                return

            # Recupera/aggiorna coordinate destinazione e assegnazione_id (con cache)
            if ship["dest_lat"] is None or (now - ship["route_cache_ts"] > ROUTE_CACHE_TTL_SEC):
                route_info = get_route_info(mmsi, is_simulation)
                if route_info:
                    ship["dest_lat"] = route_info[0]
                    ship["dest_lon"] = route_info[1]
                    ship["percorso_id"] = route_info[2]
                    ship["assegnazione_id"] = route_info[3]
                    ship["route_cache_ts"] = now
                    log(f"[ROUTE] MMSI={mmsi} destinazione cached: "
                        f"({route_info[0]:.4f}, {route_info[1]:.4f}) "
                        f"assegnazione={route_info[3]}")

            # Se non abbiamo coordinate destinazione, skip
            if ship["dest_lat"] is None or ship["dest_lon"] is None:
                await msg.ack()
                return

            # Calcola distanza dalla destinazione
            distance = haversine_distance_m(
                ship["lat"], ship["lon"],
                ship["dest_lat"], ship["dest_lon"]
            )

            in_geofence = distance <= GEOFENCE_RADIUS_M

            # ---- MACCHINA A STATI ----
            prev_state = ship["arrival_state"]

            if prev_state == "navigating":
                if in_geofence:
                    ship["confirm_count"] += 1
                    if ship["confirm_count"] >= ARRIVAL_CONFIRM_COUNT:
                        ship["arrival_state"] = "arrived"
                        if not ship["arrival_completed"] and ship["assegnazione_id"]:
                            should_mark_completed = True
                            assegnazione_id_to_complete = ship["assegnazione_id"]
                        log(f"[ARRIVED] MMSI={mmsi} dest={ship['destination']} "
                            f"dist={distance:.0f}m "
                            f"assegnazione={ship['assegnazione_id']}")
                    else:
                        ship["arrival_state"] = "arriving"
                else:
                    ship["confirm_count"] = 0

            elif prev_state == "arriving":
                if in_geofence:
                    ship["confirm_count"] += 1
                    if ship["confirm_count"] >= ARRIVAL_CONFIRM_COUNT:
                        ship["arrival_state"] = "arrived"
                        if not ship["arrival_completed"] and ship["assegnazione_id"]:
                            should_mark_completed = True
                            assegnazione_id_to_complete = ship["assegnazione_id"]
                        log(f"[ARRIVED] MMSI={mmsi} dest={ship['destination']} "
                            f"dist={distance:.0f}m "
                            f"assegnazione={ship['assegnazione_id']}")
                else:
                    # Reset se condizioni non più soddisfatte
                    ship["arrival_state"] = "navigating"
                    ship["confirm_count"] = 0

            elif prev_state == "arrived":
                # Già arrivato, nessuna azione ulteriore
                pass

        # ---- CHIAMATA API BACKEND (fuori dal lock) ----
        if should_mark_completed and assegnazione_id_to_complete:
            success = mark_assegnazione_completata(assegnazione_id_to_complete, mmsi)
            if success:
                async with state_lock:
                    if key in ships:
                        ships[key]["arrival_completed"] = True

        await msg.ack()

    except Exception as e:
        log(f"[ERROR] Errore processing AIS: {e}")
        await msg.nack()


# =============================================================================
# SUBSCRIBER KAFKA
# =============================================================================

@broker.subscriber(MAIN_TOPIC)
async def ais_consumer_real(msg: KafkaMessage):
    """
    Subscriber per il topic AIS principale (dati reali).

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

    Parameters
    ----------
    msg : KafkaMessage
        Messaggio Kafka contenente dati AIS simulati
    """
    await process_ais_message(msg, source="simulation")


# =============================================================================
# TASK PERIODICI
# =============================================================================

async def cleanup_loop():
    """
    Loop asincrono per pulizia periodica delle navi inattive.
    Eseguito ogni 5 minuti.
    """
    while True:
        await asyncio.sleep(300)
        await cleanup_inactive_ships()


async def config_watcher():
    """
    Task asincrono per ricaricamento automatico della configurazione.

    Controlla ogni 2 minuti il backend per aggiornamenti.
    """
    global PUBLISH_INTERVAL, CONFIG_LAST_UPDATE

    while True:
        await asyncio.sleep(120)

        new_config = load_kafka_config_from_dashboard()
        last_update = float(new_config.get("last_update", 0))

        if last_update > CONFIG_LAST_UPDATE:
            log(f"[CONFIG] Aggiornamento: "
                f"PUBLISH_INTERVAL {PUBLISH_INTERVAL} -> {new_config['publish_interval']} sec")

            PUBLISH_INTERVAL = int(new_config["publish_interval"])
            CONFIG_LAST_UPDATE = last_update


# =============================================================================
# LIFECYCLE HOOKS
# =============================================================================

@app.on_startup
async def startup():
    """
    Hook eseguito all'avvio dell'applicazione FastStream.

    Inizializza i task asincroni per:
    - Pulizia periodica navi inattive
    - Watcher configurazione
    """
    log("=" * 60)
    log("BRIDGE ARRIVO - Analytics Worker")
    log("=" * 60)
    log(f"Kafka Bootstrap:      {BOOTSTRAP_SERVERS}")
    log(f"Topic Input:          {MAIN_TOPIC}, {SIM_TOPIC}")
    log(f"API Backend:          {API_BASE}")
    log(f"Geofence Radius:      {GEOFENCE_RADIUS_M} m")
    log(f"Arrival Confirm:      {ARRIVAL_CONFIRM_COUNT} messaggi")
    log(f"Route Cache TTL:      {ROUTE_CACHE_TTL_SEC} sec")
    log(f"Inactive Timeout:     {SHIP_INACTIVE_TIMEOUT_SEC} sec")
    log("=" * 60)
    log("Worker avviato (real + simulation)")
    log("=" * 60)

    asyncio.create_task(cleanup_loop())
    asyncio.create_task(config_watcher())
