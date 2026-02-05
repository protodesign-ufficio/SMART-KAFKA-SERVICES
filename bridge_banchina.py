"""
Bridge Banchina - Worker Analytics FastStream
==============================================

Descrizione
-----------
Questo modulo implementa un worker FastStream che analizza i messaggi AIS
per tracciare le navi in arrivo alle banchine e pubblicare eventi di analytics
aggregati sul topic ``analytics_ais.raw``.

Funzionalità Principale
-----------------------
Monitora le navi che hanno impostato una destinazione (campo AIS "destination")
e calcola quali sono in arrivo entro una finestra temporale configurabile.
Pubblica periodicamente eventi ``berth_incoming`` con l'elenco delle navi
attese per ogni banchina/destinazione.

Architettura del Flusso Dati
----------------------------
::

    ┌─────────────────────┐                                    
    │     ais.raw         │ ──┐                                
    │  (NMEA grezzo)      │   │    ┌────────────────────────┐     ┌─────────────────────────┐
    └─────────────────────┘   ├──► │  Bridge Banchina       │ ──► │   analytics_ais.raw     │
                              │    │                        │     │                         │
    ┌─────────────────────┐   │    │  - Parsing AIS         │     │  Eventi:                │
    │  ais_simulation.raw │ ──┘    │  - Aggregazione ETA    │     │  - berth_incoming       │
    │  (NMEA grezzo)      │        │  - Publishing ciclico  │     │                         │
    └─────────────────────┘        └────────────────────────┘     └─────────────────────────┘

Topic Kafka
-----------
**Input (Subscription):**
    - ``ais.raw``: Messaggi AIS reali in formato NMEA
    - ``ais_simulation.raw``: Messaggi AIS simulati in formato NMEA

**Output (Publishing):**
    - ``analytics_ais.raw``: Eventi analytics aggregati (tipo ``berth_incoming``)

Configurazione Dinamica
-----------------------
Il worker supporta la configurazione dinamica tramite dashboard backend:

- ``WINDOW_FUTURE_MIN``: Finestra temporale in minuti per considerare una nave "in arrivo"
- ``PUBLISH_INTERVAL``: Intervallo in secondi tra le pubblicazioni degli eventi

La configurazione viene ricaricata automaticamente ogni 2 minuti dal backend,
permettendo modifiche senza riavvio del container Docker.

Logica di Business
------------------
1. **Ricezione**: Consuma messaggi AIS grezzi da entrambi i topic
2. **Parsing**: Estrae MMSI, destinazione ed ETA dal messaggio AIS tipo 5
3. **Aggregazione**: Raggruppa le navi per destinazione/banchina
4. **Filtraggio**: Seleziona solo le navi con ETA nella finestra temporale
5. **Pubblicazione**: Pubblica eventi ``berth_incoming`` periodicamente

Dipendenze
----------
- ``faststream``: Framework per streaming Kafka
- ``pyais``: Libreria per decodifica messaggi AIS
- ``pydantic``: Validazione e serializzazione dati
- ``config_loader``: Modulo per caricamento configurazione da dashboard

Note per Sviluppatori
---------------------
- I publisher stub decorati con ``@broker.publisher`` sono per AsyncAPI
- Lo stato delle navi è condiviso e protetto da asyncio.Lock
- Il buffer multipart gestisce messaggi AIS multi-sentence
- La separazione real/simulation è mantenuta nel campo ``source``

Autore: Team AIS Analytics
Versione: 2.0.0
"""

# =============================================================================
# IMPORTS
# =============================================================================

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

# Riduce la verbosità dei log FastStream
logging.getLogger("faststream").setLevel(logging.WARNING)


# =============================================================================
# CONFIGURAZIONE
# =============================================================================
# Caricamento configurazione da dashboard con fallback a valori default.
# La configurazione viene ricaricata periodicamente dal config_watcher.

config = load_kafka_config_from_dashboard()

WINDOW_FUTURE_MIN: int = int(config["window_future"])
"""int: Finestra temporale in minuti per navi "in arrivo" (default da dashboard)"""

PUBLISH_INTERVAL: int = int(config["publish_interval"])
"""int: Intervallo pubblicazione eventi in secondi (default da dashboard)"""

CONFIG_LAST_UPDATE: float = float(config.get("last_update", time.time()))
"""float: Timestamp ultimo aggiornamento configurazione (per change detection)"""

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "87.26.178.190:29092")
"""str: Indirizzo cluster Kafka (host:porta)"""

ANALYTICS_TOPIC = "analytics_ais.raw"
"""str: Topic di output per eventi analytics"""

MAIN_TOPIC = "ais.raw"
"""str: Topic input per messaggi AIS reali"""

SIM_TOPIC = "ais_simulation.raw"
"""str: Topic input per messaggi AIS simulati"""


# =============================================================================
# INIZIALIZZAZIONE FASTSTREAM
# =============================================================================

broker = KafkaBroker(BOOTSTRAP_SERVERS)
"""KafkaBroker: Istanza del broker Kafka"""

app = FastStream(broker)
"""FastStream: Applicazione principale FastStream"""

# =============================================================================
# MODELLI PYDANTIC (Schema AsyncAPI)
# =============================================================================
# Questi modelli definiscono la struttura dei messaggi per validazione,
# documentazione AsyncAPI e serializzazione JSON automatica.

class IncomingVessel(BaseModel):
    """
    Rappresenta una nave in arrivo verso una banchina.
    
    Questo modello viene usato come elemento della lista ``incoming``
    nell'evento ``BerthIncomingEvent``.
    
    Attributes
    ----------
    mmsi : str
        Maritime Mobile Service Identity - identificativo univoco nave
    eta : float
        Estimated Time of Arrival in formato Unix timestamp (secondi)
    source : str
        Topic sorgente del dato AIS:
        - "ais.raw": dati da ricevitori reali
        - "ais_simulation.raw": dati da simulatore
    
    Examples
    --------
    ::
    
        {
            "mmsi": "123456789",
            "eta": 1670000000.0,
            "source": "ais.raw"
        }
    """
    mmsi: str = Field(..., description="MMSI nave (9 cifre)")
    eta: float = Field(..., description="ETA Unix timestamp (secondi)")
    source: str = Field(..., description="Topic sorgente: 'ais.raw' | 'ais_simulation.raw'")


class BerthIncomingEvent(BaseModel):
    """
    Evento aggregato delle navi in arrivo verso una banchina/destinazione.
    
    Questo evento viene pubblicato periodicamente sul topic ``analytics_ais.raw``
    e contiene l'elenco di tutte le navi attese entro la finestra temporale
    configurata per una specifica destinazione.
    
    Attributes
    ----------
    type : str
        Tipo evento, sempre "berth_incoming"
    destination : str
        Nome della banchina/porto di destinazione (normalizzato uppercase)
    window_future_min : int
        Finestra temporale in minuti usata per il filtraggio
    incoming_vessels : int
        Numero totale di navi in arrivo (reali + simulate)
    incoming_vessels_real : int
        Numero di navi reali in arrivo (source=ais.raw)
    incoming_vessels_sim : int
        Numero di navi simulate in arrivo (source=ais_simulation.raw)
    incoming : List[IncomingVessel]
        Lista dettagliata delle navi in arrivo, ordinata per ETA crescente
    timestamp : float
        Timestamp Unix della generazione dell'evento
    
    Examples
    --------
    Evento tipico pubblicato su ``analytics_ais.raw``::
    
        {
            "type": "berth_incoming",
            "destination": "PORTO DI GENOVA",
            "window_future_min": 180,
            "incoming_vessels": 3,
            "incoming_vessels_real": 2,
            "incoming_vessels_sim": 1,
            "incoming": [
                {"mmsi": "123456789", "eta": 1670000000.0, "source": "ais.raw"},
                {"mmsi": "987654321", "eta": 1670000300.0, "source": "ais.raw"},
                {"mmsi": "111222333", "eta": 1670000600.0, "source": "ais_simulation.raw"}
            ],
            "timestamp": 1670000100.0
        }
    
    Notes
    -----
    La lista ``incoming`` è sempre ordinata per ETA crescente (prima la nave
    che arriverà prima). I conteggi ``incoming_vessels_real`` e ``incoming_vessels_sim``
    permettono di distinguere facilmente tra traffico reale e simulato.
    """
    type: str = Field("berth_incoming", description="Tipo evento (fisso: 'berth_incoming')")
    destination: str = Field(..., description="Destinazione / banchina (uppercase)")
    window_future_min: int = Field(..., description="Finestra temporale in minuti")
    incoming_vessels: int = Field(..., description="Numero totale navi in arrivo")
    incoming_vessels_real: int = Field(
        0, description="Numero navi reali (source=ais.raw)"
    )
    incoming_vessels_sim: int = Field(
        0, description="Numero navi simulate (source=ais_simulation.raw)"
    )
    incoming: List[IncomingVessel] = Field(..., description="Lista navi in arrivo (ordinate per ETA)")
    timestamp: float = Field(..., description="Timestamp generazione evento (Unix seconds)")


# -----------------------------------------------------------------------------
# Publisher Stub per documentazione AsyncAPI
# -----------------------------------------------------------------------------

@broker.publisher(ANALYTICS_TOPIC)
async def _doc_berth_incoming() -> BerthIncomingEvent:
    """Publisher stub per documentazione AsyncAPI - Eventi banchina."""
    ...

# =============================================================================
# STATO GLOBALE
# =============================================================================
# Lo stato è protetto da asyncio.Lock per garantire thread-safety
# durante l'accesso concorrente da consumer e publisher.

state_lock = asyncio.Lock()
"""asyncio.Lock: Lock per accesso thread-safe allo stato condiviso"""

ships_db: Dict[str, dict] = {}
"""
Dict[str, dict]: Database in-memory delle navi indicizzato per MMSI.

Struttura valore: tutti i campi AIS decodificati + campo "eta" calcolato
"""

berths: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
"""
Dict[str, Dict[str, Dict[str, Any]]]: Mappa banchine -> navi in arrivo.

Struttura: berths[destination][mmsi] = {"eta": float, "source": str}

Esempio:
    berths["PORTO GENOVA"]["123456789"] = {"eta": 1670000000.0, "source": "ais.raw"}
"""

multipart_buffer: Dict[tuple, dict] = {}
"""Dict[tuple, dict]: Buffer per ricomposizione messaggi NMEA multipart"""

last_cleanup = time.time()
"""float: Timestamp ultima pulizia buffer multipart"""


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
    
    Gestisce diversi formati di input:
    - Bytes UTF-8
    - Stringhe con wrapper JSON (es. da Kafka Connect)
    - Messaggi AIVDM diretti
    - Messaggi con prefissi/caratteri extra
    
    Parameters
    ----------
    raw_value : bytes | str
        Messaggio grezzo in formato bytes o stringa
    
    Returns
    -------
    str | None
        Messaggio NMEA normalizzato o None se parsing fallisce
    """
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
    Gestisce la ricomposizione di messaggi AIS multipart.
    
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
            return f"!{body}*{compute_checksum(body)}"
    except Exception:
        return None

    return None


def calculate_eta_timestamp(decoded: dict) -> Optional[float]:
    """
    Calcola l'ETA Unix timestamp dai campi AIS.
    
    Estrae i campi eta_month, eta_day, eta_hour, eta_minute dal messaggio
    AIS decodificato e li converte in Unix timestamp. Gestisce il cambio
    anno automaticamente (es. dicembre -> gennaio).
    
    Parameters
    ----------
    decoded : dict
        Dizionario con i campi AIS decodificati
    
    Returns
    -------
    float | None
        ETA in Unix timestamp (secondi) o None se non disponibile/valida
    
    Notes
    -----
    - Richiede tutti e 4 i campi (month, day, hour, minute) per essere valida
    - Valida i range: month 1-12, day 1-31, hour 0-23, minute 0-59
    - Gestisce automaticamente il passaggio anno (dicembre -> gennaio = anno+1)
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

        # Prova prima i campi con prefisso eta_, poi senza prefisso
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

        # Gestione cambio anno (se siamo a dicembre e ETA è gennaio)
        now = datetime.datetime.now()
        year = now.year + (1 if now.month == 12 and month == 1 else 0)

        return datetime.datetime(year, month, day, hour, minute).timestamp()
    except Exception:
        return None


def cleanup_multipart() -> None:
    """
    Pulizia periodica del buffer multipart.
    
    Rimuove i messaggi incompleti più vecchi di 5 secondi.
    Eseguita al massimo ogni 10 secondi per efficienza.
    """
    global last_cleanup
    now = time.time()
    if now - last_cleanup < 10:
        return

    for k, v in list(multipart_buffer.items()):
        if now - v["ts"] > 5:
            del multipart_buffer[k]

    last_cleanup = now


# =============================================================================
# CORE PROCESSOR - Elaborazione Messaggi AIS
# =============================================================================

async def process_ais(topic: str, raw_bytes: bytes) -> None:
    """
    Processa un messaggio AIS grezzo e aggiorna lo stato delle navi/banchine.
    
    Pipeline di elaborazione:
    1. Pulizia buffer multipart scaduti
    2. Normalizzazione NMEA
    3. Gestione messaggi multipart (se necessario)
    4. Decodifica AIS con pyais
    5. Estrazione MMSI, destinazione, ETA
    6. Aggiornamento database navi e mappa banchine
    
    Parameters
    ----------
    topic : str
        Topic Kafka sorgente (ais.raw | ais_simulation.raw)
    raw_bytes : bytes
        Messaggio AIS grezzo in formato NMEA
    
    Notes
    -----
    - I messaggi senza ETA valida vengono comunque salvati nel DB navi
    - Le navi con destinazione e ETA vengono aggiunte alla mappa banchine
    - I messaggi "ghost" (MMSI che inizia con "50") vengono ignorati
    """
    cleanup_multipart()

    nmea = normalize_nmea(raw_bytes)
    if not nmea or not nmea.startswith("!"):
        return

    parts = nmea.split(",")
    final = nmea

    # Gestione messaggi multipart
    if len(parts) > 5 and int(parts[1]) > 1:
        final = handle_multipart(topic, parts)

    if not final:
        return

    decoded = ais_decode(final)
    data = decoded.asdict() if hasattr(decoded, "asdict") else dict(decoded)

    mmsi = str(data.get("mmsi") or "")
    
    # Ignora messaggi "ghost" (AIS con MMSI che inizia con 50)
    if not mmsi or mmsi.startswith("50"):
        return

    # Normalizza la destinazione (uppercase, spazi singoli)
    destination = data.get("destination")
    if destination:
        destination = " ".join(destination.strip().upper().split())

    # Calcola ETA Unix timestamp
    eta = calculate_eta_timestamp(data)

    async with state_lock:
        # Aggiorna database navi
        ship = ships_db.setdefault(mmsi, {})
        ship.update(data)

        if eta is not None:
            ship["eta"] = eta

            # Se abbiamo destinazione e ETA, aggiungi alla mappa banchine
            if destination and ship.get("eta") is not None:
                berths[destination][mmsi] = {"eta": ship["eta"], "source": topic}


# =============================================================================
# SUBSCRIBER KAFKA
# =============================================================================

@broker.subscriber(MAIN_TOPIC)
async def consume_main(msg: KafkaMessage):
    """
    Subscriber per il topic AIS principale (dati reali).
    
    Parameters
    ----------
    msg : KafkaMessage
        Messaggio Kafka contenente dati AIS reali
    """
    await process_ais(MAIN_TOPIC, msg.body)
    await msg.ack()


@broker.subscriber(SIM_TOPIC)
async def consume_sim(msg: KafkaMessage):
    """
    Subscriber per il topic AIS simulazione.
    
    Parameters
    ----------
    msg : KafkaMessage
        Messaggio Kafka contenente dati AIS simulati
    """
    await process_ais(SIM_TOPIC, msg.body)
    await msg.ack()


# =============================================================================
# PUBLISHER PERIODICO
# =============================================================================

async def publisher_loop():
    """
    Loop asincrono per pubblicazione periodica eventi berth_incoming.
    
    Ogni PUBLISH_INTERVAL secondi:
    1. Calcola l'orizzonte temporale (now + WINDOW_FUTURE_MIN)
    2. Per ogni banchina, filtra le navi con ETA nell'orizzonte
    3. Genera e pubblica un evento BerthIncomingEvent per ogni banchina
    
    Notes
    -----
    - Le navi sono ordinate per ETA crescente
    - I conteggi real/sim sono calcolati separatamente
    - Il loop continua indefinitamente fino allo shutdown
    """
    log("Publisher berth_incoming avviato")

    while True:
        now = time.time()
        horizon = now + WINDOW_FUTURE_MIN * 60

        # Snapshot thread-safe dello stato banchine
        async with state_lock:
            snapshot = {d: dict(v) for d, v in berths.items()}

        for destination, ships in snapshot.items():
            # Filtra navi con ETA nella finestra temporale
            incoming = [
                IncomingVessel(mmsi=mmsi, eta=info.get("eta"), source=info.get("source"))
                for mmsi, info in ships.items()
                if now < info.get("eta", 0) <= horizon
            ]

            # Ordina per ETA crescente
            incoming.sort(key=lambda x: x.eta)

            # Conta navi real vs simulation
            real_count = sum(1 for v in incoming if v.source == MAIN_TOPIC)
            sim_count = sum(1 for v in incoming if v.source == SIM_TOPIC)

            # Costruisci e pubblica evento
            event = BerthIncomingEvent(
                destination=destination,
                window_future_min=WINDOW_FUTURE_MIN,
                incoming_vessels=len(incoming),
                incoming_vessels_real=real_count,
                incoming_vessels_sim=sim_count,
                incoming=incoming,
                timestamp=now,
            )

            await broker.publish(event, topic=ANALYTICS_TOPIC)

        await asyncio.sleep(PUBLISH_INTERVAL)


# =============================================================================
# CONFIG WATCHER - Ricaricamento Configurazione
# =============================================================================

async def config_watcher():
    """
    Task asincrono per ricaricamento automatico della configurazione.
    
    Ogni 2 minuti controlla il backend per aggiornamenti alla configurazione.
    Se il timestamp last_update è più recente, ricarica i parametri.
    
    Parametri aggiornati:
    - WINDOW_FUTURE_MIN: finestra temporale in minuti
    - PUBLISH_INTERVAL: intervallo pubblicazione in secondi
    
    Notes
    -----
    Questo permette di modificare la configurazione dalla dashboard
    senza riavviare il container Docker.
    """
    global WINDOW_FUTURE_MIN, PUBLISH_INTERVAL, CONFIG_LAST_UPDATE

    while True:
        await asyncio.sleep(120)  # Check ogni 2 minuti

        new = load_kafka_config_from_dashboard()
        last = float(new.get("last_update", 0))

        if last > CONFIG_LAST_UPDATE:
            log(f"[CONFIG] Ricaricamento configurazione...")
            log(f"[CONFIG] WINDOW_FUTURE: {WINDOW_FUTURE_MIN} -> {new['window_future']} min")
            log(f"[CONFIG] PUBLISH_INTERVAL: {PUBLISH_INTERVAL} -> {new['publish_interval']} sec")

            WINDOW_FUTURE_MIN = int(new["window_future"])
            PUBLISH_INTERVAL = int(new["publish_interval"])
            CONFIG_LAST_UPDATE = last


# =============================================================================
# LIFECYCLE HOOKS
# =============================================================================

@app.on_startup
async def startup():
    """
    Hook eseguito all'avvio dell'applicazione FastStream.
    
    Inizializza i task asincroni per:
    - Publisher periodico eventi banchina
    - Watcher configurazione
    """
    log("=" * 60)
    log("BRIDGE BANCHINA - Analytics Worker")
    log("=" * 60)
    log(f"Kafka Bootstrap: {BOOTSTRAP_SERVERS}")
    log(f"Topic Input:     {MAIN_TOPIC}, {SIM_TOPIC}")
    log(f"Topic Output:    {ANALYTICS_TOPIC}")
    log(f"Window Future:   {WINDOW_FUTURE_MIN} minuti")
    log(f"Publish Interval: {PUBLISH_INTERVAL} secondi")
    log("=" * 60)

    asyncio.create_task(publisher_loop())
    asyncio.create_task(config_watcher())
