"""
Bridge Components - Worker Analytics Utilizzo Componenti
=========================================================

Descrizione
-----------
Questo modulo implementa un worker FastStream che monitora l'utilizzo dei
componenti macchina delle navi (motore principale, generatore, cambio)
basandosi sulla velocità rilevata dai messaggi AIS.

Funzionalità Principale
-----------------------
Traccia il tempo di utilizzo dei componenti per ogni nave. Un componente
è considerato "attivo" quando la nave ha velocità > 0.1 nodi (SOG - Speed Over Ground).
Pubblica periodicamente eventi ``component_usage`` con il tempo totale di utilizzo.

Architettura del Flusso Dati
----------------------------
::

    ┌─────────────────────┐                                    
    │     ais.raw         │ ──┐                                
    │  (NMEA grezzo)      │   │    ┌────────────────────────┐     ┌─────────────────────────┐
    └─────────────────────┘   ├──► │  Bridge Components     │ ──► │   analytics_ais.raw     │
                              │    │                        │     │                         │
    ┌─────────────────────┐   │    │  - Tracking velocità   │     │  Eventi:                │
    │  ais_simulation.raw │ ──┘    │  - Calcolo utilizzo    │     │  - component_usage      │
    │  (NMEA grezzo)      │        │  - Publishing ciclico  │     │                         │
    └─────────────────────┘        └────────────────────────┘     └─────────────────────────┘

Topic Kafka
-----------
**Input (Subscription):**
    - ``ais.raw``: Messaggi AIS reali in formato NMEA
    - ``ais_simulation.raw``: Messaggi AIS simulati in formato NMEA

**Output (Publishing):**
    - ``analytics_ais.raw``: Eventi analytics (tipo ``component_usage``)

Componenti Monitorati
---------------------
Il sistema traccia tre componenti principali per ogni nave:

- ``engine_main``: Motore principale - attivo quando la nave è in movimento
- ``generator``: Generatore - attivo quando la nave è in movimento
- ``gearbox``: Cambio - attivo quando la nave è in movimento

Logica di Attivazione
---------------------
Un componente è considerato attivo quando::

    SOG (Speed Over Ground) > 0.1 nodi

Questo threshold basso permette di rilevare anche movimenti lenti
(es. manovre in porto) escludendo solo la deriva GPS.

Gestione Stato Real vs Simulation
---------------------------------
Lo stato delle navi è indicizzato per chiave composta ``(topic_sorgente, mmsi)``.
Questo garantisce che una stessa nave simulata e reale mantengano
contatori di utilizzo separati.

Dipendenze
----------
- ``faststream``: Framework per streaming Kafka
- ``pyais``: Libreria per decodifica messaggi AIS
- ``pydantic``: Validazione e serializzazione dati
- ``config_loader``: Modulo per caricamento configurazione

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
from typing import Dict, Literal, Optional, Tuple

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

PUBLISH_INTERVAL_SEC: int = int(config["publish_interval"])
"""int: Intervallo pubblicazione eventi in secondi"""

CONFIG_LAST_UPDATE: float = float(config.get("last_update", time.time()))
"""float: Timestamp ultimo aggiornamento configurazione"""

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "87.26.178.190:29092")
"""str: Indirizzo cluster Kafka"""

MAIN_TOPIC = "ais.raw"
"""str: Topic input per messaggi AIS reali"""

SIM_TOPIC = "ais_simulation.raw"
"""str: Topic input per messaggi AIS simulati"""

OUTPUT_TOPIC = "analytics_ais.raw"
"""str: Topic output per eventi analytics"""

COMPONENTS = ["engine_main", "generator", "gearbox"]
"""list[str]: Lista dei componenti monitorati per ogni nave"""


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

La chiave è una tupla (topic, mmsi) per mantenere separati 
lo stato delle navi reali e simulate con lo stesso MMSI.

Struttura valore:
{
    "ais": dict,              # Ultimi dati AIS decodificati
    "last_update_ts": float,  # Timestamp ultimo aggiornamento
    "components": {           # Stato componenti
        "engine_main": {"usage_total": float, "active": bool},
        "generator": {"usage_total": float, "active": bool},
        "gearbox": {"usage_total": float, "active": bool}
    },
    "source": str,            # Topic sorgente
    "mmsi": str               # MMSI nave
}
"""

# =============================================================================
# MODELLI PYDANTIC (Schema AsyncAPI)
# =============================================================================

class ComponentUsageEvent(BaseModel):
    """
    Evento di utilizzo componente pubblicato su analytics_ais.raw.
    
    Rappresenta lo stato corrente e il tempo di utilizzo cumulativo
    di un singolo componente macchina di una nave.
    
    Examples
    --------
    Evento tipico per motore principale attivo::
    
        {
            "type": "component_usage",
            "mmsi": "123456789",
            "component": "engine_main",
            "usage_seconds_total": 3600,
            "active": true,
            "source": "ais.raw",
            "timestamp": 1670000100.0
        }
    
    Notes
    -----
    Viene pubblicato un evento separato per ogni componente di ogni nave.
    Il campo ``usage_seconds_total`` è cumulativo dalla prima rilevazione.
    """
    type: Literal["component_usage"] = Field("component_usage", description="Tipo evento")
    mmsi: str = Field(..., description="MMSI nave (9 cifre)")
    component: str = Field(..., description="Nome componente: engine_main | generator | gearbox")
    usage_seconds_total: int = Field(..., description="Secondi totali di utilizzo (cumulativo)")
    active: bool = Field(..., description="Componente attualmente attivo")
    source: str = Field(..., description="Topic sorgente: ais.raw | ais_simulation.raw")
    timestamp: float = Field(..., description="Timestamp evento (Unix seconds)")


# -----------------------------------------------------------------------------
# Publisher Stub per documentazione AsyncAPI
# -----------------------------------------------------------------------------

@broker.publisher(OUTPUT_TOPIC)
async def _doc_component_usage() -> ComponentUsageEvent:
    """Publisher stub per documentazione AsyncAPI."""
    ...


# =============================================================================
# LOGICA DI DOMINIO
# =============================================================================

def is_component_active(component: str, ais_state: dict) -> bool:
    """
    Determina se un componente è attivo basandosi sullo stato AIS.
    
    Un componente è considerato attivo quando la nave è in movimento,
    ovvero quando la velocità (SOG - Speed Over Ground) supera 0.1 nodi.
    
    Parameters
    ----------
    component : str
        Nome del componente (non usato nella logica attuale, ma disponibile
        per logiche differenziate future)
    ais_state : dict
        Dizionario con i dati AIS decodificati (deve contenere "speed")
    
    Returns
    -------
    bool
        True se il componente è attivo, False altrimenti
    
    Notes
    -----
    Il threshold di 0.1 nodi esclude la deriva GPS mantenendo
    la sensibilità per manovre lente in porto.
    
    Esempio logica futura differenziata (non implementata):
    - engine_main: attivo se SOG > 0.1
    - generator: sempre attivo se nave operativa
    - gearbox: attivo se SOG > 1.0 (navigazione effettiva)
    """
    sog = ais_state.get("speed")
    return sog is not None and sog > 0.1


def update_component_usage(ship: dict, now: float) -> None:
    """
    Aggiorna il tempo di utilizzo dei componenti di una nave.
    
    Calcola il delta temporale dall'ultimo aggiornamento e incrementa
    i contatori di utilizzo per i componenti attivi.
    
    Parameters
    ----------
    ship : dict
        Dizionario dello stato nave (modificato in-place)
    now : float
        Timestamp Unix corrente
    
    Notes
    -----
    Questa funzione modifica ``ship`` in-place:
    - Aggiorna ``last_update_ts`` al timestamp corrente
    - Incrementa ``usage_total`` per ogni componente attivo
    - Aggiorna lo stato ``active`` di ogni componente
    """
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


# =============================================================================
# CORE PROCESSOR - Elaborazione Messaggi AIS
# =============================================================================

async def process_ais_message(topic: str, raw_bytes: bytes) -> None:
    """
    Processa un messaggio AIS e aggiorna lo stato dei componenti.
    
    Pipeline di elaborazione:
    1. Normalizzazione NMEA
    2. Decodifica AIS con pyais
    3. Estrazione velocità (SOG)
    4. Aggiornamento stato componenti
    
    Parameters
    ----------
    topic : str
        Topic Kafka sorgente (usato come parte della chiave stato)
    raw_bytes : bytes
        Messaggio AIS grezzo
    
    Notes
    -----
    Lo stato è indicizzato per ``(topic, mmsi)`` così navi reali e simulate
    con lo stesso MMSI mantengono contatori separati.
    
    I messaggi senza campo ``speed`` vengono ignorati.
    """
    raw = normalize_nmea(raw_bytes)
    if not raw:
        return

    decoded = ais_decode(raw)
    data = decoded.asdict()

    # Richiede il campo speed per il tracking componenti
    if "speed" not in data:
        return

    mmsi = str(data.get("mmsi") or "")
    if not mmsi:
        return

    now = time.time()

    # Chiave composta: (topic, mmsi) per separare real/simulation
    key: ShipKey = (topic, mmsi)

    async with state_lock:
        # Inizializza nave se non esiste
        ship = ships.setdefault(
            key,
            {
                "ais": {},
                "last_update_ts": now,
                "components": {c: {"usage_total": 0.0, "active": False} for c in COMPONENTS},
                "source": topic,
                "mmsi": mmsi,
            },
        )

        # Aggiorna stato AIS e componenti
        ship["ais"] = data
        update_component_usage(ship, now)

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
    await process_ais_message(MAIN_TOPIC, msg.body)
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
    await process_ais_message(SIM_TOPIC, msg.body)
    await msg.ack()


# =============================================================================
# PUBLISHER PERIODICO
# =============================================================================

async def publish_loop():
    """
    Loop asincrono per pubblicazione periodica eventi component_usage.
    
    Ogni PUBLISH_INTERVAL_SEC secondi:
    1. Crea snapshot dello stato navi
    2. Per ogni nave e componente, genera evento ComponentUsageEvent
    3. Pubblica su analytics_ais.raw
    
    Notes
    -----
    Viene pubblicato un evento separato per ogni combinazione (nave, componente).
    Per N navi e 3 componenti, vengono pubblicati N*3 eventi per ciclo.
    """
    while True:
        await asyncio.sleep(PUBLISH_INTERVAL_SEC)
        now = time.time()

        # Snapshot thread-safe dello stato
        async with state_lock:
            snapshot = {
                key: {
                    "mmsi": ship.get("mmsi"),
                    "components": {c: dict(state) for c, state in ship["components"].items()},
                    "source": ship.get("source"),
                }
                for key, ship in ships.items()
            }

        # Pubblica un evento per ogni componente di ogni nave
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


# =============================================================================
# CONFIG WATCHER - Ricaricamento Configurazione
# =============================================================================

async def config_watcher():
    """
    Task asincrono per ricaricamento automatico della configurazione.
    
    Controlla ogni 2 minuti il backend per aggiornamenti e ricarica
    il parametro PUBLISH_INTERVAL_SEC se modificato.
    """
    global PUBLISH_INTERVAL_SEC, CONFIG_LAST_UPDATE

    while True:
        await asyncio.sleep(120)

        new_config = load_kafka_config_from_dashboard()
        last_update = float(new_config.get("last_update", 0))

        if last_update > CONFIG_LAST_UPDATE:
            print(
                f"[CONFIG] Aggiornamento: "
                f"PUBLISH_INTERVAL {PUBLISH_INTERVAL_SEC} -> {new_config['publish_interval']} sec"
            )

            PUBLISH_INTERVAL_SEC = int(new_config["publish_interval"])
            CONFIG_LAST_UPDATE = last_update


# =============================================================================
# LIFECYCLE HOOKS
# =============================================================================

@app.on_startup
async def startup():
    """
    Hook eseguito all'avvio dell'applicazione FastStream.
    
    Inizializza i task asincroni per:
    - Publisher periodico eventi componenti
    - Watcher configurazione
    """
    print("=" * 60)
    print("BRIDGE COMPONENTS - Analytics Worker")
    print("=" * 60)
    print(f"Kafka Bootstrap:   {BOOTSTRAP_SERVERS}")
    print(f"Topic Input:       {MAIN_TOPIC}, {SIM_TOPIC}")
    print(f"Topic Output:      {OUTPUT_TOPIC}")
    print(f"Componenti:        {', '.join(COMPONENTS)}")
    print(f"Publish Interval:  {PUBLISH_INTERVAL_SEC} secondi")
    print("=" * 60)

    asyncio.create_task(publish_loop())
    asyncio.create_task(config_watcher())
