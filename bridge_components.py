"""
Bridge Components - Worker Analytics Utilizzo Componenti
=========================================================

Descrizione
-----------
Questo modulo implementa un worker FastStream che monitora l'utilizzo dei
componenti macchina delle navi basandosi sulla velocità rilevata dai
messaggi AIS.

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
Il sistema traccia i componenti restituiti dinamicamente dal backend per ogni
MMSI tramite l'endpoint ``/componente/by_mmsi/{mmsi}``.

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
from typing import Any, Dict, Literal, Optional, Tuple

import requests
from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from faststream.specification import AsyncAPI, Contact, Tag
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

API_BASE = os.getenv("API_BASE", "http://87.26.178.190:25080")
"""str: URL base dell'API backend"""

COMPONENT_API_TIMEOUT_SEC = float(os.getenv("COMPONENT_API_TIMEOUT_SEC", "10"))
"""float: Timeout HTTP per il recupero componenti dal backend"""

EMPTY_COMPONENTS_RETRY_SEC = float(os.getenv("EMPTY_COMPONENTS_RETRY_SEC", "60"))
"""float: Intervallo di retry per MMSI con lista componenti vuota"""

COMPONENT_CACHE_TTL_SEC = float(os.getenv("COMPONENT_CACHE_TTL_SEC", "300"))
"""float: TTL cache componenti (5 min) — forza re-fetch periodico dal DB"""


# =============================================================================
# INIZIALIZZAZIONE FASTSTREAM
# =============================================================================

broker = KafkaBroker(BOOTSTRAP_SERVERS)
"""KafkaBroker: Istanza del broker Kafka"""

_spec = AsyncAPI(
    broker,
    title="Bridge Components",
    version="2.0.0",
    description=(
        "**Worker di tracciamento utilizzo componenti di bordo per nave.**\n\n"
        "Consuma posizioni AIS grezze dai topic `ais.raw` (traffico reale) e `ais_simulation.raw` "
        "(traffico simulato). Per ogni nave con SOG > 0.1 nodi, interroga il backend REST "
        "(`GET /componente/by_mmsi/{mmsi}`) per ottenere la lista dei componenti installati.\n\n"
        "Accumula il tempo di utilizzo (`usage_seconds_total`) per ciascuna coppia "
        "(MMSI, componente) e ogni `PUBLISH_INTERVAL_SEC` secondi pubblica su `analytics_ais.raw` "
        "un evento `ComponentUsageEvent` per ogni componente attivo.\n\n"
        "**Architettura event bus:**\n"
        "```\n"
        "ais.raw             ──┐\n"
        "                      ├──► bridge-components ──► analytics_ais.raw\n"
        "ais_simulation.raw  ──┘          │\n"
        "                                 └──► GET /componente/by_mmsi/{mmsi}\n"
        "```\n\n"
        "**Parametri chiave:**\n"
        "- `PUBLISH_INTERVAL_SEC` — intervallo emissione eventi (da dashboard)\n"
        "- `COMPONENT_CACHE_TTL_SEC` — TTL cache componenti (default 300s)\n"
        "- `EMPTY_COMPONENTS_RETRY_SEC` — retry per MMSI senza componenti (default 60s)\n"
        "- `COMPONENT_API_TIMEOUT_SEC` — timeout HTTP verso backend (default 10s)\n\n"
        "**Consumer di `analytics_ais.raw`:** backend per storico utilizzo componenti, dashboard manutenzione."
    ),
    tags=[
        Tag(
            name="Componenti Bordo",
            description=(
                "Tracciamento tempo di attività per ciascun componente installato su ogni nave. "
                "Un componente è 'attivo' quando la nave è in movimento (SOG > 0.1 nodi)."
            ),
        ),
        Tag(
            name="AIS Reale",
            description="Messaggi NMEA/AIVDM dal traffico marittimo reale via topic `ais.raw`.",
        ),
        Tag(
            name="AIS Simulazione",
            description=(
                "Messaggi AIS generati dal simulatore via topic `ais_simulation.raw`. "
                "Consente test dell'accumulo ore senza navi reali."
            ),
        ),
    ],
    contact=Contact(name="Team AIS Analytics"),
)

app = FastStream(broker, specification=_spec)
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
    "components": {           # Stato componenti dinamici
        "nome_componente": {"usage_total": float, "active": bool}
    },
    "source": str,            # Topic sorgente
    "mmsi": str               # MMSI nave
}
"""

component_cache: Dict[str, dict] = {}
"""
Dict[str, dict]: Cache componenti per MMSI.

Struttura valore:
{
    "components": list[str],   # Componenti recuperati da backend
    "fetched_at": float        # Timestamp ultimo fetch
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

    Attributes
    ----------
    * `type` : Literal["component_usage"] - Tipo evento, sempre "component_usage"
    * `mmsi` : str - MMSI della nave
    * `component` : str - Nome del componente (dinamico dal backend)
    * `usage_seconds_total` : int - Tempo totale di utilizzo in secondi (cumulativo)
    * `active` : bool - True se il componente è attualmente attivo (nave in movimento)
    * `source` : str - Topic sorgente (ais.raw | ais_simulation.raw)
    * `timestamp` : float - Timestamp Unix della generazione evento
    
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
    component: str = Field(..., description="Nome componente (dinamico dal backend)")
    usage_seconds_total: int = Field(..., description="Secondi totali di utilizzo (cumulativo)")
    active: bool = Field(..., description="Componente attualmente attivo")
    source: str = Field(..., description="Topic sorgente: ais.raw | ais_simulation.raw")
    timestamp: float = Field(..., description="Timestamp evento (Unix seconds)")


# -----------------------------------------------------------------------------
# Publisher Stub per documentazione AsyncAPI
# -----------------------------------------------------------------------------

@broker.publisher(
    OUTPUT_TOPIC,
    description=(
        "Pubblica eventi `ComponentUsageEvent` JSON su **`analytics_ais.raw`**.\n\n"
        "Ogni evento rappresenta il tempo di utilizzo accumulato per un singolo componente "
        "installato su una nave. La pubblicazione avviene ogni `PUBLISH_INTERVAL_SEC` secondi "
        "(da dashboard) e genera un evento separato per ciascuna coppia (MMSI, componente) "
        "attiva (SOG > 0.1 nodi).\n\n"
        "I dati componente vengono recuperati via REST (`GET /componente/by_mmsi/{mmsi}`) con "
        "cache TTL `COMPONENT_CACHE_TTL_SEC` (default 300s) per ridurre il carico sul backend.\n\n"
        "**Consumer tipici:**\n"
        "- Backend per storico utilizzo e report manutenzione\n"
        "- Dashboard ore operative per componente\n"
        "- Sistemi di manutenzione predittiva\n\n"
        "**Payload chiave:** `mmsi`, `component`, `usage_seconds_total`, `active`, "
        "`source` (`real` | `simulation`), `timestamp`.\n\n"
        "**Latenza tipica:** < 150 ms (include lookup cache componenti)."
    ),
)
async def _doc_component_usage() -> ComponentUsageEvent:
    """Publisher stub — analytics_ais.raw (utilizzo componenti bordo)."""
    ...


# =============================================================================
# LOGICA DI DOMINIO
# =============================================================================

def _dedupe_preserve_order(values: list[str]) -> list[str]:
    """Rimuove duplicati preservando l'ordine originale."""
    seen = set()
    result = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def extract_component_names(payload: Any) -> list[str]:
    """
    Estrae in modo robusto i nomi componenti dalla risposta API.

    Supporta payload in forma lista o dizionario.
    La chiave primaria attesa e' ``nome_componente``.
    """
    if payload is None:
        return []

    component_name_keys = (
        "nome_componente",
        "nome",
        "name",
        "codice",
        "code",
        "component",
        "componente",
    )
    container_keys = ("components", "componenti", "data", "items", "rows")

    def parse_node(node: Any) -> list[str]:
        if isinstance(node, str):
            name = node.strip()
            return [name] if name else []

        if isinstance(node, list):
            out: list[str] = []
            for item in node:
                out.extend(parse_node(item))
            return out

        if isinstance(node, dict):
            out: list[str] = []

            for key in component_name_keys:
                value = node.get(key)
                if isinstance(value, str) and value.strip():
                    out.append(value.strip())

            for key in container_keys:
                if key in node:
                    out.extend(parse_node(node.get(key)))

            return out

        return []

    return _dedupe_preserve_order(parse_node(payload))


def fetch_components_for_mmsi(mmsi: str) -> list[str]:
    """Recupera i componenti della nave dall'API backend."""
    try:
        url = f"{API_BASE}/componente/by_mmsi/{mmsi}"
        response = requests.get(url, timeout=COMPONENT_API_TIMEOUT_SEC)
        if response.status_code != 200:
            print(f"[COMPONENTS API] HTTP {response.status_code} su MMSI={mmsi}")
            return []

        return extract_component_names(response.json())
    except Exception as exc:
        print(f"[COMPONENTS API] Errore recupero componenti MMSI={mmsi}: {exc}")
        return []


async def get_components_for_mmsi(mmsi: str) -> list[str]:
    """
    Restituisce i componenti per MMSI usando cache in-memory con TTL.

    Se la cache è assente o scaduta (COMPONENT_CACHE_TTL_SEC), fa fetch immediato.
    Se la cache esiste ma è vuota, ritenta dopo EMPTY_COMPONENTS_RETRY_SEC.
    """
    now = time.time()
    async with state_lock:
        cached = component_cache.get(mmsi)

        if cached and cached.get("components"):
            age = now - float(cached.get("fetched_at", 0.0))
            if age < COMPONENT_CACHE_TTL_SEC:
                return list(cached["components"])
            # TTL scaduto: ri-fetch anche se non vuoto

        should_retry_empty = bool(
            cached
            and not cached.get("components")
            and (now - float(cached.get("fetched_at", 0.0)) >= EMPTY_COMPONENTS_RETRY_SEC)
        )

        if cached and not cached.get("components") and not should_retry_empty:
            return []

    components = await asyncio.to_thread(fetch_components_for_mmsi, mmsi)
    print(f"[COMP FETCH] mmsi={mmsi} → {len(components)} components: {components[:3]}...", flush=True)

    async with state_lock:
        component_cache[mmsi] = {
            "components": list(components),
            "fetched_at": time.time(),
        }

    return components

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
    
    Gestisce diversi formati di input:
    - dict (da FastStream auto-deserializzazione JSON, es. simulazione)
    - Bytes UTF-8
    - Stringhe con wrapper JSON (es. da Kafka Connect)
    - Messaggi AIVDM diretti
    
    Parameters
    ----------
    raw_value : dict | bytes | str
        Messaggio grezzo
    
    Returns
    -------
    str | None
        Messaggio normalizzato o None se parsing fallisce
    """
    try:
        # Gestione input dict (tipico da ais_simulation.raw deserializzato da FastStream)
        if isinstance(raw_value, dict):
            fields = raw_value.get("fields")
            if isinstance(fields, dict):
                v = fields.get("value")
                if isinstance(v, str):
                    raw_value = v
            if isinstance(raw_value, dict):
                v = raw_value.get("value")
                if isinstance(v, str):
                    raw_value = v

        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8", errors="ignore")

        if not isinstance(raw_value, str):
            return None

        raw_value = raw_value.strip()

        if raw_value.startswith("{"):
            try:
                data = json.loads(raw_value)
                if "fields" in data and "value" in data["fields"]:
                    return data["fields"]["value"]
                if "value" in data and isinstance(data["value"], str):
                    return data["value"]
            except Exception:
                pass

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

async def process_ais_message(topic: str, raw_bytes) -> None:
    """
    Processa un messaggio AIS e aggiorna lo stato dei componenti.
    
    Pipeline di elaborazione:
    1. Normalizzazione NMEA
    2. Skip multipart (Type 5 etc.) — serve solo speed da Type 1-3
    3. Decodifica AIS con pyais
    4. Estrazione velocità (SOG)
    5. Aggiornamento stato componenti
    
    Parameters
    ----------
    topic : str
        Topic Kafka sorgente (usato come parte della chiave stato)
    raw_bytes : bytes | dict | str
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

    # Skip multipart bundled (Type 5 etc.) — bridge_components usa solo speed (Type 1-3)
    lines = [l.strip() for l in raw.split("\n") if l.strip().startswith("!AIVDM")]
    if len(lines) > 1:
        return

    # Skip singoli frammenti multipart
    parts = raw.split(",")
    if len(parts) > 5:
        try:
            if int(parts[1]) > 1:
                return
        except Exception:
            pass

    try:
        decoded = ais_decode(raw)
        data = decoded.asdict()
    except Exception:
        return

    # Richiede il campo speed per il tracking componenti
    if "speed" not in data:
        return

    mmsi = str(data.get("mmsi") or "")
    if not mmsi:
        return

    components = await get_components_for_mmsi(mmsi)
    if not components:
        return

    now = time.time()

    # Chiave composta: (topic, mmsi) per separare real/simulation
    key: ShipKey = (topic, mmsi)

    async with state_lock:
        is_new = key not in ships
        # Inizializza nave se non esiste
        ship = ships.setdefault(
            key,
            {
                "ais": {},
                "last_update_ts": now,
                "components": {c: {"usage_total": 0.0, "active": False} for c in components},
                "source": topic,
                "mmsi": mmsi,
            },
        )

        if is_new:
            print(f"[SHIP INIT] mmsi={mmsi} topic={topic} components={len(components)}: {components[:5]}...", flush=True)
        else:
            # Aggiunge componenti mancanti senza azzerare i contatori esistenti
            added = []
            for c in components:
                if c not in ship["components"]:
                    ship["components"][c] = {"usage_total": 0.0, "active": False}
                    added.append(c)
            if added:
                print(f"[SHIP UPDATE] mmsi={mmsi} added {len(added)} new components: {added[:5]}", flush=True)

        # Aggiorna stato AIS e componenti
        ship["ais"] = data
        update_component_usage(ship, now)

# =============================================================================
# SUBSCRIBER KAFKA
# =============================================================================

@broker.subscriber(
    MAIN_TOPIC,
    description=(
        "Consuma messaggi NMEA/AIVDM grezzi dal topic **`ais.raw`** (traffico AIS reale).\n\n"
        "Per ogni messaggio: decodifica il frame NMEA tramite `pyais`, verifica se la nave è in "
        "movimento (SOG > 0.1 nodi), recupera la lista componenti installati via REST "
        "(`GET /componente/by_mmsi/{mmsi}`) con cache TTL `COMPONENT_CACHE_TTL_SEC`, e aggiorna "
        "il contatore `usage_seconds_total` per ciascun componente attivo.\n\n"
        "**Altri consumer dello stesso topic:**\n"
        "- `decoder-ais-faststream` (decodifica su `ais_decoded.raw`)\n"
        "- `bridge-banchina` (aggregazione per banchina)\n"
        "- `bridge-deltaeta` (calcolo delta ETA)\n"
        "- `bridge-arrivo` (geofencing arrivo)\n\n"
        "**Formato messaggio:** stringa NMEA AIVDM.\n\n"
        "**Frequenza tipica:** 1–10 msg/s in condizioni normali di traffico portuale."
    ),
)
async def consume_main(msg: KafkaMessage):
    """Subscriber ais.raw — dati AIS reali per tracciamento componenti."""
    await process_ais_message(MAIN_TOPIC, msg.body)
    await msg.ack()


@broker.subscriber(
    SIM_TOPIC,
    description=(
        "Consuma messaggi AIS simulati dal topic **`ais_simulation.raw`**.\n\n"
        "Identico al canale reale nel tracciamento componenti. Consente di simulare l'accumulo "
        "ore operative dei componenti senza navi reali. Gli eventi prodotti avranno "
        "`source: simulation`.\n\n"
        "**Formato messaggio:** JSON con campo `body` contenente frame AIVDM, oppure stringa NMEA "
        "diretta (normalizzazione automatica).\n\n"
        "**Altri consumer dello stesso topic:**\n"
        "- `decoder-ais-faststream` (decodifica simulazione)\n"
        "- `bridge-banchina` (aggregazione simulazione)\n"
        "- `bridge-deltaeta` (delta ETA simulazione)\n"
        "- `bridge-arrivo` (geofencing simulazione)\n\n"
        "**Frequenza tipica:** controllata dal simulatore, configurabile via dashboard."
    ),
)
async def consume_sim(msg: KafkaMessage):
    """Subscriber ais_simulation.raw — dati AIS simulati per tracciamento componenti."""
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
    Per N navi e C componenti medi, vengono pubblicati N*C eventi per ciclo.
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
    print(f"API Backend:       {API_BASE}")
    print("Componenti:        dinamici da /componente/by_mmsi/{mmsi}")
    print(f"Publish Interval:  {PUBLISH_INTERVAL_SEC} secondi")
    print("=" * 60)

    asyncio.create_task(publish_loop())
    asyncio.create_task(config_watcher())
