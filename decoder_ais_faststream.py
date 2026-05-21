"""
AIS Decoder FastStream Worker
=============================

Descrizione
-----------
Worker FastStream di decodifica messaggi AIS NMEA/AIVDM. Consuma messaggi grezzi
dai topic Kafka ``ais.raw`` e ``ais_simulation.raw``, decodifica i frame con la
libreria ``pyais`` e pubblica eventi JSON strutturati sui topic
``ais_decoded.raw`` e ``ais_decoded_simulation.raw``.

Ecosistema Event Bus
--------------------
Di seguito la mappa completa dei producer/consumer per ogni topic::

                        ┌─────────────────────────────────────────────────────────────────┐
                        │                    KAFKA EVENT BUS                              │
                        └─────────────────────────────────────────────────────────────────┘

   [AIS Receiver HW]                                                   [Backend / Dashboard]
    AIS Connector  ──►  ais.raw  ──┬──► decoder-ais-main ──► ais_decoded.raw  ──►  consumer
                                   ├──► bridge-banchina    (berth ETA analytics)
                                   ├──► bridge-components  (component usage tracking)
                                   ├──► bridge-delta-eta   (ETA delta calculation)
                                   └──► bridge-arrivo      (arrival geofencing)

   [Simulator]                                                         [Backend / Dashboard]
    AIS Sim  ──►  ais_simulation.raw  ──┬──► decoder-ais-sim ──► ais_decoded_simulation.raw ──► consumer
                                        ├──► bridge-banchina
                                        ├──► bridge-components
                                        ├──► bridge-delta-eta
                                        └──► bridge-arrivo

Topic Kafka — Dettaglio
-----------------------
+------------------------------+---------------------------+--------------------------------------+
| Topic                        | Producer(s)               | Consumer(s)                          |
+==============================+===========================+======================================+
| ais.raw                      | AIS Receiver / Connector  | decoder-ais-main,                    |
|                              |                           | bridge-banchina,                     |
|                              |                           | bridge-components,                   |
|                              |                           | bridge-delta-eta,                    |
|                              |                           | bridge-arrivo                        |
+------------------------------+---------------------------+--------------------------------------+
| ais_simulation.raw           | AIS Simulator             | decoder-ais-sim,                     |
|                              |                           | bridge-banchina,                     |
|                              |                           | bridge-components,                   |
|                              |                           | bridge-delta-eta,                    |
|                              |                           | bridge-arrivo                        |
+------------------------------+---------------------------+--------------------------------------+
| ais_decoded.raw              | decoder-ais-main          | Backend applicativo, dashboard,      |
|                              | (questo servizio)         | client websocket (navi reali)        |
+------------------------------+---------------------------+--------------------------------------+
| ais_decoded_simulation.raw   | decoder-ais-sim           | Backend applicativo, dashboard,      |
|                              | (questo servizio)         | client websocket (simulazione)       |
+------------------------------+---------------------------+--------------------------------------+

Caratteristiche della Messaggistica
------------------------------------
**Input (ais.raw / ais_simulation.raw)**

- Formato: frame NMEA/AIVDM in testo ASCII, es. ``!AIVDM,1,1,,B,15N4cJ`000rk3HH,0*37``
- Frequenza reale: continua, ~0.3-2 msg/sec per nave attiva nell'area
- Frequenza simulata: configurabile, tipicamente 1-10 msg/sec per nave
- Chiave Kafka: assente o byte arbitrari
- Payload: bytes UTF-8 (stringa NMEA) oppure JSON wrapper da Kafka Connect
  (es. ``{"fields": {"value": "!AIVDM,..."}}`` )
- Messaggi multipart: i Type 5 arrivano come 2 frammenti separati su Kafka
  (buffer intra-worker) oppure bundled ``\\n``-separated in un unico messaggio

**Output (ais_decoded.raw / ais_decoded_simulation.raw)**

- Formato: JSON (AisDecodedEvent serializzato)
- Latenza decodifica: < 50 ms tipico, < 500 ms nel 99° percentile
- Chiave Kafka: MMSI in bytes → ordine garantito per-nave all'interno della stessa partizione
- Retention: dipende dalla configurazione Kafka del cluster (default 7 giorni)

Modalità Operative
------------------
Variabile d'ambiente ``DECODER_MODE``:

+-------+--------------------------------------------------+---------------------------------+
| Valore | Subscriber attivi                               | Uso tipico                      |
+=======+==================================================+=================================+
| all   | ais.raw + ais_simulation.raw                    | Sviluppo / test locale          |
+-------+--------------------------------------------------+---------------------------------+
| main  | solo ais.raw                                    | Container produzione AIS reale  |
+-------+--------------------------------------------------+---------------------------------+
| sim   | solo ais_simulation.raw                         | Container dedicato simulazione  |
+-------+--------------------------------------------------+---------------------------------+

In produzione vengono eseguiti **due container separati** (``decoder-ais-main`` e
``decoder-ais-sim``) per isolare i loop asincroni ed eliminare la contesa di
risorse tra traffico reale e simulato.

Gestione Messaggi Multipart
---------------------------
I messaggi AIS Tipo 5 (dati statici nave) superano la dimensione massima NMEA
e vengono divisi in 2 frammenti. Questo worker gestisce due scenari:

1. **Bundled** — Il simulatore invia entrambi i frammenti concatenati con ``\\n``
   in un unico messaggio Kafka. → Decodifica diretta con pyais multipart.
2. **Sequenziale** — I due frammenti arrivano come messaggi Kafka separati.
   → Buffer a coda indicizzato per ``(topic, canale_radio, n_frammenti)``.
   Frammenti non ricongiunti entro ``MULTIPART_TTL_SEC`` (120s) vengono scartati.

La coda per chiave evita le collisioni causate dal campo ``seq`` (digit 0-9
condiviso tra tutti i vascelli sullo stesso canale: riutilizzato ogni 10 navi).

Dipendenze
----------
- ``faststream``: Framework per streaming Kafka
- ``pyais``: Libreria per decodifica messaggi AIS (ITU-R M.1371)
- ``pydantic``: Validazione e serializzazione dati / schema AsyncAPI

Autore: Team AIS Analytics
Versione: 2.1.0
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
from functools import reduce
from typing import Any, Dict, List, Literal, Optional

from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from faststream.specification import AsyncAPI, Contact, ExternalDocs, Tag
from pyais import decode as ais_decode
from pydantic import BaseModel, Field
import logging

logging.getLogger("faststream").setLevel(logging.WARNING)


# =============================================================================
# CONFIGURAZIONE
# =============================================================================

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")
"""str: Indirizzo del cluster Kafka (formato: host:porta). Default: localhost:29092"""

DECODER_MODE = os.getenv("DECODER_MODE", "all")
"""str: Modalità decoder: 'all' | 'main' | 'sim'. Controlla quali subscriber vengono registrati."""

MAIN_INPUT_TOPIC = "ais.raw"
"""str: Topic Kafka per messaggi AIS reali in formato NMEA grezzo."""

SIM_INPUT_TOPIC = "ais_simulation.raw"
"""str: Topic Kafka per messaggi AIS simulati in formato NMEA grezzo."""

MAIN_OUTPUT_TOPIC = "ais_decoded.raw"
"""str: Topic Kafka per messaggi AIS reali decodificati in JSON."""

SIM_OUTPUT_TOPIC = "ais_decoded_simulation.raw"
"""str: Topic Kafka per messaggi AIS simulati decodificati in JSON."""

MULTIPART_TTL_SEC: float = 120.0
"""float: TTL (secondi) dei frammenti multipart nel buffer prima dello scarto."""


# =============================================================================
# INIZIALIZZAZIONE FASTSTREAM
# =============================================================================

broker = KafkaBroker(BOOTSTRAP_SERVERS)

_spec = AsyncAPI(
    broker,
    title="AIS Decoder FastStream",
    version="2.1.0",
    description=(
        "**Worker di decodifica messaggi AIS NMEA/AIVDM.**\n\n"
        "Consuma frame grezzi da `ais.raw` e `ais_simulation.raw`, decodifica con **pyais** "
        "(ITU-R M.1371) e pubblica eventi JSON su `ais_decoded.raw` e "
        "`ais_decoded_simulation.raw`.\n\n"
        "In produzione vengono eseguiti due container separati:\n"
        "- `decoder-ais-main` — `DECODER_MODE=main`, processa solo AIS reale\n"
        "- `decoder-ais-sim` — `DECODER_MODE=sim`, processa solo simulazione\n\n"
        "**Chiave di partizione Kafka:** MMSI in bytes → ordine garantito per nave.\n\n"
        "**Latenza tipica:** < 50 ms end-to-end (normalizzazione + decodifica pyais + publish)."
    ),
    tags=[
        Tag(
            name="Decodifica AIS",
            description=(
                "Operazioni di normalizzazione NMEA, ricomposizione frame multipart "
                "e decodifica payload AIS in JSON strutturato."
            ),
        ),
        Tag(
            name="AIS Reale",
            description=(
                "Flusso dati AIS reali provenienti dal ricevitore hardware. "
                "Topic: `ais.raw` → `ais_decoded.raw`."
            ),
        ),
        Tag(
            name="AIS Simulazione",
            description=(
                "Flusso dati AIS simulati. "
                "Topic: `ais_simulation.raw` → `ais_decoded_simulation.raw`."
            ),
        ),
    ],
    contact=Contact(name="Team AIS Analytics"),
)

app = FastStream(broker, specification=_spec)


# =============================================================================
# MODELLI PYDANTIC — SCHEMA ASYNCAPI
# =============================================================================

class AisPositionPayload(BaseModel):
    """
    Payload decodificato per messaggi di posizione AIS.

    Presente nel campo ``payload`` di ``AisDecodedEvent`` quando
    ``msg_type`` è **1, 2, 3** (Class A position report) o **18** (Class B CS position).

    I messaggi di posizione sono i più frequenti: ogni nave attiva li trasmette
    ogni **2–10 secondi** (Class A) o **30 secondi** (Class B).

    Attributes
    ----------
    * `msg_type` : int
        Tipo messaggio AIS:
        - 1 = Class A position report (scheduled)
        - 2 = Class A position report (assigned)
        - 3 = Class A position report (interrogated)
        - 18 = Class B CS position report
    * `mmsi` : str
        Maritime Mobile Service Identity — identificatore univoco a 9 cifre.
        Assegnato dalla ITU per ogni stazione radio navale.
    * `status` : int, optional
        Stato di navigazione AIS (0–15):
        - 0 = In navigazione a motore
        - 1 = Alla fonda
        - 2 = Non in governo
        - 3 = Manovrabilità ridotta
        - 4 = Vincolata dal pescaggio
        - 5 = Ormeggiata
        - 6 = In secco
        - 7 = Pesca in corso
        - 8 = In navigazione a vela
        - 15 = Non definito / default
    * `turn` : float, optional
        Rate of Turn (ROT) in gradi/minuto.
        Positivo = virata a dritta, negativo = virata a sinistra.
        -128 = nessun sensore ROT disponibile.
    * `speed` : float, optional
        Speed Over Ground (SOG) in nodi. Risoluzione: 0.1 nodi. Max: 102.2 nodi.
    * `accuracy` : bool, optional
        Accuratezza posizione: ``true`` = < 10 m (DGPS), ``false`` = > 10 m.
    * `lon` : float, optional
        Longitudine in gradi decimali (negativo = Ovest). Precisione: ~11 m.
    * `lat` : float, optional
        Latitudine in gradi decimali (negativo = Sud). Precisione: ~11 m.
    * `course` : float, optional
        Course Over Ground (COG) in gradi (0.0–359.9). 360.0 = non disponibile.
    * `heading` : int, optional
        True Heading (HDG) in gradi (0–359). 511 = non disponibile.
    * `second` : int, optional
        Secondi UTC del fix posizione (0–59). 60 = timestamp non disponibile.
    * `maneuver` : int, optional
        Indicatore manovra speciale: 0 = N/A, 1 = manovra speciale, 2 = non definito.
    * `raim` : bool, optional
        Receiver Autonomous Integrity Monitoring flag.
        ``true`` = RAIM attivo (controllo integrità GPS).
    * `radio` : int, optional
        Informazione radio / stato SOTDMA (Self-Organized TDMA).

    Examples
    --------
    ::

        {
            "msg_type": 1,
            "mmsi": "247123456",
            "status": 0,
            "turn": 0,
            "speed": 12.3,
            "accuracy": true,
            "lon": 14.267,
            "lat": 40.851,
            "course": 95.0,
            "heading": 94,
            "second": 22,
            "maneuver": 0,
            "raim": false,
            "radio": 49152
        }
    """
    msg_type: int = Field(..., description="Tipo messaggio AIS (1, 2, 3 o 18)")
    mmsi: str = Field(..., description="MMSI nave — identificatore univoco 9 cifre (ITU)")
    status: Optional[int] = Field(None, description="Stato navigazione AIS (0–15, vedi spec ITU-R M.1371)")
    turn: Optional[float] = Field(None, description="Rate of Turn ROT (gradi/min). -128 = sensore assente")
    speed: Optional[float] = Field(None, description="Speed Over Ground SOG (nodi, risoluzione 0.1)")
    accuracy: Optional[bool] = Field(None, description="Accuratezza GPS: true = DGPS <10m, false = >10m")
    lon: Optional[float] = Field(None, description="Longitudine gradi decimali (negativo = Ovest)")
    lat: Optional[float] = Field(None, description="Latitudine gradi decimali (negativo = Sud)")
    course: Optional[float] = Field(None, description="Course Over Ground COG (0.0–359.9°, 360.0 = N/A)")
    heading: Optional[int] = Field(None, description="True Heading HDG (0–359°, 511 = N/A)")
    second: Optional[int] = Field(None, description="Secondi UTC del fix posizione (0–59, 60 = N/A)")
    maneuver: Optional[int] = Field(None, description="Indicatore manovra: 0=N/A, 1=speciale, 2=non def.")
    raim: Optional[bool] = Field(None, description="RAIM flag: true = controllo integrità GPS attivo")
    radio: Optional[int] = Field(None, description="Info radio / stato SOTDMA/ITDMA")


class AisStaticPayload(BaseModel):
    """
    Payload decodificato per messaggi di dati statici e di viaggio AIS.

    Presente nel campo ``payload`` di ``AisDecodedEvent`` quando
    ``msg_type`` è **5** (Class A static and voyage related data).

    I messaggi Tipo 5 sono trasmessi ogni **6 minuti** circa e contengono
    informazioni identificative della nave e della missione corrente.
    Sono messaggi **multipart** (2 frame NMEA): il worker li ricompone prima
    della decodifica.

    Attributes
    ----------
    * `msg_type` : int — Tipo messaggio AIS (5)
    * `mmsi` : str — MMSI nave (9 cifre)
    * `imo` : str, optional
        Numero IMO (International Maritime Organization) — 7 cifre, univoco per scafo.
        Invariato durante tutta la vita della nave.
    * `callsign` : str, optional
        Indicativo di chiamata radio — max 7 caratteri alfanumerici.
        Assegnato dall'autorità di telecomunicazioni dello Stato di bandiera.
    * `shipname` : str, optional
        Nome della nave — max 20 caratteri (padding con spazi).
    * `shiptype` : int, optional
        Tipo nave (codice ITU-R M.1371 Tabella 50, 0–99):
        - 0 = Non disponibile
        - 30 = Pesca
        - 36 = Vela
        - 37 = Imbarcazione da diporto
        - 50 = Pilotina
        - 60–69 = Passeggeri
        - 70–79 = Cargo
        - 80–89 = Tanker
        - 90–99 = Altro
    * `to_bow` : int, optional — Distanza trasponditore–prua (metri)
    * `to_stern` : int, optional — Distanza trasponditore–poppa (metri)
    * `to_port` : int, optional — Distanza trasponditore–sinistra (metri)
    * `to_starboard` : int, optional — Distanza trasponditore–dritta (metri)
    * `epfd` : int, optional
        Tipo dispositivo EPFD (Electronic Position Fixing Device):
        - 1 = GPS, 2 = GLONASS, 3 = GPS+GLONASS, 4 = Loran-C, 5 = Chayka, 6 = Integrated
    * `eta_month` : int, optional — ETA mese (1–12, 0 = non disponibile)
    * `eta_day` : int, optional — ETA giorno (1–31)
    * `eta_hour` : int, optional — ETA ora UTC (0–23)
    * `eta_minute` : int, optional — ETA minuti (0–59)
    * `draught` : float, optional — Pescaggio massimo corrente (metri, risoluzione 0.1 m)
    * `destination` : str, optional — Destinazione dichiarata (max 20 car., uppercase)
    * `dte` : int, optional — Data Terminal Equipment: 0 = disponibile, 1 = non disponibile

    Examples
    --------
    ::

        {
            "msg_type": 5,
            "mmsi": "247123456",
            "imo": "9876543",
            "callsign": "IABCD",
            "shipname": "NAVE ESEMPIO      ",
            "shiptype": 70,
            "to_bow": 80,
            "to_stern": 20,
            "to_port": 10,
            "to_starboard": 10,
            "epfd": 1,
            "eta_month": 6,
            "eta_day": 15,
            "eta_hour": 14,
            "eta_minute": 30,
            "draught": 6.5,
            "destination": "SALERNO         ",
            "dte": 0
        }
    """
    msg_type: int = Field(..., description="Tipo messaggio AIS (5 = Class A static & voyage)")
    mmsi: str = Field(..., description="MMSI nave — identificatore univoco 9 cifre")
    imo: Optional[str] = Field(None, description="Numero IMO (7 cifre) — univoco per scafo, invariato nel tempo")
    callsign: Optional[str] = Field(None, description="Indicativo radio (max 7 car.) — assegnato dallo Stato di bandiera")
    shipname: Optional[str] = Field(None, description="Nome nave (max 20 car., padding spazi)")
    shiptype: Optional[int] = Field(None, description="Tipo nave ITU (0–99). 70–79=Cargo, 80–89=Tanker, 60–69=Passeggeri")
    to_bow: Optional[int] = Field(None, description="Distanza trasponditore–prua (m)")
    to_stern: Optional[int] = Field(None, description="Distanza trasponditore–poppa (m)")
    to_port: Optional[int] = Field(None, description="Distanza trasponditore–sinistra (m)")
    to_starboard: Optional[int] = Field(None, description="Distanza trasponditore–dritta (m)")
    epfd: Optional[int] = Field(None, description="Tipo EPFD: 1=GPS, 2=GLONASS, 3=GPS+GLONASS, 4=Loran-C")
    eta_month: Optional[int] = Field(None, description="ETA mese (1–12, 0 = N/A)")
    eta_day: Optional[int] = Field(None, description="ETA giorno (1–31)")
    eta_hour: Optional[int] = Field(None, description="ETA ora UTC (0–23)")
    eta_minute: Optional[int] = Field(None, description="ETA minuti (0–59)")
    draught: Optional[float] = Field(None, description="Pescaggio massimo corrente (m, risoluzione 0.1 m)")
    destination: Optional[str] = Field(None, description="Destinazione dichiarata (max 20 car., uppercase)")
    dte: Optional[int] = Field(None, description="DTE: 0 = disponibile, 1 = non disponibile")


class AisDecodedEvent(BaseModel):
    """
    Evento AIS decodificato — schema pubblicato su ``ais_decoded.raw`` e
    ``ais_decoded_simulation.raw``.

    Prodotto da questo worker per ogni messaggio NMEA decodificato con successo.
    Il campo ``payload`` contiene tutti i campi restituiti da ``pyais`` (ITU-R M.1371):
    il contenuto varia in base al tipo di messaggio.

    Frequenza di produzione
    -----------------------
    - **Tipo 1/2/3** (posizione Class A): ogni 2–10 secondi per nave attiva
    - **Tipo 18** (posizione Class B): ogni 30 secondi
    - **Tipo 5** (dati statici): ogni 6 minuti circa
    - **Altri tipi**: variabile

    Chiave di partizione Kafka
    --------------------------
    Il MMSI è usato come chiave di partizione → messaggi della stessa nave
    vanno sempre nella stessa partizione → ordine cronologico garantito per-nave.

    Routing per ``source``
    ----------------------
    - ``source = "ais.raw"`` → pubblicato su ``ais_decoded.raw`` (canale reale)
    - ``source = "ais_simulation.raw"`` → pubblicato su ``ais_decoded_simulation.raw``

    Payload per tipo messaggio
    --------------------------
    - **Tipo 1, 2, 3, 18** → campi posizione (vedi ``AisPositionPayload``)
    - **Tipo 5** → campi statici e di viaggio (vedi ``AisStaticPayload``)
    - **Tipo 4** (base station), **Tipo 14** (safety broadcast),
      **Tipo 21** (aid-to-navigation), **Tipo 24** (Class B static):
      campi specifici per tipo, struttura analoga

    Attributes
    ----------
    * `type` : str — Tipo evento. Valore fisso: ``"ais_decoded"``
    * `msg_type` : int, optional — Tipo messaggio AIS (1–27)
    * `mmsi` : str, optional — MMSI nave (9 cifre)
    * `payload` : dict — Tutti i campi AIS decodificati (dipendente da msg_type)
    * `timestamp` : float — Unix timestamp (secondi) dell'istante di decodifica
    * `source` : str — Topic Kafka sorgente del messaggio grezzo

    Examples
    --------
    Posizione Class A (Tipo 1) — evento più frequente::

        {
            "type": "ais_decoded",
            "msg_type": 1,
            "mmsi": "247123456",
            "payload": {
                "msg_type": 1,
                "mmsi": "247123456",
                "status": 0,
                "turn": 0,
                "speed": 12.3,
                "accuracy": true,
                "lon": 14.267,
                "lat": 40.851,
                "course": 95.0,
                "heading": 94,
                "second": 22,
                "maneuver": 0,
                "raim": false,
                "radio": 49152
            },
            "timestamp": 1716288000.0,
            "source": "ais.raw"
        }

    Dati statici nave (Tipo 5) — ogni ~6 minuti::

        {
            "type": "ais_decoded",
            "msg_type": 5,
            "mmsi": "247123456",
            "payload": {
                "msg_type": 5,
                "mmsi": "247123456",
                "imo": "9876543",
                "callsign": "IABCD",
                "shipname": "NAVE ESEMPIO",
                "shiptype": 70,
                "to_bow": 80, "to_stern": 20, "to_port": 10, "to_starboard": 10,
                "epfd": 1,
                "eta_month": 6, "eta_day": 15, "eta_hour": 14, "eta_minute": 30,
                "draught": 6.5,
                "destination": "SALERNO",
                "dte": 0
            },
            "timestamp": 1716288360.0,
            "source": "ais.raw"
        }

    Posizione Class B simulata (Tipo 18)::

        {
            "type": "ais_decoded",
            "msg_type": 18,
            "mmsi": "338123456",
            "payload": {
                "msg_type": 18,
                "mmsi": "338123456",
                "speed": 6.2,
                "accuracy": false,
                "lon": 14.267,
                "lat": 40.851,
                "course": 95.0,
                "heading": 95,
                "raim": false
            },
            "timestamp": 1716288030.0,
            "source": "ais_simulation.raw"
        }
    """
    type: Literal["ais_decoded"] = Field(
        "ais_decoded",
        description="Tipo evento. Valore fisso: 'ais_decoded'.",
    )
    msg_type: Optional[int] = Field(
        None,
        description=(
            "Tipo messaggio AIS (1–27). "
            "Tipi più comuni: 1/2/3=posizione Class A, 5=dati statici, 18=posizione Class B."
        ),
    )
    mmsi: Optional[str] = Field(
        None,
        description=(
            "Maritime Mobile Service Identity — identificatore univoco nave (9 cifre). "
            "Usato anche come chiave di partizione Kafka."
        ),
    )
    payload: Dict[str, Any] = Field(
        ...,
        description=(
            "Payload AIS completo restituito da pyais (ITU-R M.1371). "
            "Struttura dipendente da msg_type: "
            "Tipo 1/2/3/18 → posizione (AisPositionPayload), "
            "Tipo 5 → dati statici/viaggio (AisStaticPayload)."
        ),
    )
    timestamp: float = Field(
        ...,
        description=(
            "Unix timestamp (secondi) dell'istante di decodifica sul worker. "
            "Non corrisponde al timestamp AIS originale del trasponditore."
        ),
    )
    source: str = Field(
        ...,
        description=(
            "Topic Kafka sorgente del messaggio grezzo. "
            "Valori: 'ais.raw' (reale) | 'ais_simulation.raw' (simulazione)."
        ),
    )


# =============================================================================
# PUBLISHER STUBS — documentazione AsyncAPI
# =============================================================================
# Funzioni stub vuote: non contengono logica di runtime.
# Usate da FastStream per generare automaticamente i canali di output
# nella specifica AsyncAPI. Il tipo di ritorno definisce lo schema del messaggio.

@broker.publisher(
    MAIN_OUTPUT_TOPIC,
    description=(
        "Pubblica eventi `AisDecodedEvent` JSON su **`ais_decoded.raw`** per ogni frame "
        "NMEA reale decodificato con successo da `ais.raw`.\n\n"
        "**Chiave Kafka:** MMSI in bytes → ordine per-nave garantito.\n\n"
        "**Consumer tipici di questo topic:**\n"
        "- Backend applicativo (API REST / WebSocket)\n"
        "- Dashboard real-time\n"
        "- Sistemi di archivio / data lake\n\n"
        "**Latenza tipica:** < 50 ms dalla ricezione del frame grezzo.\n\n"
        "**Attivo quando:** `DECODER_MODE = all | main`."
    ),
)
async def _doc_ais_decoded_main() -> AisDecodedEvent:
    """Publisher stub — ais_decoded.raw (canale AIS reale)."""
    ...


@broker.publisher(
    SIM_OUTPUT_TOPIC,
    description=(
        "Pubblica eventi `AisDecodedEvent` JSON su **`ais_decoded_simulation.raw`** per ogni "
        "frame NMEA simulato decodificato con successo da `ais_simulation.raw`.\n\n"
        "**Chiave Kafka:** MMSI in bytes → ordine per-nave garantito.\n\n"
        "**Consumer tipici di questo topic:**\n"
        "- Backend applicativo (visualizzazione simulazione)\n"
        "- Dashboard simulazione real-time\n"
        "- Testing e validazione algoritmi\n\n"
        "**Frequenza:** dipende dalla velocità del simulatore (configurabile).\n\n"
        "**Attivo quando:** `DECODER_MODE = all | sim`."
    ),
)
async def _doc_ais_decoded_sim() -> AisDecodedEvent:
    """Publisher stub — ais_decoded_simulation.raw (canale simulazione)."""
    ...


# =============================================================================
# STATO GLOBALE
# =============================================================================

state_lock = asyncio.Lock()
"""asyncio.Lock: Lock per accesso thread-safe al buffer multipart condiviso."""

multipart_buffer: Dict[tuple, List] = {}
"""
Dict[tuple, List[dict]]: Buffer a coda per ricomposizione messaggi NMEA multipart.

Chiave: ``(topic, canale_radio, num_frammenti_totali)``
    - ``topic``: nome del topic Kafka sorgente
    - ``canale_radio``: "A" o "B" (campo 5 NMEA)
    - ``num_frammenti_totali``: numero di frammenti attesi (tipicamente 2 per Tipo 5)

Valore: lista di entry in attesa per quella chiave::

    [
        {
            "total": 2,           # frammenti totali
            "parts": {1: "...", 2: "..."},  # indice → payload
            "ts": 1716288000.0    # timestamp ultimo frammento (per TTL)
        },
        ...  # più entry per vascelli diversi sulla stessa chiave
    ]

La struttura a **coda per chiave** evita collisioni del campo ``seq`` (0-9,
condiviso tra tutti i vascelli sullo stesso canale radio).
"""

last_cleanup = time.time()
"""float: Timestamp dell'ultima pulizia del buffer multipart."""


# =============================================================================
# FUNZIONI UTILITY
# =============================================================================

def log(msg: str) -> None:
    """Stampa un messaggio di log con timestamp HH:MM:SS e flush immediato."""
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def normalize_nmea(raw_value) -> Optional[str]:
    """
    Normalizza un messaggio NMEA grezzo in stringa AIVDM standard.

    Gestisce i seguenti formati di input:

    1. **dict** — da FastStream che auto-deserializza JSON in ingresso
       (tipico per ``ais_simulation.raw``):
       ``{"fields": {"value": "!AIVDM,..."}}``
    2. **bytes** — da topic Kafka con payload binario (encoding UTF-8/ASCII)
    3. **str JSON wrapper** — da Kafka Connect:
       ``{"fields": {"value": "!AIVDM,..."}}`` oppure ``{"value": "!AIVDM,..."}``
    4. **str AIVDM diretto** — già in formato ``!AIVDM,...``
    5. **str con prefissi** — caratteri extra prima del ``!`` (es. log con timestamp)

    Parameters
    ----------
    raw_value : dict | bytes | str
        Messaggio grezzo in qualsiasi formato supportato.

    Returns
    -------
    str | None
        Stringa NMEA normalizzata (inizia con ``!AIVDM``) oppure ``None``
        se il formato non è riconoscibile o il parsing fallisce.
    """
    try:
        # Gestione dict (FastStream auto-deserializza JSON da ais_simulation.raw)
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

        # JSON wrapper (es. Kafka Connect)
        if raw_value.startswith("{"):
            try:
                data = json.loads(raw_value)
                if "fields" in data and "value" in data["fields"]:
                    return data["fields"]["value"].encode("ascii", "ignore").decode("ascii")
                if "value" in data and isinstance(data["value"], str):
                    return data["value"]
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
    Calcola il checksum NMEA (XOR di tutti i caratteri tra '!' e '*').

    Parameters
    ----------
    body : str
        Corpo del messaggio NMEA (con o senza il prefisso '!').

    Returns
    -------
    str
        Checksum esadecimale a 2 cifre maiuscole (es. '3F').
    """
    content = body[1:] if body.startswith("!") else body
    return f"{reduce(operator.xor, (ord(c) for c in content), 0):02X}"


def handle_multipart(topic: str, parts: list) -> Optional[str]:
    """
    Ricompone messaggi AIS multipart da frammenti NMEA sequenziali.

    Strategia a **coda per chiave** ``(topic, canale, num_frammenti)``:
    - Ogni chiave mantiene una coda di entry in attesa di completamento.
    - Evita collisioni del campo ``seq`` NMEA (digit 0-9 condiviso tra
      tutti i vascelli sullo stesso canale radio).
    - Con più navi attive che trasmettono Tipo 5 contemporaneamente,
      l'approccio basato su ``seq`` produce mescolamento dei payload.

    Parameters
    ----------
    topic : str
        Nome del topic Kafka sorgente.
    parts : list
        Campi del messaggio NMEA dopo split per ",".
        Struttura: ``[tipo, totale, indice, seq, canale, payload, fill+checksum]``

    Returns
    -------
    str | None
        Messaggio NMEA ricomposto (singola riga, checksum ricalcolato) se
        tutti i frammenti sono disponibili, altrimenti ``None``.
    """
    try:
        total = int(parts[1])
        index = int(parts[2])
        chan = parts[4]
        payload = parts[5]

        queue_key = (topic, chan, total)
        queue = multipart_buffer.setdefault(queue_key, [])

        target_entry = None
        if index == 1:
            target_entry = {"total": total, "parts": {}, "ts": time.time()}
            queue.append(target_entry)
        else:
            for entry in queue:
                if index not in entry["parts"] and len(entry["parts"]) < entry["total"]:
                    target_entry = entry
                    break
            if target_entry is None:
                target_entry = {"total": total, "parts": {}, "ts": time.time()}
                queue.append(target_entry)

        target_entry["parts"][index] = payload
        target_entry["ts"] = time.time()

        if len(target_entry["parts"]) == target_entry["total"]:
            full = "".join(target_entry["parts"][i] for i in range(1, target_entry["total"] + 1))
            try:
                queue.remove(target_entry)
            except ValueError:
                pass
            if not queue:
                del multipart_buffer[queue_key]

            body = f"AIVDM,1,1,,{chan},{full},0"
            return f"!{body}*{compute_checksum(body)}"

    except Exception as e:
        log(f"[MULTIPART ERROR] {e}")

    return None


# =============================================================================
# CONTATORI DIAGNOSTICI
# =============================================================================

_decode_count_main: int = 0
_decode_count_sim: int = 0
_decode_errors: int = 0
_last_diag_ts: float = time.time()
_publish_slow_count: int = 0
_publish_total_time: float = 0.0
_publish_count: int = 0


async def _periodic_cleanup_loop() -> None:
    """
    Task asincrono periodico: cleanup buffer multipart + diagnostica.

    Avviato all'on_startup come task indipendente (non chiamato per messaggio).

    **Ogni 10 secondi:**
    - Rimuove dal buffer multipart le entry con timestamp più vecchio di
      ``MULTIPART_TTL_SEC`` (120 s). Frammenti orfani vengono scartati.

    **Ogni 30 secondi:**
    - Logga statistiche diagnostiche:
      messaggi decodificati (main/sim), errori, dimensione buffer multipart,
      publish lenti (> 0.5 s), latenza media publish.
    - Azzera i contatori per il prossimo ciclo.
    """
    global _last_diag_ts, _decode_count_main, _decode_count_sim, _decode_errors
    global _publish_slow_count, _publish_total_time, _publish_count

    while True:
        await asyncio.sleep(10)
        now = time.time()

        async with state_lock:
            for qk, queue in list(multipart_buffer.items()):
                queue[:] = [e for e in queue if now - e["ts"] <= MULTIPART_TTL_SEC]
                if not queue:
                    del multipart_buffer[qk]

        if now - _last_diag_ts >= 30:
            avg_pub = (_publish_total_time / _publish_count * 1000) if _publish_count > 0 else 0
            log(
                f"[DIAG] decoded main={_decode_count_main} sim={_decode_count_sim} "
                f"errors={_decode_errors} multipart_buf={len(multipart_buffer)} "
                f"pub_slow={_publish_slow_count} pub_avg={avg_pub:.1f}ms"
            )
            _decode_count_main = 0
            _decode_count_sim = 0
            _decode_errors = 0
            _publish_slow_count = 0
            _publish_total_time = 0.0
            _publish_count = 0
            _last_diag_ts = now


# =============================================================================
# LOGICA DI DECODIFICA E PUBBLICAZIONE
# =============================================================================

async def decode_and_publish(raw_value, source_topic: str, output_topic: str) -> None:
    """
    Decodifica un messaggio AIS grezzo e pubblica l'evento sul topic di output.

    Pipeline:

    1. **Normalizzazione** — ``normalize_nmea()`` converte qualsiasi formato
       di input in una stringa AIVDM valida.
    2. **Rilevamento bundled multipart** — se il messaggio contiene più righe
       ``\\n``-separate, usa tutte le righe come argomenti per pyais (percorso
       simulatore).
    3. **Buffer multipart** — se il messaggio è un singolo frammento con
       ``total > 1``, lo accumula nel buffer e attende i frammenti mancanti.
    4. **Decodifica pyais** — eseguita in un thread executor (CPU-bound)
       per non bloccare l'event loop asyncio.
    5. **Pubblicazione** — costruisce l'evento JSON e lo pubblica su Kafka
       con chiave MMSI. Timeout: 5 secondi. Publish > 0.5 s → warning.

    Parameters
    ----------
    raw_value : bytes | dict | str
        Messaggio AIS grezzo in qualsiasi formato (normalizzato internamente).
    source_topic : str
        Topic Kafka sorgente (usato per il campo ``source`` dell'evento e
        come dimensione del buffer multipart).
    output_topic : str
        Topic Kafka di destinazione per l'evento decodificato.
    """
    global _publish_slow_count, _publish_total_time, _publish_count

    nmea = normalize_nmea(raw_value)
    if not nmea or not nmea.startswith("!"):
        return

    lines = [l.strip() for l in nmea.split("\n") if l.strip().startswith("!AIVDM")]
    if len(lines) > 1:
        decode_args = tuple(lines)
    else:
        final = None
        parts = nmea.split(",")
        try:
            if len(parts) > 5 and int(parts[1]) > 1:
                async with state_lock:
                    final = handle_multipart(source_topic, parts)
            else:
                final = nmea
        except Exception:
            pass

        if not final:
            return
        decode_args = (final,)

    try:
        loop = asyncio.get_running_loop()
        decoded = await loop.run_in_executor(None, lambda: ais_decode(*decode_args))
        data = decoded.asdict() if hasattr(decoded, "asdict") else dict(decoded)

        mmsi = data.get("mmsi")
        event = {
            "type": "ais_decoded",
            "msg_type": data.get("msg_type"),
            "mmsi": mmsi,
            "payload": data,
            "timestamp": time.time(),
            "source": source_topic,
        }

        partition_key = str(mmsi).encode("utf-8") if mmsi else None

        t0 = time.time()
        await asyncio.wait_for(
            broker.publish(event, topic=output_topic, key=partition_key),
            timeout=5.0,
        )
        pub_elapsed = time.time() - t0
        _publish_count += 1
        _publish_total_time += pub_elapsed
        if pub_elapsed > 0.5:
            _publish_slow_count += 1
            log(f"[PUBLISH SLOW] {pub_elapsed:.2f}s → {output_topic} mmsi={mmsi}")

        global _decode_count_main, _decode_count_sim
        if output_topic == SIM_OUTPUT_TOPIC:
            _decode_count_sim += 1
        else:
            _decode_count_main += 1

    except asyncio.TimeoutError:
        global _decode_errors
        _decode_errors += 1
        log(f"[PUBLISH TIMEOUT] 5s timeout → {output_topic}")
    except Exception as e:
        _decode_errors += 1
        log(f"[DECODE ERROR] {e}")


# =============================================================================
# SUBSCRIBER KAFKA
# =============================================================================
# Registrati condizionalmente in base a DECODER_MODE.
# DECODER_MODE=main → solo handle_main_ais (container produzione AIS reale)
# DECODER_MODE=sim  → solo handle_sim_ais  (container dedicato simulazione)
# DECODER_MODE=all  → entrambi            (sviluppo / test locale)

if DECODER_MODE in ("all", "main"):
    @broker.subscriber(
        MAIN_INPUT_TOPIC,
        description=(
            "Consuma messaggi NMEA/AIVDM grezzi dal topic **`ais.raw`** "
            "prodotti dal ricevitore AIS hardware (via Kafka Connector o feeder diretto).\n\n"
            "**Altri consumer dello stesso topic:**\n"
            "- `bridge-banchina` — calcola ETA per banchina\n"
            "- `bridge-components` — traccia utilizzo componenti\n"
            "- `bridge-delta-eta` — calcola scostamento ETA schedulata\n"
            "- `bridge-arrivo` — rileva arrivo a destinazione (geofencing)\n\n"
            "**Formato input:** bytes UTF-8 contenente stringa NMEA/AIVDM, "
            "oppure JSON wrapper da Kafka Connect "
            "(`{\"fields\": {\"value\": \"!AIVDM,...\"}}`). "
            "Gestisce anche messaggi bundled multipart separati da `\\n`.\n\n"
            "**Attivo quando:** `DECODER_MODE = all | main`."
        ),
    )
    async def handle_main_ais(msg: KafkaMessage):
        """
        Subscriber AIS reale: normalizza, decodifica e pubblica su ais_decoded.raw.
        """
        try:
            await decode_and_publish(msg.body, MAIN_INPUT_TOPIC, MAIN_OUTPUT_TOPIC)
        except Exception as e:
            log(f"[SUBSCRIBER ERROR] MAIN: {e}")
        await msg.ack()


if DECODER_MODE in ("all", "sim"):
    @broker.subscriber(
        SIM_INPUT_TOPIC,
        description=(
            "Consuma messaggi NMEA/AIVDM grezzi dal topic **`ais_simulation.raw`** "
            "prodotti dal servizio di simulazione AIS.\n\n"
            "**Altri consumer dello stesso topic:**\n"
            "- `bridge-banchina` — ETA per banchina (simulazione)\n"
            "- `bridge-components` — utilizzo componenti (simulazione)\n"
            "- `bridge-delta-eta` — scostamento ETA (simulazione)\n"
            "- `bridge-arrivo` — geofencing arrivo (simulazione)\n\n"
            "**Formato input:** identico a `ais.raw`. Il simulatore invia spesso "
            "messaggi Tipo 5 (multipart) bundled come singolo messaggio Kafka "
            "con i due frammenti separati da `\\n`.\n\n"
            "**Frequenza:** configurabile nel simulatore — tipicamente più alta "
            "del reale per accelerare i test (es. 10× velocità reale).\n\n"
            "**Attivo quando:** `DECODER_MODE = all | sim`."
        ),
    )
    async def handle_sim_ais(msg: KafkaMessage):
        """
        Subscriber AIS simulazione: normalizza, decodifica e pubblica su ais_decoded_simulation.raw.
        """
        try:
            await decode_and_publish(msg.body, SIM_INPUT_TOPIC, SIM_OUTPUT_TOPIC)
        except Exception as e:
            log(f"[SUBSCRIBER ERROR] SIM: {e}")
        await msg.ack()


# =============================================================================
# LIFECYCLE HOOKS
# =============================================================================

@app.on_startup
async def on_startup():
    """
    Hook eseguito all'avvio: loga la configurazione e avvia il cleanup periodico.
    """
    log("=" * 60)
    log("AIS DECODER FASTSTREAM v2.1.0 — Avvio")
    log("=" * 60)
    log(f"Kafka Bootstrap:  {BOOTSTRAP_SERVERS}")
    log(f"Decoder Mode:     {DECODER_MODE}")
    log(f"Input Topics:     {MAIN_INPUT_TOPIC}, {SIM_INPUT_TOPIC}")
    log(f"Output Topics:    {MAIN_OUTPUT_TOPIC}, {SIM_OUTPUT_TOPIC}")
    log(f"Multipart TTL:    {MULTIPART_TTL_SEC}s")
    asyncio.create_task(_periodic_cleanup_loop())
    log("Task cleanup/diagnostica avviato (ogni 10s cleanup, ogni 30s log)")


@app.after_startup
async def after_startup():
    """Hook eseguito dopo che tutti i subscriber sono attivi."""
    log("Worker pronto — in ascolto sui topic configurati.")
    log("=" * 60)


@app.on_shutdown
async def on_shutdown():
    """Hook eseguito allo shutdown: logga lo stato del buffer multipart."""
    log("Shutdown in corso...")
    log(f"Buffer multipart residuo: {len(multipart_buffer)} chiavi, "
        f"{sum(len(q) for q in multipart_buffer.values())} entry")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    asyncio.run(app.run())
