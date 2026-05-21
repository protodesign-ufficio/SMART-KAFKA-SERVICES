"""
AIS Decoder FastStream Worker
=============================

Descrizione
-----------
Questo modulo implementa un worker FastStream che consuma messaggi AIS grezzi
(formato NMEA/AIVDM) da topic Kafka e pubblica i messaggi decodificati in formato JSON
sui rispettivi topic di output.

Architettura del Flusso Dati
----------------------------
::

    ┌─────────────────────┐         ┌──────────────────────┐         ┌─────────────────────────┐
    │     ais.raw         │ ──────► │  AIS Decoder Worker  │ ──────► │   ais_decoded.raw       │
    │  (NMEA grezzo)      │         │                      │         │   (JSON decodificato)   │
    └─────────────────────┘         │  - Normalizzazione   │         └─────────────────────────┘
                                    │  - Gestione multipart│
    ┌─────────────────────┐         │  - Decodifica pyais  │         ┌─────────────────────────┐
    │  ais_simulation.raw │ ──────► │  - Pubblicazione     │ ──────► │ais_decoded_simulation.raw│
    │  (NMEA grezzo)      │         │                      │         │   (JSON decodificato)   │
    └─────────────────────┘         └──────────────────────┘         └─────────────────────────┘

Topic Kafka
-----------
**Input:**
    - ``ais.raw``: Messaggi AIS reali in formato NMEA
    - ``ais_simulation.raw``: Messaggi AIS simulati in formato NMEA

**Output:**
    - ``ais_decoded.raw``: Messaggi AIS reali decodificati in JSON
    - ``ais_decoded_simulation.raw``: Messaggi AIS simulati decodificati in JSON

Modalità Operative (DECODER_MODE)
----------------------------------
Il decoder può essere avviato in tre modalità tramite la variabile d'ambiente ``DECODER_MODE``:

- ``all``: Ascolta entrambi i topic (default)
- ``main``: Ascolta solo ``ais.raw`` (dati reali)
- ``sim``: Ascolta solo ``ais_simulation.raw`` (simulazione)

In produzione vengono eseguiti due container separati (main + sim) per isolare
i loop asincroni ed evitare contesa tra traffico reale e simulato.

Formato Messaggi NMEA/AIVDM
---------------------------
I messaggi AIVDM seguono il formato::

    !AIVDM,1,1,,B,15N4cJ`000rk3HH@1T7q@?v00000,0*37

    Dove:
    - Campo 1: Tipo messaggio (AIVDM)
    - Campo 2: Numero totale di frammenti (per messaggi multipart)
    - Campo 3: Numero del frammento corrente
    - Campo 4: ID sequenza (per messaggi multipart)
    - Campo 5: Canale radio (A o B)
    - Campo 6: Payload codificato (6-bit ASCII)
    - Campo 7: Bit di riempimento + checksum

Gestione Messaggi Multipart
---------------------------
Alcuni messaggi AIS (es. Tipo 5 - dati statici nave) sono troppo lunghi
per un singolo messaggio NMEA e vengono suddivisi in più frammenti.

Il buffer multipart usa una **coda per chiave** ``(topic, canale, num_frammenti)``
anziché il campo ``seq`` come discriminatore: il seq è un digit 0-9 riutilizzato
da tutti i vascelli sullo stesso canale e causa collisioni con più navi attive.

Due percorsi di ricomposizione:
1. **Bundled** (frammenti in un unico messaggio separati da ``\\n``): decodifica
   nativa pyais multipart senza buffer.
2. **Sequenziale** (frammenti in messaggi Kafka separati): buffer a coda con
   TTL di ``MULTIPART_TTL_SEC`` secondi.

Tipi Messaggi AIS
-----------------
I tipi più comuni gestiti:

+----------+------------------------------------------+
| Tipo     | Descrizione                              |
+==========+==========================================+
| 1, 2, 3  | Class A - Rapporto posizione             |
+----------+------------------------------------------+
| 4        | Base Station Report                      |
+----------+------------------------------------------+
| 5        | Class A - Dati statici e di viaggio      |
+----------+------------------------------------------+
| 14       | Safety-related broadcast                 |
+----------+------------------------------------------+
| 18       | Class B CS - Rapporto posizione          |
+----------+------------------------------------------+
| 21       | Aid-to-Navigation Report                 |
+----------+------------------------------------------+
| 24       | Class B CS - Dati statici                |
+----------+------------------------------------------+

Dipendenze
----------
- ``faststream``: Framework per streaming Kafka
- ``pyais``: Libreria per decodifica messaggi AIS
- ``pydantic``: Validazione e serializzazione dati

Note per Sviluppatori
---------------------
- I publisher stub decorati con ``@broker.publisher`` servono per generare
  la documentazione AsyncAPI automatica e non contengono logica runtime
- Il buffer multipart viene pulito automaticamente ogni 10 secondi (TTL 120s)
- I messaggi con errori di decodifica vengono loggati ma non bloccano il flusso
- La chiave di partizione Kafka è il MMSI: tutti i messaggi della stessa nave
  finiscono nella stessa partizione → ordine garantito per nave

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
from typing import Any, Dict, List, Optional

from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from pyais import decode as ais_decode
from pydantic import BaseModel, Field
import logging

# Riduce la verbosità dei log FastStream per evitare rumore nei log di produzione
logging.getLogger("faststream").setLevel(logging.WARNING)


# =============================================================================
# CONFIGURAZIONE
# =============================================================================

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")
"""str: Indirizzo del cluster Kafka (formato: host:porta). Default: localhost:29092"""

DECODER_MODE = os.getenv("DECODER_MODE", "all")
"""str: Modalità decoder: 'all' | 'main' | 'sim'"""

MAIN_INPUT_TOPIC = "ais.raw"
"""str: Topic per messaggi AIS reali grezzi"""

SIM_INPUT_TOPIC = "ais_simulation.raw"
"""str: Topic per messaggi AIS simulati grezzi"""

MAIN_OUTPUT_TOPIC = "ais_decoded.raw"
"""str: Topic per messaggi AIS reali decodificati"""

SIM_OUTPUT_TOPIC = "ais_decoded_simulation.raw"
"""str: Topic per messaggi AIS simulati decodificati"""

MULTIPART_TTL_SEC: float = 120.0
"""float: TTL dei frammenti multipart nel buffer (secondi)."""


# =============================================================================
# INIZIALIZZAZIONE FASTSTREAM
# =============================================================================

broker = KafkaBroker(BOOTSTRAP_SERVERS)
"""KafkaBroker: Istanza del broker Kafka per comunicazione pub/sub"""

app = FastStream(
    broker,
    title="AIS Decoder FastStream",
    version="2.1.0",
    description=(
        "Worker di decodifica messaggi AIS NMEA/AIVDM. "
        "Consuma messaggi grezzi da ais.raw e ais_simulation.raw, "
        "decodifica con pyais e pubblica JSON strutturati su ais_decoded.raw "
        "e ais_decoded_simulation.raw. Supporta modalità multi-istanza tramite "
        "DECODER_MODE (main | sim | all)."
    ),
)
"""FastStream: Applicazione principale FastStream"""


# =============================================================================
# MODELLI PYDANTIC (Schema AsyncAPI)
# =============================================================================

class AisPositionPayload(BaseModel):
    """
    Campi payload per messaggi di posizione AIS (Tipo 1, 2, 3, 18).

    Attributes
    ----------
    * `msg_type` : int - Tipo messaggio AIS
    * `mmsi` : str - MMSI nave (9 cifre)
    * `status` : int, optional - Stato navigazione (0=in rotta, 1=fermo in porto, 5=ormeggiato, etc.)
    * `turn` : float, optional - Velocità di virata ROT in gradi/min
    * `speed` : float, optional - Velocità SOG (Speed Over Ground) in nodi
    * `accuracy` : bool, optional - Accuratezza posizione GPS (true=<10m)
    * `lon` : float, optional - Longitudine in gradi decimali (negativo=Ovest)
    * `lat` : float, optional - Latitudine in gradi decimali (negativo=Sud)
    * `course` : float, optional - Rotta COG (Course Over Ground) in gradi (0-360)
    * `heading` : int, optional - Rotta prua HDG in gradi (0-359, 511=non disponibile)
    * `second` : int, optional - Secondi UTC del fix posizione
    * `maneuver` : int, optional - Indicatore manovra speciale (0=n/a, 1=manovra speciale, 2=non def.)
    * `raim` : bool, optional - Stato RAIM (Receiver Autonomous Integrity Monitoring)
    * `radio` : int, optional - Informazione radio/stato SOTDMA
    """
    msg_type: int = Field(..., description="Tipo messaggio AIS (1, 2, 3 o 18)")
    mmsi: str = Field(..., description="MMSI nave (9 cifre)")
    status: Optional[int] = Field(None, description="Stato navigazione AIS (0-15)")
    turn: Optional[float] = Field(None, description="Velocità di virata ROT (gradi/min)")
    speed: Optional[float] = Field(None, description="Velocità SOG in nodi")
    accuracy: Optional[bool] = Field(None, description="Accuratezza GPS (true=<10m, false=DGPS/>10m)")
    lon: Optional[float] = Field(None, description="Longitudine gradi decimali")
    lat: Optional[float] = Field(None, description="Latitudine gradi decimali")
    course: Optional[float] = Field(None, description="Rotta COG in gradi (0-360)")
    heading: Optional[int] = Field(None, description="Rotta prua HDG in gradi (0-359, 511=N/A)")
    second: Optional[int] = Field(None, description="Secondi UTC fix posizione")
    maneuver: Optional[int] = Field(None, description="Indicatore manovra speciale (0-2)")
    raim: Optional[bool] = Field(None, description="RAIM flag")
    radio: Optional[int] = Field(None, description="Info radio / stato SOTDMA")


class AisStaticPayload(BaseModel):
    """
    Campi payload per messaggi dati statici e di viaggio AIS (Tipo 5).

    Attributes
    ----------
    * `msg_type` : int - Tipo messaggio AIS (5)
    * `mmsi` : str - MMSI nave (9 cifre)
    * `imo` : str, optional - Numero IMO nave (7 cifre)
    * `callsign` : str, optional - Indicativo di chiamata radio (max 7 car.)
    * `shipname` : str, optional - Nome della nave (max 20 car.)
    * `shiptype` : int, optional - Tipo nave (codice ITU-R M.1371, 0-99)
    * `to_bow` : int, optional - Distanza antenna-prua in metri
    * `to_stern` : int, optional - Distanza antenna-poppa in metri
    * `to_port` : int, optional - Distanza antenna-sinistra in metri
    * `to_starboard` : int, optional - Distanza antenna-dritta in metri
    * `epfd` : int, optional - Tipo dispositivo EPFD (1=GPS, 2=GLONASS, etc.)
    * `eta_month` : int, optional - ETA mese (1-12, 0=non disponibile)
    * `eta_day` : int, optional - ETA giorno (1-31)
    * `eta_hour` : int, optional - ETA ora UTC (0-23)
    * `eta_minute` : int, optional - ETA minuti (0-59)
    * `draught` : float, optional - Pescaggio massimo in metri (0.1 risoluzione)
    * `destination` : str, optional - Destinazione dichiarata (max 20 car.)
    * `dte` : int, optional - DTE Data Terminal Equipment (0=disponibile)
    """
    msg_type: int = Field(..., description="Tipo messaggio AIS (5)")
    mmsi: str = Field(..., description="MMSI nave (9 cifre)")
    imo: Optional[str] = Field(None, description="Numero IMO nave")
    callsign: Optional[str] = Field(None, description="Indicativo radio (max 7 car.)")
    shipname: Optional[str] = Field(None, description="Nome nave (max 20 car.)")
    shiptype: Optional[int] = Field(None, description="Tipo nave ITU (0-99)")
    to_bow: Optional[int] = Field(None, description="Distanza antenna-prua (m)")
    to_stern: Optional[int] = Field(None, description="Distanza antenna-poppa (m)")
    to_port: Optional[int] = Field(None, description="Distanza antenna-sinistra (m)")
    to_starboard: Optional[int] = Field(None, description="Distanza antenna-dritta (m)")
    epfd: Optional[int] = Field(None, description="Tipo EPFD (1=GPS, 2=GLONASS, 3=GPS+GLONASS)")
    eta_month: Optional[int] = Field(None, description="ETA mese (1-12)")
    eta_day: Optional[int] = Field(None, description="ETA giorno (1-31)")
    eta_hour: Optional[int] = Field(None, description="ETA ora UTC (0-23)")
    eta_minute: Optional[int] = Field(None, description="ETA minuti (0-59)")
    draught: Optional[float] = Field(None, description="Pescaggio massimo (m, risoluzione 0.1m)")
    destination: Optional[str] = Field(None, description="Destinazione dichiarata (max 20 car.)")
    dte: Optional[int] = Field(None, description="DTE (0=disponibile, 1=non disponibile)")


class AisDecodedEvent(BaseModel):
    """
    Evento AIS decodificato pubblicato sui topic di output.

    Questo è il formato standard per tutti i messaggi decodificati
    pubblicati su ``ais_decoded.raw`` e ``ais_decoded_simulation.raw``.

    Il campo ``payload`` contiene tutti i campi AIS decodificati da pyais.
    Il contenuto varia in base al ``msg_type``:

    - **Tipo 1, 2, 3, 18**: Campi posizione (vedi ``AisPositionPayload``)
    - **Tipo 5**: Campi statici e di viaggio (vedi ``AisStaticPayload``)
    - **Altri tipi**: Campi specifici per tipo

    Attributes
    ----------
    * `type` : str - Tipo evento, sempre "ais_decoded"
    * `msg_type` : int, optional - Tipo messaggio AIS originale (1-27)
    * `mmsi` : str, optional - MMSI della nave (9 cifre)
    * `payload` : dict - Payload completo con tutti i campi AIS decodificati
    * `timestamp` : float - Timestamp Unix (secondi) della decodifica
    * `source` : str - Topic sorgente del messaggio originale

    Examples
    --------
    Messaggio di posizione (Tipo 1 - Class A)::

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

    Dati statici nave (Tipo 5 - Class A Voyage)::

        {
            "type": "ais_decoded",
            "msg_type": 5,
            "mmsi": "123456789",
            "payload": {
                "msg_type": 5,
                "mmsi": "123456789",
                "imo": "1234567",
                "callsign": "ABCD123",
                "shipname": "NAVE ESEMPIO",
                "shiptype": 70,
                "destination": "PORTO X",
                "eta_month": 3,
                "eta_day": 15,
                "eta_hour": 14,
                "eta_minute": 30,
                "draught": 5.2
            },
            "timestamp": 1670000000.0,
            "source": "ais.raw"
        }

    Posizione Class B (Tipo 18)::

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
            "timestamp": 1670000000.0,
            "source": "ais_simulation.raw"
        }
    """
    type: str = Field("ais_decoded", description="Tipo evento (fisso: 'ais_decoded')")
    msg_type: Optional[int] = Field(None, description="Tipo messaggio AIS (1-27)")
    mmsi: Optional[str] = Field(None, description="MMSI nave (9 cifre)")
    payload: Dict[str, Any] = Field(
        ...,
        description=(
            "Payload AIS decodificato completo. "
            "Campi variano per tipo: Tipo 1-3/18 → posizione+velocità (vedi AisPositionPayload), "
            "Tipo 5 → dati statici+viaggio (vedi AisStaticPayload)."
        ),
    )
    timestamp: float = Field(..., description="Timestamp Unix della decodifica (secondi)")
    source: str = Field(
        ..., description="Topic sorgente: 'ais.raw' | 'ais_simulation.raw'"
    )


# -----------------------------------------------------------------------------
# Publisher Stubs per documentazione AsyncAPI
# -----------------------------------------------------------------------------
# Queste funzioni sono stub vuoti che servono esclusivamente per la
# generazione automatica della documentazione AsyncAPI. Non contengono
# logica di runtime e NON devono essere rimosse.

@broker.publisher(MAIN_OUTPUT_TOPIC)
async def _doc_ais_decoded_main() -> AisDecodedEvent:
    """
    Publisher output topic principale.

    Pubblica messaggi AIS reali decodificati da ``ais.raw``.
    Attivo quando ``DECODER_MODE`` è ``all`` o ``main``.
    """
    ...

@broker.publisher(SIM_OUTPUT_TOPIC)
async def _doc_ais_decoded_sim() -> AisDecodedEvent:
    """
    Publisher output topic simulazione.

    Pubblica messaggi AIS simulati decodificati da ``ais_simulation.raw``.
    Attivo quando ``DECODER_MODE`` è ``all`` o ``sim``.
    """
    ...


# =============================================================================
# STATO GLOBALE
# =============================================================================

state_lock = asyncio.Lock()
"""asyncio.Lock: Lock per accesso thread-safe allo stato condiviso"""

multipart_buffer: Dict[tuple, List] = {}
"""
Dict[tuple, List[dict]]: Buffer a coda per ricomposizione messaggi NMEA multipart.

Chiave: (topic, canale, num_frammenti_totali)
Valore: lista di entry ``{"total": int, "parts": dict, "ts": float}``

La coda per chiave permette di gestire correttamente più vascelli
che usano lo stesso canale radio senza collisioni di seq ID.
"""

last_cleanup = time.time()
"""float: Timestamp dell'ultima pulizia del buffer multipart"""


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

    Examples
    --------
    >>> log("Connessione Kafka stabilita")
    [14:30:45] Connessione Kafka stabilita
    """
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def normalize_nmea(raw_value) -> Optional[str]:
    """
    Normalizza un messaggio NMEA grezzo in formato standard AIVDM.

    Gestisce diversi formati di input:

    - **dict**: da FastStream auto-deserializzazione JSON
      (es. ``{"fields": {"value": "!AIVDM,..."}}`` da simulazione)
    - **bytes UTF-8**: da topic Kafka con encoding binario
    - **str JSON wrapper**: da Kafka Connect (es. ``{"fields":{"value":"!AIVDM,..."}}`` )
    - **str AIVDM diretto**: messaggi già in formato ``!AIVDM,...``
    - **str con prefissi**: messaggi con caratteri extra prima del ``!``

    Parameters
    ----------
    raw_value : dict | bytes | str
        Messaggio grezzo in uno dei formati supportati

    Returns
    -------
    str | None
        Messaggio NMEA normalizzato (inizia con "!AIVDM")
        o None se il parsing fallisce

    Examples
    --------
    >>> normalize_nmea(b'!AIVDM,1,1,,B,15N4cJ`000rk3HH,0*37')
    '!AIVDM,1,1,,B,15N4cJ`000rk3HH,0*37'

    >>> normalize_nmea('{"fields":{"value":"!AIVDM,1,1,,B,15N4cJ`,0*37"}}')
    '!AIVDM,1,1,,B,15N4cJ`,0*37'
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

        # Gestione wrapper JSON (es. da Kafka Connect)
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

        # Cerca il marker "!" nel messaggio (per messaggi con prefissi)
        if "!" in raw_value:
            return raw_value[raw_value.find("!"):]

        return None
    except Exception:
        return None


def compute_checksum(body: str) -> str:
    """
    Calcola il checksum NMEA per un messaggio.

    Il checksum NMEA è calcolato come XOR di tutti i caratteri
    tra "!" e "*" (esclusi).

    Parameters
    ----------
    body : str
        Corpo del messaggio NMEA (con o senza il prefisso "!")

    Returns
    -------
    str
        Checksum esadecimale a 2 cifre (es. "3F")

    Examples
    --------
    >>> compute_checksum("AIVDM,1,1,,B,15N4cJ`000rk3HH,0")
    '37'
    """
    content = body[1:] if body.startswith("!") else body
    return f"{reduce(operator.xor, (ord(c) for c in content), 0):02X}"


def handle_multipart(topic: str, parts: list) -> Optional[str]:
    """
    Gestisce la ricomposizione di messaggi AIS multipart (multi-sentence).

    I messaggi AIS tipo 5 (dati statici nave) e altri tipi lunghi vengono
    suddivisi in più frammenti NMEA. Questa funzione raccoglie i frammenti
    e li ricompone quando sono tutti disponibili.

    Strategia buffer a coda per chiave ``(topic, canale, num_frammenti)``:
    evita collisioni causate dal campo ``seq`` (digit 0-9 condiviso tra
    tutti i vascelli sullo stesso canale). Con più navi attive, il seq
    viene riutilizzato e causa mescolamento dei frammenti se usato come chiave.

    Parameters
    ----------
    topic : str
        Nome del topic Kafka sorgente (usato come parte della chiave buffer)
    parts : list
        Lista dei campi del messaggio NMEA split per ","
        Formato: [tipo, totale, indice, seq, canale, payload, ...]

    Returns
    -------
    str | None
        Messaggio NMEA ricomposto se tutti i frammenti sono disponibili,
        altrimenti None (in attesa di altri frammenti)

    Examples
    --------
    Primo frammento (ritorna None, in attesa del secondo)::

        >>> handle_multipart("ais.raw", ["AIVDM", "2", "1", "3", "B", "55?MbV02...", "0"])
        None

    Secondo frammento (ritorna messaggio completo)::

        >>> handle_multipart("ais.raw", ["AIVDM", "2", "2", "3", "B", "000000...", "2"])
        '!AIVDM,1,1,,B,55?MbV02...000000...,0*XX'
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
            # Nuovo messaggio multipart: crea entry e accoda
            target_entry = {"total": total, "parts": {}, "ts": time.time()}
            queue.append(target_entry)
        else:
            # Frammento successivo: trova la entry più vecchia compatibile
            for entry in queue:
                if index not in entry["parts"] and len(entry["parts"]) < entry["total"]:
                    target_entry = entry
                    break
            if target_entry is None:
                # Nessuna entry compatibile (frammento orfano): crea nuova entry
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
        log(f"[MULTIPART ERROR] Errore ricomposizione: {e}")

    return None


# Contatori diagnostici per monitorare throughput decoder
_decode_count_main = 0
_decode_count_sim = 0
_decode_errors = 0
_last_diag_ts = time.time()


async def _periodic_cleanup_loop():
    """
    Task periodico che pulisce il buffer multipart ogni 10 secondi
    e logga statistiche diagnostiche ogni 30 secondi.

    Eseguito come task asincrono indipendente (lanciato in on_startup),
    NON chiamato per ogni messaggio. Questo elimina la contention
    sul lock che rallentava il processing dei messaggi.

    Cleanup multipart:
        Rimuove dai buffer le entry con timestamp più vecchio di
        ``MULTIPART_TTL_SEC`` secondi (default 120s).

    Diagnostica (ogni 30s):
        Logga messaggi decodificati per canale, errori, dimensione
        buffer multipart e statistiche latenza di publish.
    """
    global _last_diag_ts, _decode_count_main, _decode_count_sim, _decode_errors
    global _publish_slow_count, _publish_total_time, _publish_count
    while True:
        await asyncio.sleep(10)
        now = time.time()

        # Cleanup buffer multipart stale (queue-based)
        async with state_lock:
            for qk, queue in list(multipart_buffer.items()):
                queue[:] = [entry for entry in queue if now - entry["ts"] <= MULTIPART_TTL_SEC]
                if not queue:
                    del multipart_buffer[qk]

        # Diagnostica periodica ogni 30s
        if now - _last_diag_ts >= 30:
            avg_pub = (_publish_total_time / _publish_count * 1000) if _publish_count > 0 else 0
            log(f"[DIAG] decoded main={_decode_count_main} sim={_decode_count_sim} "
                f"errors={_decode_errors} multipart_buf={len(multipart_buffer)} "
                f"pub_slow={_publish_slow_count} pub_avg={avg_pub:.1f}ms")
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

# Contatori per diagnostica publish
_publish_slow_count = 0
_publish_total_time = 0.0
_publish_count = 0

async def decode_and_publish(raw_value, source_topic: str, output_topic: str) -> None:
    """
    Decodifica un messaggio AIS grezzo e lo pubblica sul topic di output.

    Pipeline di elaborazione:

    1. **Normalizzazione**: Converti il raw_value in stringa NMEA valida
    2. **Bundled multipart**: Se il messaggio contiene più righe (``\\n``-separated),
       decodifica direttamente con pyais multipart senza buffer
    3. **Buffer multipart**: Se frammento singolo con ``total > 1``,
       accumula nel buffer e attendi gli altri frammenti
    4. **Decodifica**: Chiama pyais in un thread executor (CPU-bound,
       non blocca l'event loop asyncio)
    5. **Pubblicazione**: Pubblica l'evento JSON su Kafka con chiave MMSI
       per garantire ordine per-nave

    Parameters
    ----------
    raw_value : bytes | dict | str
        Messaggio AIS grezzo (qualsiasi formato, normalizzato internamente)
    source_topic : str
        Topic Kafka sorgente (``ais.raw`` | ``ais_simulation.raw``)
    output_topic : str
        Topic Kafka destinazione (``ais_decoded.raw`` | ``ais_decoded_simulation.raw``)

    Notes
    -----
    - Timeout publish: 5 secondi. Superato il timeout, incrementa ``_decode_errors``
    - Publish lento (>0.5s): loggato come warning con latenza e MMSI
    - Chiave partizione Kafka: MMSI in bytes → ordine garantito per nave
    """
    global _publish_slow_count, _publish_total_time, _publish_count

    # Step 1: Normalizzazione NMEA
    nmea = normalize_nmea(raw_value)

    if not nmea or not nmea.startswith("!"):
        return

    # Step 2b: Gestione messaggi multi-riga (frammenti bundled in un unico messaggio Kafka)
    # Il simulatore invia tutti i frammenti Type 5 concatenati con \n in un singolo messaggio.
    # In questo caso decodifichiamo direttamente con pyais multipart senza buffer.
    lines = [l.strip() for l in nmea.split("\n") if l.strip().startswith("!AIVDM")]
    if len(lines) > 1:
        # Tutti i frammenti sono già presenti: decodifica nativa pyais multipart
        decode_args = tuple(lines)
    else:
        # Singola riga NMEA: gestione standard (single-part o buffer multipart)
        final = None
        parts = nmea.split(",")

        try:
            # Step 3: Gestione multipart (se totale frammenti > 1)
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
        # Step 4: Decodifica AIS con pyais in executor (CPU-bound, non blocca event loop)
        loop = asyncio.get_running_loop()
        decoded = await loop.run_in_executor(None, lambda: ais_decode(*decode_args))
        data = decoded.asdict() if hasattr(decoded, "asdict") else dict(decoded)

        # Step 5: Costruzione e pubblicazione evento
        mmsi = data.get("mmsi")
        event = {
            "type": "ais_decoded",
            "msg_type": data.get("msg_type"),
            "mmsi": mmsi,
            "payload": data,
            "timestamp": time.time(),
            "source": source_topic
        }

        # Usa MMSI come partition key: tutti i messaggi della stessa nave
        # finiscono nella stessa partizione Kafka → ordine garantito per nave
        partition_key = str(mmsi).encode("utf-8") if mmsi else None

        t0 = time.time()
        await asyncio.wait_for(broker.publish(event, topic=output_topic, key=partition_key), timeout=5.0)
        pub_elapsed = time.time() - t0
        _publish_count += 1
        _publish_total_time += pub_elapsed
        if pub_elapsed > 0.5:
            _publish_slow_count += 1
            log(f"[PUBLISH SLOW] {pub_elapsed:.2f}s to {output_topic} mmsi={data.get('mmsi')}")

        # Contatore diagnostico
        global _decode_count_main, _decode_count_sim
        if output_topic == SIM_OUTPUT_TOPIC:
            _decode_count_sim += 1
        else:
            _decode_count_main += 1

    except asyncio.TimeoutError:
        global _decode_errors
        _decode_errors += 1
        log(f"[PUBLISH TIMEOUT] 5s timeout publishing to {output_topic}")
    except Exception as e:
        _decode_errors += 1
        log(f"[DECODE ERROR] Errore decodifica AIS: {e}")


# =============================================================================
# SUBSCRIBER KAFKA
# =============================================================================
# I subscriber vengono registrati condizionalmente in base a DECODER_MODE.
# Con DECODER_MODE='sim' viene creato SOLO il subscriber per la simulazione,
# eliminando completamente la contesa con il traffico AIS reale.

if DECODER_MODE in ("all", "main"):
    @broker.subscriber(MAIN_INPUT_TOPIC)
    async def handle_main_ais(msg: KafkaMessage):
        """
        Subscriber per il topic AIS principale (dati reali).

        Consuma messaggi NMEA grezzi da ``ais.raw``, decodifica e pubblica
        su ``ais_decoded.raw``. Attivo quando ``DECODER_MODE`` è ``all`` o ``main``.

        Parameters
        ----------
        msg : KafkaMessage
            Messaggio Kafka contenente dati AIS reali in formato NMEA/AIVDM
        """
        try:
            await decode_and_publish(msg.body, MAIN_INPUT_TOPIC, MAIN_OUTPUT_TOPIC)
        except Exception as e:
            log(f"[SUBSCRIBER ERROR] MAIN topic: {e}")
        await msg.ack()

if DECODER_MODE in ("all", "sim"):
    @broker.subscriber(SIM_INPUT_TOPIC)
    async def handle_sim_ais(msg: KafkaMessage):
        """
        Subscriber per il topic AIS simulazione.

        Consuma messaggi NMEA grezzi da ``ais_simulation.raw``, decodifica e
        pubblica su ``ais_decoded_simulation.raw``. Attivo quando ``DECODER_MODE``
        è ``all`` o ``sim``.

        Parameters
        ----------
        msg : KafkaMessage
            Messaggio Kafka contenente dati AIS simulati in formato NMEA/AIVDM
        """
        try:
            await decode_and_publish(msg.body, SIM_INPUT_TOPIC, SIM_OUTPUT_TOPIC)
        except Exception as e:
            log(f"[SUBSCRIBER ERROR] SIM topic: {e}")
        await msg.ack()


# =============================================================================
# LIFECYCLE HOOKS
# =============================================================================

@app.on_startup
async def on_startup():
    """
    Hook eseguito all'avvio dell'applicazione FastStream.

    Logga la configurazione corrente e avvia il task periodico di cleanup
    del buffer multipart e di diagnostica.
    """
    log("=" * 60)
    log("AIS DECODER FASTSTREAM - Avvio")
    log("=" * 60)
    log(f"Kafka Bootstrap: {BOOTSTRAP_SERVERS}")
    log(f"Decoder Mode:    {DECODER_MODE}")
    log(f"Topic Input:     {MAIN_INPUT_TOPIC}, {SIM_INPUT_TOPIC}")
    log(f"Topic Output:    {MAIN_OUTPUT_TOPIC}, {SIM_OUTPUT_TOPIC}")
    log(f"Multipart TTL:   {MULTIPART_TTL_SEC}s")

    # Avvia cleanup periodico come task asincrono indipendente
    asyncio.create_task(_periodic_cleanup_loop())
    log("Cleanup periodico multipart buffer avviato (ogni 10s)")


@app.after_startup
async def after_startup():
    """
    Hook eseguito dopo che tutti i subscriber sono attivi.

    Conferma che il worker è pronto per elaborare messaggi.
    """
    log("AIS Decoder FastStream pronto e in ascolto!")
    log("=" * 60)


@app.on_shutdown
async def on_shutdown():
    """
    Hook eseguito allo shutdown dell'applicazione.

    Esegue cleanup delle risorse e logga lo stato finale del buffer multipart.
    """
    log("AIS Decoder FastStream - Shutdown in corso...")
    log(f"Buffer multipart residuo: {len(multipart_buffer)} chiavi")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    asyncio.run(app.run())
