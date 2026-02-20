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
Questo worker implementa un buffer per ricomporre automaticamente
questi messaggi prima della decodifica.

Dipendenze
----------
- ``faststream``: Framework per streaming Kafka
- ``pyais``: Libreria per decodifica messaggi AIS
- ``pydantic``: Validazione e serializzazione dati

Esempio di Utilizzo
-------------------
>>> # Avvio standalone
>>> python decoder_ais_faststream.py
>>>
>>> # Oppure con uvicorn
>>> uvicorn decoder_ais_faststream:app --host 0.0.0.0 --port 8000

Note per Sviluppatori
---------------------
- I publisher stub decorati con ``@broker.publisher`` servono per generare
  la documentazione AsyncAPI automatica e non contengono logica runtime
- Il buffer multipart viene pulito automaticamente ogni 10 secondi
- I messaggi con errori di decodifica vengono loggati ma non bloccano il flusso

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
from functools import reduce
from typing import Optional, Dict, Any

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
# Le variabili di configurazione possono essere sovrascritte tramite
# variabili d'ambiente per facilitare il deployment Docker/Kubernetes.

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")
"""str: Indirizzo del cluster Kafka (formato: host:porta). Default: localhost:29092"""

# Topic di input (messaggi NMEA grezzi)
MAIN_INPUT_TOPIC = "ais.raw"
"""str: Topic per messaggi AIS reali grezzi"""

SIM_INPUT_TOPIC = "ais_simulation.raw"
"""str: Topic per messaggi AIS simulati grezzi"""

# Topic di output (messaggi JSON decodificati)
MAIN_OUTPUT_TOPIC = "ais_decoded.raw"
"""str: Topic per messaggi AIS reali decodificati"""

SIM_OUTPUT_TOPIC = "ais_decoded_simulation.raw"
"""str: Topic per messaggi AIS simulati decodificati"""


# =============================================================================
# INIZIALIZZAZIONE FASTSTREAM
# =============================================================================
# Il broker Kafka e l'applicazione FastStream vengono inizializzati a livello
# di modulo per essere condivisi tra tutti i subscriber e publisher.

broker = KafkaBroker(BOOTSTRAP_SERVERS)
"""KafkaBroker: Istanza del broker Kafka per comunicazione pub/sub"""

app = FastStream(broker)
"""FastStream: Applicazione principale FastStream"""

# =============================================================================
# MODELLI PYDANTIC (Schema AsyncAPI)
# =============================================================================
# Questi modelli definiscono la struttura dei messaggi per:
# 1. Validazione automatica dei dati
# 2. Generazione della documentazione AsyncAPI
# 3. Serializzazione/deserializzazione JSON

class AisDecodedPayload(BaseModel):
    """
    Payload AIS decodificato contenente tutti i campi disponibili.
    
    Il contenuto varia in base al tipo di messaggio AIS (msg_type).
    I campi più comuni includono posizione, velocità, rotta e dati nave.

    Attributes
    ----------
    * `msg_type` : int, optional - Tipo di messaggio AIS (1-27)
    * `mmsi` : str, optional - Maritime Mobile Service Identity (9 cifre)
    
    Note
    ----
    I campi aggiuntivi dipendono dal msg_type e vengono inclusi dinamicamente
    nel payload. Consultare la specifica ITU-R M.1371 per dettagli completi.
    """
    msg_type: Optional[int] = Field(None, description="Tipo messaggio AIS (1-27)")
    mmsi: Optional[str] = Field(None, description="MMSI nave (9 cifre)")


class AisDecodedEvent(BaseModel):
    """
    Evento AIS decodificato pubblicato sui topic di output.
    
    Questo è il formato standard per tutti i messaggi decodificati
    pubblicati su ``ais_decoded.raw`` e ``ais_decoded_simulation.raw``.

    Attributes
    ----------
    * `type` : str - Tipo evento, sempre "ais_decoded"
    * `msg_type` : int, optional - Tipo messaggio AIS originale
    * `mmsi` : str, optional - MMSI della nave
    * `payload` : dict - Payload completo con tutti i campi AIS decodificati
    * `timestamp` : float - Timestamp Unix (secondi) della decodifica
    * `source` : str - Topic sorgente del messaggio originale
    
    Examples
    --------
    Esempio di evento per messaggio di posizione (tipo 1)::
    
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
    
    Esempio di evento per dati statici nave (tipo 5)::
    
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
                "eta_minute": 30
            },
            "timestamp": 1670000000.0,
            "source": "ais.raw"
        }
    """
    type: str = Field("ais_decoded", description="Tipo evento (fisso: 'ais_decoded')")
    msg_type: Optional[int] = Field(None, description="Tipo messaggio AIS (1-27)")
    mmsi: Optional[str] = Field(None, description="MMSI nave (9 cifre)")
    payload: Dict[str, Any] = Field(..., description="Payload AIS decodificato completo")
    timestamp: float = Field(..., description="Timestamp Unix della decodifica (secondi)")
    source: str = Field(..., description="Topic sorgente: 'ais.raw' | 'ais_simulation.raw'")


# -----------------------------------------------------------------------------
# Publisher Stubs per documentazione AsyncAPI
# -----------------------------------------------------------------------------
# Queste funzioni sono stub vuoti che servono esclusivamente per la
# generazione automatica della documentazione AsyncAPI. Non contengono
# logica di runtime e NON devono essere rimosse.

@broker.publisher(MAIN_OUTPUT_TOPIC)
async def _doc_ais_decoded_main() -> AisDecodedEvent:
    """Publisher stub per documentazione AsyncAPI - Topic principale."""
    ...

@broker.publisher(SIM_OUTPUT_TOPIC)
async def _doc_ais_decoded_sim() -> AisDecodedEvent:
    """Publisher stub per documentazione AsyncAPI - Topic simulazione."""
    ...

# =============================================================================
# STATO GLOBALE
# =============================================================================
# Lo stato viene gestito con lock asincrono per garantire thread-safety
# durante l'accesso concorrente da più consumer.

state_lock = asyncio.Lock()
"""asyncio.Lock: Lock per accesso thread-safe allo stato condiviso"""

multipart_buffer: Dict[tuple, dict] = {}
"""
Dict[tuple, dict]: Buffer per ricomposizione messaggi NMEA multipart.

Struttura chiave: (topic, canale, sequenza)
Struttura valore: {
    "total": int,      # Numero totale frammenti attesi
    "parts": dict,     # Frammenti ricevuti {indice: payload}
    "ts": float        # Timestamp ultimo aggiornamento (per cleanup)
}
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
        # Converti bytes in stringa se necessario
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8", errors="ignore")

        raw_value = raw_value.strip()

        # Gestione wrapper JSON (es. da Kafka Connect)
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
        Corpo del messaggio NMEA (senza checksum)
    
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
    
    Parameters
    ----------
    topic : str
        Nome del topic Kafka sorgente (usato come chiave buffer)
    parts : list
        Lista dei campi del messaggio NMEA split per ","
        Formato: [tipo, totale, indice, seq, canale, payload, ...]
    
    Returns
    -------
    str | None
        Messaggio NMEA ricomposto se tutti i frammenti sono disponibili,
        altrimenti None (in attesa di altri frammenti)
    
    Notes
    -----
    Il buffer viene indicizzato per (topic, canale, sequenza) per gestire
    correttamente messaggi concorrenti da topic diversi o con sequenze diverse.
    
    Examples
    --------
    Primo frammento (ritorna None, in attesa del secondo)::
    
        >>> handle_multipart("ais.raw", ["AIVDM", "2", "1", "3", "B", "55?MbV02...", "0"])
        None
    
    Secondo frammento (ritorna messaggio completo)::
    
        >>> handle_multipart("ais.raw", ["AIVDM", "2", "2", "3", "B", "000000...", "2"])
        '!AIVDM,1,1,,B,55?MbV02...000000...,0*XX'
    """
    global last_cleanup
    
    try:
        total = int(parts[1])      # Numero totale frammenti
        index = int(parts[2])      # Indice frammento corrente (1-based)
        seq = parts[3] or "0"      # ID sequenza (per disambiguare)
        chan = parts[4]            # Canale radio (A o B)
        payload = parts[5]         # Payload codificato

        # Chiave univoca per questo messaggio multipart
        key = (topic, chan, seq)
        
        # Inizializza o recupera l'entry nel buffer
        entry = multipart_buffer.setdefault(
            key, {"total": total, "parts": {}, "ts": time.time()}
        )

        # Salva il frammento e aggiorna timestamp
        entry["parts"][index] = payload
        entry["ts"] = time.time()

        # Verifica se abbiamo tutti i frammenti
        if len(entry["parts"]) == total:
            # Ricomponi il payload in ordine
            full = "".join(entry["parts"][i] for i in range(1, total + 1))
            del multipart_buffer[key]

            # Costruisci il messaggio NMEA finale
            body = f"AIVDM,1,1,,{chan},{full},0"
            return f"!{body}*{compute_checksum(body)}"

    except Exception as e:
        log(f"[MULTIPART ERROR] Errore ricomposizione: {e}")

    return None


async def cleanup_multipart_buffer():
    """
    Esegue la pulizia periodica del buffer multipart.
    
    Rimuove i messaggi incompleti più vecchi di 5 secondi per prevenire
    memory leak in caso di frammenti persi.
    
    La pulizia viene eseguita al massimo ogni 10 secondi per efficienza.
    
    Notes
    -----
    Questa funzione è idempotente e può essere chiamata frequentemente
    senza impatto sulle performance.
    """
    global last_cleanup
    
    now = time.time()
    if now - last_cleanup > 10:
        async with state_lock:
            stale_keys = [k for k, v in multipart_buffer.items() if now - v["ts"] > 5]
            for k in stale_keys:
                del multipart_buffer[k]
        last_cleanup = now


# =============================================================================
# LOGICA DI DECODIFICA E PUBBLICAZIONE
# =============================================================================

async def decode_and_publish(raw_value, source_topic: str, output_topic: str) -> None:
    """
    Decodifica un messaggio AIS grezzo e lo pubblica sul topic di output.
    
    Questa funzione implementa il core della pipeline di elaborazione:
    1. Pulizia buffer multipart scaduti
    2. Normalizzazione del messaggio NMEA
    3. Gestione messaggi multipart (se necessario)
    4. Decodifica tramite libreria pyais
    5. Pubblicazione dell'evento JSON sul topic di output
    
    Parameters
    ----------
    raw_value : bytes | str
        Messaggio AIS grezzo in formato NMEA
    source_topic : str
        Topic Kafka sorgente (usato per tracking e buffer multipart)
    output_topic : str
        Topic Kafka di destinazione per il messaggio decodificato
    
    Notes
    -----
    Gli errori di decodifica vengono loggati ma non interrompono il flusso.
    I messaggi malformati vengono semplicemente ignorati.
    
    See Also
    --------
    normalize_nmea : Normalizzazione messaggi NMEA
    handle_multipart : Gestione messaggi multi-sentence
    AisDecodedEvent : Schema del messaggio pubblicato
    """
    # Step 1: Pulizia periodica buffer
    await cleanup_multipart_buffer()
    
    # Step 2: Normalizzazione NMEA
    nmea = normalize_nmea(raw_value)
    
    if not nmea or not nmea.startswith("!"):
        return

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

    try:
        # Step 4: Decodifica AIS con pyais
        decoded = ais_decode(final)
        data = decoded.asdict() if hasattr(decoded, "asdict") else dict(decoded)

        # Step 5: Costruzione e pubblicazione evento
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
        log(f"[DECODE ERROR] Errore decodifica AIS: {e}")


# =============================================================================
# SUBSCRIBER KAFKA
# =============================================================================
# I subscriber sono le funzioni che consumano messaggi dai topic Kafka.
# FastStream gestisce automaticamente la connessione, il commit degli offset
# e il retry in caso di errori.

@broker.subscriber(MAIN_INPUT_TOPIC)
async def handle_main_ais(msg: KafkaMessage):
    """
    Subscriber per il topic AIS principale (dati reali).
    
    Consuma messaggi dal topic ``ais.raw`` contenenti dati AIS reali
    provenienti da ricevitori fisici e li inoltra alla pipeline di decodifica.
    
    Parameters
    ----------
    msg : KafkaMessage
        Messaggio Kafka contenente payload AIS grezzo in formato NMEA
    
    Notes
    -----
    I messaggi decodificati vengono pubblicati su ``ais_decoded.raw``.
    """
    try:
        await decode_and_publish(msg.body, MAIN_INPUT_TOPIC, MAIN_OUTPUT_TOPIC)
        await msg.ack()
    except Exception as e:
        log(f"[SUBSCRIBER ERROR] MAIN topic: {e}")
        await msg.nack()


@broker.subscriber(SIM_INPUT_TOPIC)
async def handle_sim_ais(msg: KafkaMessage):
    """
    Subscriber per il topic AIS simulazione.
    
    Consuma messaggi dal topic ``ais_simulation.raw`` contenenti dati AIS
    generati da simulatori e li inoltra alla pipeline di decodifica.
    
    Parameters
    ----------
    msg : KafkaMessage
        Messaggio Kafka contenente payload AIS simulato in formato NMEA
    
    Notes
    -----
    I messaggi decodificati vengono pubblicati su ``ais_decoded_simulation.raw``.
    Questo permette di mantenere separati i flussi reali e simulati.
    """
    try:
        await decode_and_publish(msg.body, SIM_INPUT_TOPIC, SIM_OUTPUT_TOPIC)
        await msg.ack()
    except Exception as e:
        log(f"[SUBSCRIBER ERROR] SIM topic: {e}")
        await msg.nack()


# =============================================================================
# LIFECYCLE HOOKS
# =============================================================================
# FastStream fornisce hook per eseguire codice all'avvio e allo shutdown
# dell'applicazione. Utili per logging, inizializzazione risorse e cleanup.

@app.on_startup
async def on_startup():
    """
    Hook eseguito all'avvio dell'applicazione FastStream.
    
    Logga la configurazione corrente per facilitare il debugging.
    """
    log("=" * 60)
    log("AIS DECODER FASTSTREAM - Avvio")
    log("=" * 60)
    log(f"Kafka Bootstrap: {BOOTSTRAP_SERVERS}")
    log(f"Topic Input:     {MAIN_INPUT_TOPIC}, {SIM_INPUT_TOPIC}")
    log(f"Topic Output:    {MAIN_OUTPUT_TOPIC}, {SIM_OUTPUT_TOPIC}")


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
    
    Esegue cleanup delle risorse e logga lo stato finale.
    """
    log("AIS Decoder FastStream - Shutdown in corso...")
    log(f"Buffer multipart residuo: {len(multipart_buffer)} messaggi incompleti")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    """
    Avvia l'applicazione FastStream in modalità standalone.
    
    Per ambienti di produzione, si consiglia l'utilizzo di uvicorn:
        uvicorn decoder_ais_faststream:app --host 0.0.0.0 --port 8000
    """
    asyncio.run(app.run())
