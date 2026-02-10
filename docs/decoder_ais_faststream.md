# Decoder AIS FastStream

## Descrizione

Il **Decoder AIS FastStream** è un worker che consuma messaggi AIS grezzi in formato NMEA/AIVDM da topic Kafka e pubblica i messaggi decodificati in formato JSON sui rispettivi topic di output.

## Architettura del Flusso Dati

```
┌─────────────────────┐         ┌──────────────────────┐         ┌─────────────────────────┐
│     ais.raw         │ ──────► │  AIS Decoder Worker  │ ──────► │   ais_decoded.raw       │
│  (NMEA grezzo)      │         │                      │         │   (JSON decodificato)   │
└─────────────────────┘         │  - Normalizzazione   │         └─────────────────────────┘
                                │  - Gestione multipart│
┌─────────────────────┐         │  - Decodifica pyais  │         ┌─────────────────────────┐
│  ais_simulation.raw │ ──────► │  - Pubblicazione     │ ──────► │ais_decoded_simulation.raw│
│  (NMEA grezzo)      │         │                      │         │   (JSON decodificato)   │
└─────────────────────┘         └──────────────────────┘         └─────────────────────────┘
```

## Topic Kafka

### Input
| Topic | Descrizione |
|-------|-------------|
| `ais.raw` | Messaggi AIS reali in formato NMEA |
| `ais_simulation.raw` | Messaggi AIS simulati in formato NMEA |

### Output
| Topic | Descrizione |
|-------|-------------|
| `ais_decoded.raw` | Messaggi AIS reali decodificati in JSON |
| `ais_decoded_simulation.raw` | Messaggi AIS simulati decodificati in JSON |

## Formato Messaggi NMEA/AIVDM

I messaggi AIVDM seguono il formato:

```
!AIVDM,1,1,,B,15N4cJ`000rk3HH@1T7q@?v00000,0*37
```

| Campo | Descrizione |
|-------|-------------|
| Campo 1 | Tipo messaggio (AIVDM) |
| Campo 2 | Numero totale di frammenti (per messaggi multipart) |
| Campo 3 | Numero del frammento corrente |
| Campo 4 | ID sequenza (per messaggi multipart) |
| Campo 5 | Canale radio (A o B) |
| Campo 6 | Payload codificato (6-bit ASCII) |
| Campo 7 | Bit di riempimento + checksum |

## Gestione Messaggi Multipart

Alcuni messaggi AIS (es. Tipo 5 - dati statici nave) sono troppo lunghi per un singolo messaggio NMEA e vengono suddivisi in più frammenti. Questo worker implementa un buffer per ricomporre automaticamente questi messaggi prima della decodifica.

### Funzionamento Buffer

1. Il messaggio in arrivo viene analizzato per verificare se è multipart
2. Se è un frammento, viene memorizzato nel buffer
3. Quando tutti i frammenti sono ricevuti, vengono combinati
4. Il messaggio completo viene decodificato
5. Il buffer viene pulito automaticamente ogni 10 secondi

## Modelli Pydantic

### AisDecodedPayload

Schema del messaggio AIS decodificato in output:

```json
{
  "mmsi": "123456789",
  "msg_type": 1,
  "timestamp": 1670000100.0,
  "lat": 45.4234,
  "lon": 9.1234,
  "sog": 12.5,
  "cog": 180.0,
  "heading": 175,
  "status": "Under way using engine",
  "destination": "PORTO X",
  "eta": 1670100000.0,
  "raw_data": {...}
}
```

## Configurazione

### Variabili d'Ambiente

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `BOOTSTRAP_SERVERS` | `localhost:29092` | Indirizzo cluster Kafka |

## Avvio

### Standalone
```bash
python decoder_ais_faststream.py
```

### Con Uvicorn
```bash
uvicorn decoder_ais_faststream:app --host 0.0.0.0 --port 8000
```

### Docker
```bash
docker build -f Dockerfile.decoder_ais_faststream -t decoder-ais-faststream .
docker run -e BOOTSTRAP_SERVERS=kafka:9092 decoder-ais-faststream
```

## Note per Sviluppatori

- I publisher stub decorati con `@broker.publisher` servono per generare la documentazione AsyncAPI automatica e non contengono logica runtime
- Il buffer multipart viene pulito automaticamente ogni 10 secondi
- I messaggi con errori di decodifica vengono loggati ma non bloccano il flusso
- Il logging di FastStream è impostato a WARNING per ridurre il rumore nei log di produzione

## Dipendenze

- `faststream` - Framework per streaming Kafka
- `pyais` - Libreria per decodifica messaggi AIS
- `pydantic` - Validazione e serializzazione dati

## Versione

Versione: 2.0.0
