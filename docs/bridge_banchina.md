# Bridge Banchina

## Descrizione

Il **Bridge Banchina** è un worker FastStream che analizza i messaggi AIS per tracciare le navi in arrivo alle banchine e pubblicare eventi di analytics aggregati sul topic `analytics_ais.raw`.

## Funzionalità Principale

Monitora le navi che hanno impostato una destinazione (campo AIS "destination") e calcola quali sono in arrivo entro una finestra temporale configurabile. Pubblica periodicamente eventi `berth_incoming` con l'elenco delle navi attese per ogni banchina/destinazione.

## Architettura del Flusso Dati

```
┌─────────────────────┐                                    
│     ais.raw         │ ──┐                                
│  (NMEA grezzo)      │   │    ┌────────────────────────┐     ┌─────────────────────────┐
└─────────────────────┘   ├──► │  Bridge Banchina       │ ──► │   analytics_ais.raw     │
                          │    │                        │     │                         │
┌─────────────────────┐   │    │  - Parsing AIS         │     │  Eventi:                │
│  ais_simulation.raw │ ──┘    │  - Aggregazione ETA    │     │  - berth_incoming       │
│  (NMEA grezzo)      │        │  - Publishing ciclico  │     │                         │
└─────────────────────┘        └────────────────────────┘     └─────────────────────────┘
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
| `analytics_ais.raw` | Eventi analytics aggregati (tipo `berth_incoming`) |

## Logica di Business

1. **Ricezione**: Consuma messaggi AIS grezzi da entrambi i topic
2. **Parsing**: Estrae MMSI, destinazione ed ETA dal messaggio AIS tipo 5
3. **Aggregazione**: Raggruppa le navi per destinazione/banchina
4. **Filtraggio**: Seleziona solo le navi con ETA nella finestra temporale
5. **Pubblicazione**: Pubblica eventi `berth_incoming` periodicamente

## Configurazione Dinamica

Il worker supporta la configurazione dinamica tramite dashboard backend:

| Parametro | Descrizione |
|-----------|-------------|
| `WINDOW_FUTURE_MIN` | Finestra temporale in minuti per considerare una nave "in arrivo" |
| `PUBLISH_INTERVAL` | Intervallo in secondi tra le pubblicazioni degli eventi |

La configurazione viene ricaricata automaticamente ogni 2 minuti dal backend, permettendo modifiche senza riavvio del container Docker.

## Modelli Pydantic

### IncomingVessel

Rappresenta una nave in arrivo:

```json
{
  "mmsi": "123456789",
  "eta": 1670000000.0,
  "source": "ais.raw"
}
```

### BerthIncomingEvent

Evento pubblicato per ogni banchina:

```json
{
  "type": "berth_incoming",
  "destination": "PORTO X",
  "window_future_min": 180,
  "incoming_vessels": 2,
  "incoming": [
    {"mmsi": "123456789", "eta": 1670000000.0, "source": "ais.raw"},
    {"mmsi": "987654321", "eta": 1670000500.0, "source": "ais_simulation.raw"}
  ],
  "timestamp": 1670000100.0
}
```

## Gestione Stato

- Lo stato delle navi è condiviso e protetto da `asyncio.Lock`
- Il buffer multipart gestisce messaggi AIS multi-sentence
- La separazione real/simulation è mantenuta nel campo `source`

## Configurazione

### Variabili d'Ambiente

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `BOOTSTRAP_SERVERS` | `87.26.178.190:29092` | Indirizzo cluster Kafka |

## Avvio

### Standalone
```bash
python bridge_banchina.py
```

### Docker
```bash
docker build -f Dockerfile.banchina -t bridge-banchina .
docker run -e BOOTSTRAP_SERVERS=kafka:9092 bridge-banchina
```

### Documentazione AsyncAPI
```bash
faststream docs serve bridge_banchina:app --host 0.0.0.0 --port 8000
```

Disponibile su porta **9001** quando avviato con Docker Compose.

## Note per Sviluppatori

- I publisher stub decorati con `@broker.publisher` sono per AsyncAPI
- Lo stato delle navi è condiviso e protetto da asyncio.Lock
- Il buffer multipart gestisce messaggi AIS multi-sentence
- La separazione real/simulation è mantenuta nel campo `source`

## Dipendenze

- `faststream` - Framework per streaming Kafka
- `pyais` - Libreria per decodifica messaggi AIS
- `pydantic` - Validazione e serializzazione dati
- `config_loader` - Modulo per caricamento configurazione da dashboard

## Versione

Versione: 2.0.0
