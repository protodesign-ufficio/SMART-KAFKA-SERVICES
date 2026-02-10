# AisConsumer - Sistema di Analytics AIS

## Panoramica

AisConsumer è un sistema di microservizi basato su **FastStream** e **Apache Kafka** per l'elaborazione in tempo reale di messaggi AIS (Automatic Identification System) navali. Il sistema consuma messaggi AIS grezzi in formato NMEA, li decodifica e produce eventi analytics per il monitoraggio delle navi.

## Architettura Generale

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────────┐
│     ais.raw         │     │  Decoder AIS        │     │   ais_decoded.raw       │
│  (NMEA grezzo)      │────►│  FastStream         │────►│   (JSON decodificato)   │
└─────────────────────┘     └─────────────────────┘     └─────────────────────────┘
         │                                                        
         │                                                        
         ▼                                                        
┌─────────────────────┐     ┌─────────────────────────┐
│  Bridge Services    │────►│   analytics_ais.raw     │
│  - Banchina         │     │                         │
│  - Components       │     │  Eventi:                │
│  - Delta ETA        │     │  - berth_incoming       │
└─────────────────────┘     │  - component_usage      │
                            │  - delta_eta            │
                            └─────────────────────────┘
```

## Servizi Disponibili

| Servizio | File | Descrizione | Porta Docs |
|----------|------|-------------|------------|
| **Decoder AIS** | `decoder_ais_faststream.py` | Decodifica messaggi NMEA in JSON | - |
| **Bridge Banchina** | `bridge_banchina.py` | Analytics navi in arrivo per banchina | 9001 |
| **Bridge Components** | `bridge_components.py` | Monitoraggio utilizzo componenti nave | 9002 |
| **Bridge Delta ETA** | `bridge_delta_eta.py` | Calcolo scostamento ETA | 9000 |
| **Config Loader** | `config_loader.py` | Gestione configurazione dinamica | - |

## Topic Kafka

### Input
- `ais.raw` - Messaggi AIS reali in formato NMEA
- `ais_simulation.raw` - Messaggi AIS simulati in formato NMEA

### Output
- `ais_decoded.raw` - Messaggi AIS reali decodificati in JSON
- `ais_decoded_simulation.raw` - Messaggi AIS simulati decodificati in JSON
- `analytics_ais.raw` - Eventi analytics aggregati

## Tecnologie Utilizzate

- **FastStream** - Framework per streaming Kafka
- **PyAIS** - Libreria per decodifica messaggi AIS
- **Pydantic** - Validazione e serializzazione dati
- **Apache Kafka** - Message broker
- **Docker** - Containerizzazione

## Quick Start

### Requisiti
- Python 3.8+
- Docker e Docker Compose
- Apache Kafka cluster

### Installazione Dipendenze
```bash
pip install -r requirements.txt
```

### Avvio con Docker Compose
```bash
docker-compose up -d
```

### Avvio Standalone
```bash
# Decoder AIS
python decoder_ais_faststream.py

# Bridge Banchina
python bridge_banchina.py

# Bridge Components
python bridge_components.py

# Bridge Delta ETA
python bridge_delta_eta.py
```

## Variabili d'Ambiente

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `BOOTSTRAP_SERVERS` | `localhost:29092` | Indirizzo cluster Kafka |
| `BACKEND_URL` | `http://87.26.178.190:15080` | URL backend per configurazione |
| `WINDOW_FUTURE` | Da dashboard | Finestra temporale analytics |
| `PUBLISH_INTERVAL` | Da dashboard | Intervallo pubblicazione eventi |

## Documentazione AsyncAPI

Il sistema supporta la generazione automatica di documentazione AsyncAPI tramite FastStream:

```bash
# Genera documentazione per un servizio
faststream docs serve bridge_banchina:app --host 0.0.0.0 --port 8000
```

I servizi docs sono disponibili alle seguenti porte quando avviati con Docker Compose:
- **Bridge Delta ETA Docs**: http://localhost:9000
- **Bridge Banchina Docs**: http://localhost:9001
- **Bridge Components Docs**: http://localhost:9002

## Struttura del Progetto

```
AisConsumer/
├── bridge_banchina.py       # Analytics navi in arrivo
├── bridge_components.py     # Monitoraggio componenti
├── bridge_delta_eta.py      # Calcolo delta ETA
├── config_loader.py         # Gestione configurazione
├── decoder_ais_faststream.py # Decoder AIS
├── docker-compose.yml       # Orchestrazione container
├── Dockerfile.*             # Definizioni container
├── requirements.txt         # Dipendenze Python
├── README.md                # Documentazione principale
├── docs/                    # Documentazione dettagliata
│   ├── README.md
│   ├── decoder_ais_faststream.md
│   ├── bridge_banchina.md
│   ├── bridge_components.md
│   ├── bridge_delta_eta.md
│   └── config_loader.md
└── old-ignore/             # File deprecati
```

## Versione

Versione: 2.0.0

## Autore

Team AIS Analytics
