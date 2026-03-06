# Bridge Components

## Descrizione

Il **Bridge Components** è un worker FastStream che monitora l'utilizzo dei componenti macchina delle navi basandosi sulla velocità rilevata dai messaggi AIS.

## Funzionalità Principale

Traccia il tempo di utilizzo dei componenti per ogni nave. Un componente è considerato "attivo" quando la nave ha velocità > 0.1 nodi (SOG - Speed Over Ground). Pubblica periodicamente eventi `component_usage` con il tempo totale di utilizzo.

## Architettura del Flusso Dati

```
┌─────────────────────┐                                    
│     ais.raw         │ ──┐                                
│  (NMEA grezzo)      │   │    ┌────────────────────────┐     ┌─────────────────────────┐
└─────────────────────┘   ├──► │  Bridge Components     │ ──► │   analytics_ais.raw     │
                          │    │                        │     │                         │
┌─────────────────────┐   │    │  - Tracking velocità   │     │  Eventi:                │
│  ais_simulation.raw │ ──┘    │  - Calcolo utilizzo    │     │  - component_usage      │
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
| `analytics_ais.raw` | Eventi analytics (tipo `component_usage`) |

## Componenti Monitorati

Il sistema traccia i componenti restituiti dinamicamente dal backend per ogni MMSI tramite:

`GET /componente/by_mmsi/{mmsi}`

Il nome componente usato negli eventi `component_usage` viene letto dal campo `nome_componente`.
Ogni componente ricevuto dall'API viene monitorato con la stessa logica di attivazione basata su SOG.

## Logica di Attivazione

Un componente è considerato attivo quando:

```
SOG (Speed Over Ground) > 0.1 nodi
```

Questo threshold basso permette di rilevare anche movimenti lenti (es. manovre in porto) escludendo solo la deriva GPS.

## Gestione Stato Real vs Simulation

Lo stato delle navi è indicizzato per chiave composta `(topic_sorgente, mmsi)`. Questo garantisce che una stessa nave simulata e reale mantengano contatori di utilizzo separati.

```python
ShipKey = Tuple[str, str]  # (topic, mmsi)
ships: Dict[ShipKey, dict] = {}
```

## Modelli Pydantic

### ComponentUsageEvent

Evento pubblicato per ogni componente:

```json
{
  "type": "component_usage",
  "mmsi": "123456789",
  "component": "engine_main",
  "usage_seconds_total": 3600,
  "active": true,
  "source": "ais.raw",
  "timestamp": 1670000100.0
}
```

## Calcolo Utilizzo

1. **Ricezione messaggio AIS**: Estrae MMSI e SOG
2. **Verifica velocità**: `SOG > 0.1` → componenti attivi
3. **Aggiornamento contatore**: Se attivo, incrementa `usage_seconds_total`
4. **Pubblicazione periodica**: Ogni `PUBLISH_INTERVAL_SEC` secondi

## Configurazione

### Variabili d'Ambiente

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `BOOTSTRAP_SERVERS` | `87.26.178.190:29092` | Indirizzo cluster Kafka |

### Parametri da Dashboard

| Parametro | Descrizione |
|-----------|-------------|
| `PUBLISH_INTERVAL_SEC` | Intervallo pubblicazione eventi in secondi |

## Avvio

### Standalone
```bash
python bridge_components.py
```

### Docker
```bash
docker build -f Dockerfile.components -t bridge-components .
docker run -e BOOTSTRAP_SERVERS=kafka:9092 bridge-components
```

### Documentazione AsyncAPI
```bash
faststream docs serve bridge_components:app --host 0.0.0.0 --port 8000
```

Disponibile su porta **9002** quando avviato con Docker Compose.

## Dipendenze

- `faststream` - Framework per streaming Kafka
- `pyais` - Libreria per decodifica messaggi AIS
- `pydantic` - Validazione e serializzazione dati
- `config_loader` - Modulo per caricamento configurazione

## Versione

Versione: 2.0.0
