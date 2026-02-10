# Bridge Delta ETA

## Descrizione

Il **Bridge Delta ETA** è un worker FastStream che calcola la differenza (delta) tra l'ETA osservata nei messaggi AIS e l'ETA attesa/schedulata recuperata dal sistema di backend.

## Funzionalità Principale

Confronta l'ETA dichiarata dalla nave (campo AIS) con l'ETA attesa basata sugli orari schedulati e il tempo di percorrenza previsto. Pubblica eventi `delta_eta` che indicano se la nave è in anticipo, puntuale o in ritardo.

## Architettura del Flusso Dati

```
┌─────────────────────┐     ┌──────────────────┐
│     ais.raw         │ ──► │                  │
│  (NMEA grezzo)      │     │  Bridge Delta    │     ┌─────────────────────────┐
└─────────────────────┘     │  ETA             │ ──► │   analytics_ais.raw     │
                            │                  │     │                         │
┌─────────────────────┐     │  - Parsing ETA   │     │  Eventi:                │
│  ais_simulation.raw │ ──► │  - Query API     │     │  - delta_eta            │
│  (NMEA grezzo)      │     │  - Calcolo delta │     │                         │
└─────────────────────┘     └──────────────────┘     └─────────────────────────┘
                                    │
                                    ▼
                            ┌──────────────────┐
                            │  Backend API     │
                            │  /vascello/{mmsi}│
                            │  /percorso_attivo│
                            └──────────────────┘
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
| `analytics_ais.raw` | Eventi analytics (tipo `delta_eta`) |

## Calcolo Delta ETA

Il delta è calcolato come:

```
delta_min = (ETA_AIS - ETA_attesa) / 60
```

Dove:
- **ETA_AIS**: ETA dichiarata dalla nave nel messaggio AIS (tipo 5)
- **ETA_attesa**: ETA calcolata come `orario_partenza_schedulato + tempo_percorrenza`

### Interpretazione del Delta

| Delta | Significato |
|-------|-------------|
| `delta_min < 0` | Nave in anticipo (arriva prima del previsto) |
| `delta_min = 0` | Nave puntuale |
| `delta_min > 0` | Nave in ritardo (arriva dopo il previsto) |

## Gestione Real vs Simulation

### Navi Reali (source="real")
- L'ETA attesa viene recuperata dal percorso con `virtuale=false`
- Basata su `orario_partenza_schedulato + tempo_percorrenza`

### Navi Simulate (source="simulation")
- L'ETA attesa viene calcolata al primo messaggio ricevuto
- Formula: `timestamp_primo_messaggio + tempo_percorrenza`
- Il percorso deve avere `virtuale=true`

## Modelli Pydantic

### DeltaEtaEvent

Evento pubblicato per ogni nave:

```json
{
  "type": "delta_eta",
  "mmsi": "123456789",
  "delta_min": -12.5,
  "destination": "PORTO X",
  "eta": 1670000000.0,
  "eta_expected": 1670000720.0,
  "source": "real",
  "timestamp": 1670000100.0
}
```

## Cleanup Automatico

Le navi vengono rimosse dalla memoria quando non ricevono dati per un tempo pari a **1/5 del tempo di percorrenza** del loro percorso attivo. Questo evita accumulo di memoria per navi che hanno terminato la navigazione.

## API Backend

Il worker interroga il backend per recuperare informazioni aggiuntive:

| Endpoint | Descrizione |
|----------|-------------|
| `/vascello/{mmsi}` | Recupera dati della nave |
| `/percorso_attivo` | Recupera il percorso attivo della nave |

### URL Base API
```
http://87.26.178.190:15080
```

## Configurazione

### Variabili d'Ambiente

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `BOOTSTRAP_SERVERS` | `localhost:9092` | Indirizzo cluster Kafka |

### Costanti

| Costante | Valore | Descrizione |
|----------|--------|-------------|
| `API_BASE` | `http://87.26.178.190:15080` | URL base backend |

## Avvio

### Standalone
```bash
python bridge_delta_eta.py
```

### Docker
```bash
docker build -f Dockerfile.deltaeta -t bridge-deltaeta .
docker run -e BOOTSTRAP_SERVERS=kafka:9092 bridge-deltaeta
```

### Documentazione AsyncAPI
```bash
faststream docs serve bridge_delta_eta:app --host 0.0.0.0 --port 8000
```

Disponibile su porta **9000** quando avviato con Docker Compose.

## Dipendenze

- `faststream` - Framework per streaming Kafka
- `pyais` - Libreria per decodifica messaggi AIS
- `pydantic` - Validazione e serializzazione dati
- `requests` - Client HTTP per query API backend

## Versione

Versione: 2.0.0
