# Config Loader

## Descrizione

Il **Config Loader** è un modulo di utilità che fornisce funzionalità per il caricamento e l'aggiornamento automatico della configurazione dei servizi Kafka dalla dashboard backend.

## Caratteristiche Principali

1. **Caricamento Iniziale**: Recupera la configurazione dal backend all'avvio
2. **Fallback Robusto**: Usa valori default o variabili d'ambiente se il backend non è raggiungibile
3. **Auto-Update**: Thread opzionale per aggiornamento automatico ogni N secondi
4. **Zero Downtime**: Non richiede riavvio dei container Docker per applicare modifiche

## Architettura

```
┌─────────────────┐        ┌──────────────────┐        ┌─────────────────┐
│  Worker Kafka   │ ─────► │  config_loader   │ ─────► │  Dashboard API  │
│  (bridge_*.py)  │        │                  │        │  /api/config/   │
└─────────────────┘        │  - Fetch config  │        │  kafka-settings │
                           │  - Fallback ENV  │        └─────────────────┘
┌─────────────────┐        │  - Auto-update   │
│  Docker ENV     │ ─────► │                  │
│  WINDOW_FUTURE  │        └──────────────────┘
│  PUBLISH_INTERVAL│
└─────────────────┘
```

## Parametri Configurabili

| Parametro | Tipo | Descrizione |
|-----------|------|-------------|
| `WINDOW_FUTURE` | int | Finestra temporale in secondi per analytics banchine |
| `PUBLISH_INTERVAL` | int | Intervallo pubblicazione eventi in secondi |
| `PUBLISH_INTERVAL_SEC` | int | Alias di PUBLISH_INTERVAL per compatibilità |
| `last_update` | float | Timestamp ultimo aggiornamento (per change detection) |

## Priorità Configurazione

La configurazione viene determinata con la seguente priorità (dalla più alta alla più bassa):

1. **Backend API** (priorità massima): Se raggiungibile, usa i valori dal backend
2. **Variabili d'ambiente**: Se backend non raggiungibile, usa ENV variables
3. **Valori default**: Se né backend né ENV disponibili, usa default hardcoded

## Utilizzo

### Caricamento Configurazione

```python
from config_loader import load_kafka_config_from_dashboard

# Carica la configurazione
config = load_kafka_config_from_dashboard()

# Accesso ai parametri
window_future = config["window_future"]
publish_interval = config["publish_interval"]
last_update = config["last_update"]
```

### Esempio di Configurazione Restituita

```python
{
    "window_future": 1800,        # 30 minuti in secondi
    "publish_interval": 30,       # ogni 30 secondi
    "publish_interval_sec": 30,   # alias
    "last_update": 1670000100.0   # timestamp Unix
}
```

## API Backend

### Endpoint

```
GET /api/config/kafka-settings
```

### URL Base

```
http://87.26.178.190:15080
```

### Risposta Attesa

```json
{
  "window_future": 1800,
  "publish_interval": 30
}
```

## Gestione Errori

In caso di errore nella comunicazione con il backend:

1. Viene loggato un warning
2. Si attende `CONFIG_RETRY_INTERVAL` secondi (default: 5)
3. Si utilizzano i valori di fallback (ENV o default)

## Configurazione Modulo

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `BACKEND_URL` | `http://87.26.178.190:15080` | URL base del backend |
| `CONFIG_RETRY_INTERVAL` | `5` | Intervallo retry in secondi |

## Auto-Update in Background

I worker possono avviare un thread di auto-update che ricarica la configurazione periodicamente:

```python
import threading
import time

def config_watcher():
    """Thread che ricarica la configurazione ogni 2 minuti."""
    global WINDOW_FUTURE_MIN, PUBLISH_INTERVAL
    while True:
        time.sleep(120)  # 2 minuti
        config = load_kafka_config_from_dashboard()
        WINDOW_FUTURE_MIN = int(config["window_future"])
        PUBLISH_INTERVAL = int(config["publish_interval"])

# Avvia il watcher
watcher_thread = threading.Thread(target=config_watcher, daemon=True)
watcher_thread.start()
```

## Change Detection

Il campo `last_update` può essere utilizzato per verificare se la configurazione è cambiata:

```python
current_update = config.get("last_update", 0)
if current_update > CONFIG_LAST_UPDATE:
    # La configurazione è cambiata, applica le modifiche
    apply_new_config(config)
    CONFIG_LAST_UPDATE = current_update
```

## Dipendenze

- `requests` - Client HTTP per query API backend
- `os` - Accesso alle variabili d'ambiente
- `time` - Gestione timestamp
- `threading` - Thread per auto-update

## Note per Sviluppatori

- Il modulo è thread-safe per le operazioni di lettura
- Le modifiche alla configurazione dovrebbero essere gestite atomicamente nei worker
- Il retry interval è configurabile ma non dovrebbe essere troppo basso per evitare sovraccarico del backend

## Versione

Versione: 2.0.0
