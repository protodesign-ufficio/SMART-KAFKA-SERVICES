# Bridge Arrivo - Rilevamento Arrivo Navi (Geofencing)

## Panoramica

Il servizio **Bridge Arrivo** rileva automaticamente l'arrivo delle navi a destinazione
utilizzando il **geofencing** basato sulle coordinate dell'ultimo punto della rotta attiva
(`geom_rotta`) recuperata dal backend.

All'arrivo, aggiorna lo stato dell'assegnazione a **COMPLETATA** tramite l'API backend:
`PATCH /assegnazione/{assegnazione_id}/stato`

## Architettura

```
┌─────────────────────┐                                    
│     ais.raw         │ ──┐                                
│  (NMEA grezzo)      │   │    ┌────────────────────────┐
└─────────────────────┘   ├──► │  Bridge Arrivo         │
                          │    │                        │
┌─────────────────────┐   │    │  - Parsing posizione   │
│  ais_simulation.raw │ ──┘    │  - Geofencing          │
│  (NMEA grezzo)      │        │  - State tracking      │
└─────────────────────┘        └────────────────────────┘
                                    │
                                    ▼
                            ┌──────────────────────────────┐
                            │  Backend API                 │
                            │  GET  /vascello/{mmsi}/      │
                            │       percorso_attivo        │
                            │  GET  /percorso/{id}         │
                            │  PATCH /assegnazione/{id}/   │
                            │       stato                  │
                            └──────────────────────────────┘
```

## Logica di Rilevamento

### Condizioni di Arrivo

La nave è considerata **arrivata** quando **tutte** le condizioni sono verificate
per un numero configurabile di messaggi consecutivi:

1. **Geofence**: Distanza (haversine) dalla destinazione < `GEOFENCE_RADIUS_M` (default: 500m)
2. **Persistenza**: La condizione deve persistere per `ARRIVAL_CONFIRM_COUNT` messaggi
   consecutivi (default: 3) per evitare falsi positivi

### Macchina a Stati

```
                     in_geofence
    ┌──────────┐  ──────────────────►  ┌──────────┐
    │NAVIGATING│                       │ ARRIVING │
    └──────────┘  ◄──────────────────  └──────────┘
                   condizione persa          │
                                             │ confirm_count >= threshold
                                             ▼
                                       ┌──────────┐
                                       │ ARRIVED  │
                                       └──────────┘
                                       PATCH /assegnazione/{id}/stato
                                       {"stato_esecuzione": "COMPLETATA"}
```

### Pipeline API

1. `GET /vascello/{mmsi}/percorso_attivo` → ottiene `percorso_id` e `assegnazione_id`
2. `GET /percorso/{percorso_id}` → ottiene `geom_rotta`
3. Estrae l'**ultimo punto** della geometria come coordinate destinazione
4. All'arrivo: `PATCH /assegnazione/{assegnazione_id}/stato` con body:
   ```json
   {"stato_esecuzione": "COMPLETATA"}
   ```

### Esempio Response `percorso_attivo`

```json
{
  "vascello": {
    "id": "149f1532-4424-404f-b189-7ee8f7e39658",
    "mmsi": "247232500",
    "nome": "ACQUARIUS"
  },
  "percorsi": [
    {
      "assegnazione": {
        "id": "bc251e12-e9e4-4187-9662-35293b2a784c",
        "piano_id": "38a9af84-b075-47ba-b70b-604517a031d2",
        "virtuale": true
      },
      "percorso": {
        "id": "d77dfd8c-a9c4-4663-9e02-c7d3a7d87709",
        "corsa_id": "63b1b7f6-50b2-4f2d-905c-dd02f5d0ac89",
        "orario_partenza_schedulato": "2026-02-11T07:30:00",
        "tratta_id": "d6ad3938-2d30-4e5a-a475-d56f4c289246",
        "tratta_nome": "CET-MAI",
        "tempo_percorrenza": 27.55,
        "consumo": 4.13
      }
    }
  ]
}
```

## Configurazione

### Variabili d'Ambiente

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `BOOTSTRAP_SERVERS` | `87.26.178.190:29092` | Indirizzo cluster Kafka |
| `API_BASE` | `http://87.26.178.190:15080` | URL base API backend |
| `GEOFENCE_RADIUS_M` | `500` | Raggio geofence destinazione (metri) |
| `ARRIVAL_CONFIRM_COUNT` | `3` | Messaggi consecutivi per conferma arrivo |
| `ROUTE_CACHE_TTL_SEC` | `600` | Tempo cache coordinate rotta (secondi) |
| `SHIP_INACTIVE_TIMEOUT_SEC` | `3600` | Timeout rimozione navi inattive (secondi) |

### Configurazione Docker

```yaml
bridge-arrivo:
  image: bridge-arrivo
  build:
    context: .
    dockerfile: Dockerfile.arrivo
  environment:
    BOOTSTRAP_SERVERS: kafka:9092
    GEOFENCE_RADIUS_M: "500"
    ARRIVAL_CONFIRM_COUNT: "3"
  restart: unless-stopped
  networks:
    - kafka-net
```

## Topic Kafka

| Direzione | Topic | Formato |
|-----------|-------|---------|
| Input | `ais.raw` | NMEA/AIVDM grezzo |
| Input | `ais_simulation.raw` | NMEA/AIVDM grezzo |

Nessun topic di output: il servizio comunica con il backend via REST.

## Esecuzione

### Standalone

```bash
faststream run bridge_arrivo:app
```

### Docker

```bash
docker-compose up bridge-arrivo
```

### Documentazione AsyncAPI

Disponibile su porta `9004`:

```bash
docker-compose up bridge-arrivo-docs
```

## Formato `geom_rotta` Supportati

Il servizio supporta diversi formati per `geom_rotta`:

- **GeoJSON LineString**: `{"type": "LineString", "coordinates": [[lon, lat], ...]}`
- **GeoJSON MultiLineString**: `{"type": "MultiLineString", "coordinates": [[[lon, lat], ...], ...]}`
- **Lista coordinate**: `[[lon, lat], ...]`
- **Stringa JSON**: Viene parsata automaticamente

In tutti i casi, l'**ultimo punto** della geometria viene usato come destinazione.

## Note per Sviluppatori

- La formula di **Haversine** è usata per il calcolo della distanza (approssimazione sferica)
- Le coordinate GeoJSON usano ordine `[lon, lat]`, il servizio converte internamente in `(lat, lon)`
- Le coordinate `(0, 0)` vengono ignorate (posizione GPS invalida)
- I messaggi "ghost" (MMSI che inizia con "50") vengono ignorati
- Lo stato è indicizzato per `(topic, mmsi)` per separare navi reali e simulate
- La chiamata PATCH viene effettuata **una sola volta** per assegnazione (flag `arrival_completed`)
- Se la PATCH fallisce, verrà ritentata al prossimo messaggio in stato "arrived"
