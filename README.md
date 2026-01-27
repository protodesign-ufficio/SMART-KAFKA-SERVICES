AisConsumer - Documentazione FastStream
=====================================

Questo repository contiene i worker FastStream che consumano messaggi AIS grezzi
(`ais.raw`, `ais_simulation.raw`) e pubblicano eventi di analytics su
`analytics_ais.raw`.

Scopo
-----
- Rendere la documentazione generata (AsyncAPI) più leggibile e presentabile.
- Fornire esempi di payload per i modelli Pydantic usati nella generazione AsyncAPI.

Topici Kafka principali
-----------------------
- `ais.raw` - flusso AIS reale
- `ais_simulation.raw` - flusso AIS simulato
- `analytics_ais.raw` - topic di output dove vengono pubblicati gli eventi tipizzati

Worker principali
-----------------
- `bridge_banchina.py` - pubblica eventi `berth_incoming` contenenti navi in arrivo per banchina
- `bridge_components.py` - pubblica eventi `component_usage` con l'utilizzo dei componenti nave
- `bridge_delta_eta.py` - pubblica eventi `delta_eta` che confrontano ETA AIS vs ETA attesa

Come leggere la documentazione AsyncAPI
--------------------------------------
I file `bridge_*.py` contengono Pydantic models (classi) usate da FastStream per
generare lo schema AsyncAPI. Le funzioni annotate con `@broker.publisher(topic)`
sono dei *stub* che permettono a FastStream di inferire i payload tipizzati.

Esempi di payload
------------------
`DeltaEtaEvent` (bridge_delta_eta.py):
```
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

`BerthIncomingEvent` (bridge_banchina.py):
```
{
  "type": "berth_incoming",
  "destination": "PORTO X",
  "window_future_min": 180,
  "incoming_vessels": 2,
  "incoming": [{"mmsi":"123","eta":1670000000.0,"source":"ais.raw"}],
  "timestamp": 1670000100.0
}
```

`ComponentUsageEvent` (bridge_components.py):
```
{
  "type": "component_usage",
  "mmsi": "123456789",
  "component": "engine_main",
  "usage_seconds_total": 120,
  "active": true,
  "source": "ais.raw",
  "timestamp": 1670000100.0
}
```

