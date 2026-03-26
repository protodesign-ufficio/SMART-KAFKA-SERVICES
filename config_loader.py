"""
Config Loader - Gestione Configurazione Dinamica
=================================================

Descrizione
-----------
Questo modulo fornisce funzionalità per il caricamento e l'aggiornamento
automatico della configurazione dei servizi Kafka dalla dashboard backend.

Caratteristiche Principali
--------------------------
1. **Caricamento Iniziale**: Recupera la configurazione dal backend all'avvio
2. **Fallback Robusto**: Usa valori default o variabili d'ambiente se il backend non è raggiungibile
3. **Auto-Update**: Thread opzionale per aggiornamento automatico ogni N secondi
4. **Zero Downtime**: Non richiede riavvio dei container Docker per applicare modifiche

Architettura
------------
::

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

Parametri Configurabili
-----------------------
- ``WINDOW_FUTURE``: Finestra temporale in secondi per analytics banchine
- ``PUBLISH_INTERVAL``: Intervallo pubblicazione eventi in secondi  
- ``PUBLISH_INTERVAL_SEC``: Alias di PUBLISH_INTERVAL per compatibilità
- ``last_update``: Timestamp ultimo aggiornamento (per change detection)

Priorità Configurazione
-----------------------
1. **Backend API** (priorità massima): Se raggiungibile, usa i valori dal backend
2. **Variabili d'ambiente**: Se backend non raggiungibile, usa ENV variables
3. **Valori default**: Se né backend né ENV disponibili, usa default hardcoded

Autore: Team AIS Analytics
Versione: 2.0.0
"""

# =============================================================================
# IMPORTS
# =============================================================================

import os
import requests
import time
import threading


# =============================================================================
# CONFIGURAZIONE MODULO
# =============================================================================

BACKEND_URL = os.getenv("BACKEND_URL", "http://87.26.178.190:25080")
"""str: URL base del backend per recupero configurazione"""

CONFIG_RETRY_INTERVAL = 5
"""int: Intervallo in secondi tra i retry in caso di errore"""


# =============================================================================
# FUNZIONI PRINCIPALI
# =============================================================================

def load_kafka_config_from_dashboard() -> dict:
    """
    Carica la configurazione Kafka dal backend (dashboard).
    
    Interroga l'endpoint ``/api/config/kafka-settings`` per recuperare
    i parametri di configurazione. In caso di fallimento, utilizza
    valori di default o variabili d'ambiente.
    
    Returns
    -------
    dict
        Dizionario con le chiavi:
        - ``window_future``: int - Finestra temporale in secondi
        - ``publish_interval``: int - Intervallo pubblicazione in secondi
        - ``publish_interval_sec``: int - Alias di publish_interval
           - ``sim_speed_factor``: float - Fattore velocità simulazione
           - ``last_update``: float - Timestamp ultimo aggiornamento (SEMPRE presente)
    
    Examples
    --------
    >>> config = load_kafka_config_from_dashboard()
    >>> print(config["window_future"])
    1800
    >>> print(config["publish_interval"])
    30
    
    Notes
    -----
    - Timeout HTTP: 5 secondi per evitare blocchi prolungati
    - I parametri vengono loggati su stdout per debugging
    - In caso di errore, il fallback è silenzioso (solo log)
       - Il campo ``last_update`` è SEMPRE presente anche se il backend non lo ritorna
    """
    try:
        print(f"[CONFIG] Caricamento configurazione da: {BACKEND_URL}/api/config/kafka-settings")
        
        response = requests.get(
            f"{BACKEND_URL}/api/config/kafka-settings",
            timeout=30
        )
        
        if response.status_code == 200:
            config = response.json()

            # Garantisce sempre la presenza del timestamp di update.
            if "last_update" not in config or config["last_update"] is None:
                config["last_update"] = time.time()
                print(
                    f"[CONFIG] Backend non ha ritornato 'last_update', impostato a: {config['last_update']}"
                )

            print(f"[CONFIG] ✓ Configurazione caricata dal backend:")
            print(f"         - WINDOW_FUTURE:       {config['window_future']} sec")
            print(f"         - PUBLISH_INTERVAL:    {config['publish_interval']} sec")
            print(f"         - PUBLISH_INTERVAL_SEC: {config['publish_interval_sec']} sec")
            print(f"         - SIM_SPEED_FACTOR:    {config.get('sim_speed_factor', 1.0)}")
            print(f"         - Last Update:         {config['last_update']}")
            return config
        else:
            raise Exception(f"HTTP {response.status_code}")
            
    except Exception as e:
        print(f"[CONFIG] ✗ Impossibile caricare dal backend: {e}")
        print(f"[CONFIG] Utilizzo fallback (ENV/default)")
        
        # Fallback: variabili d'ambiente o valori default
        return {
            "window_future": int(os.getenv("WINDOW_FUTURE", "1800")),
            "publish_interval": int(os.getenv("PUBLISH_INTERVAL", "30")),
            "publish_interval_sec": int(os.getenv("PUBLISH_INTERVAL_SEC", "30")),
            "sim_speed_factor": float(os.getenv("SIM_SPEED_FACTOR", "1.0")),
            "last_update": time.time()
        }


def load_config_with_retry(max_retries: int = 3) -> dict:
    """
    Carica la configurazione con retry automatico.
    
    Utile all'avvio dei servizi quando il backend potrebbe non essere
    ancora pronto (es. durante il boot di un cluster Docker).
    
    Parameters
    ----------
    max_retries : int, optional
        Numero massimo di tentativi (default: 3)
    
    Returns
    -------
    dict
        Configurazione caricata (vedi load_kafka_config_from_dashboard)
    
    Notes
    -----
    Tra un tentativo e l'altro attende CONFIG_RETRY_INTERVAL secondi.
    """
    for attempt in range(max_retries):
        config = load_kafka_config_from_dashboard()
        if config:
            return config
        
        if attempt < max_retries - 1:
            print(f"[CONFIG] Retry {attempt + 1}/{max_retries} tra {CONFIG_RETRY_INTERVAL} secondi...")
            time.sleep(CONFIG_RETRY_INTERVAL)
    
    print("[CONFIG] Fallback completo ai valori di default")
    return load_kafka_config_from_dashboard()


# =============================================================================
# MONITORAGGIO PERIODICO
# =============================================================================

def periodic_config_check() -> dict:
    """
    Controlla la configurazione e rileva cambiamenti.
    
    Questa funzione è progettata per essere chiamata periodicamente
    (es. ogni 60-120 secondi) per rilevare aggiornamenti alla
    configurazione senza richiedere il riavvio del servizio.
    
    Returns
    -------
    dict
        Configurazione aggiornata
    
    Notes
    -----
    I worker devono salvare il valore precedente di ``last_update``
    e confrontarlo con quello ritornato per decidere se applicare
    le modifiche.
    """
    return load_kafka_config_from_dashboard()


# =============================================================================
# THREAD DI AGGIORNAMENTO (Opzionale)
# =============================================================================

def _config_update_thread_target():
    """
    Target function per il thread di aggiornamento configurazione.
    
    Warning
    -------
    Non usare questo thread insieme ai watcher asyncio dei worker
    FastStream per evitare conflitti.
    """
    while True:
        time.sleep(30)  # Check ogni 30 secondi
        try:
            periodic_config_check()
        except Exception as e:
            print(f"[CONFIG] Errore nel check periodico: {e}")


def start_config_update_thread() -> threading.Thread:
    """
    Avvia il thread di aggiornamento configurazione.
    
    Returns
    -------
    threading.Thread
        Thread avviato (daemon)
    
    Warning
    -------
    Usare solo se NON si usa il watcher asyncio nei worker FastStream.
    """
    thread = threading.Thread(target=_config_update_thread_target, daemon=True)
    thread.start()
    print("[CONFIG] Thread di auto-update avviato (check ogni 2 minuti)")
    return thread


# =============================================================================
# GUIDA ALL'INTEGRAZIONE
# =============================================================================

"""
GUIDA ALL'INTEGRAZIONE NEI WORKER KAFKA
========================================

Per integrare questo modulo nei worker FastStream esistenti:

1. IMPORT E CARICAMENTO INIZIALE
   -----------------------------
   All'inizio del file, dopo gli import::
   
       from config_loader import load_kafka_config_from_dashboard
       
       config = load_kafka_config_from_dashboard()
       WINDOW_FUTURE_MIN = int(config["window_future"])
       PUBLISH_INTERVAL = int(config["publish_interval"])
       CONFIG_LAST_UPDATE = float(config.get("last_update", time.time()))

2. WATCHER ASINCRONO (Raccomandato per FastStream)
   ------------------------------------------------
   Creare un task asyncio che controlla periodicamente::
   
       async def config_watcher():
           global WINDOW_FUTURE_MIN, PUBLISH_INTERVAL, CONFIG_LAST_UPDATE
           
           while True:
               await asyncio.sleep(120)  # Check ogni 2 minuti
               
               new_config = load_kafka_config_from_dashboard()
               last = float(new_config.get("last_update", 0))
               
               if last > CONFIG_LAST_UPDATE:
                   print("[CONFIG] Configurazione aggiornata!")
                   WINDOW_FUTURE_MIN = int(new_config["window_future"])
                   PUBLISH_INTERVAL = int(new_config["publish_interval"])
                   CONFIG_LAST_UPDATE = last
       
       # Avviare in @app.on_startup
       asyncio.create_task(config_watcher())

3. CONFIGURAZIONE DOCKER (opzionale)
   ----------------------------------
   Se il backend non è raggiungibile, i fallback usano ENV::
   
       environment:
         - BACKEND_URL=http://backend:25080
         - WINDOW_FUTURE=1800
         - PUBLISH_INTERVAL=30
"""
