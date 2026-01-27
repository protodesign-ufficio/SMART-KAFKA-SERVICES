"""
ESEMPIO DI INTEGRAZIONE: Come modificare i servizi Kafka per leggere dalla dashboard

Questo file mostra come i servizi Kafka dovrebbero integrarsi con il backend
per caricare la configurazione dalla dashboard "Impostazioni Avanzate".

🚀 CARATTERISTICA PRINCIPALE: AUTO-UPDATE IN TEMPO REALE
I servizi hanno un thread che ogni 10 secondi controlla l'API del backend
e aggiorna automaticamente i parametri se sono stati modificati.

NON È PIÙ NECESSARIO RIAVVIARE I CONTAINER DOCKER! ✅

La procedura rispetta il deployment indipendente su Docker mantenendo la flessibilità
di fallback ai valori di default o variabili d'ambiente.
"""

import os
import requests
import time

# =====================================================
# CONFIGURAZIONE: LETTURA DA DASHBOARD + FALLBACK
# =====================================================

BACKEND_URL = os.getenv("BACKEND_URL", "http://87.26.178.190:15080")
CONFIG_RETRY_INTERVAL = 5  # secondi

def load_kafka_config_from_dashboard():
    """
    Legge la configurazione dal backend (dashboard).
    Se fallisce, usa i valori di default o ENV variables.
    
    Returns:
        dict: Configurazione con chiavi (window_future, publish_interval, etc.)
    """
    try:
        print(f"[CONFIG] Tentativo di caricamento da backend: {BACKEND_URL}/api/config/kafka-settings")
        
        response = requests.get(
            f"{BACKEND_URL}/api/config/kafka-settings",
            timeout=5
        )
        
        if response.status_code == 200:
            config = response.json()
            print(f"[CONFIG] ✓ Configurazione caricata dal backend:")
            print(f"         - WINDOW_FUTURE: {config['window_future']} sec")
            print(f"         - PUBLISH_INTERVAL: {config['publish_interval']} sec")
            print(f"         - PUBLISH_INTERVAL_SEC: {config['publish_interval_sec']} sec")
            #print(f"         - API_BASE: {config['api_base']}")
            print(f"         - KAFKA_TOPIC: {config['kafka_topic']}")
            print(f"         - Last Update: {config['last_update']}")
            return config
        else:
            raise Exception(f"HTTP {response.status_code}")
            
    except Exception as e:
        print(f"[CONFIG] ✗ Impossibile caricare dal backend: {e}")
        print(f"[CONFIG] Fallback ai valori di default/ENV")
        
        # Fallback ai valori di default o variabili d'ambiente
        return {
            "window_future": int(os.getenv("WINDOW_FUTURE", "1800")),
            "publish_interval": int(os.getenv("PUBLISH_INTERVAL", "30")),
            "publish_interval_sec": int(os.getenv("PUBLISH_INTERVAL_SEC", "30")),
            "kafka_topic": os.getenv("KAFKA_TOPIC", "ais.raw"),
            #"api_base": os.getenv("API_BASE", "http://87.26.178.190:15080"),
            "last_update": time.time()
        }


def load_config_with_retry():
    """
    Tenta di caricare la configurazione con retry.
    Utile se il backend non è ancora pronto al boot del servizio.
    """
    max_retries = 3
    for attempt in range(max_retries):
        config = load_kafka_config_from_dashboard()
        if config:
            return config
        
        if attempt < max_retries - 1:
            print(f"[CONFIG] Retry {attempt + 1}/{max_retries} in {CONFIG_RETRY_INTERVAL} secondi...")
            time.sleep(CONFIG_RETRY_INTERVAL)
    
    print("[CONFIG] Fallback completo ai valori di default")
    return load_kafka_config_from_dashboard()


# =====================================================
# ESEMPIO DI UTILIZZO IN bridge_banchina.py
# =====================================================

# Al boot del servizio
#print("=" * 60)
#print("BRIDGE BANCHINA - Startup")
#print("=" * 60)

# Carica la configurazione
#config = load_config_with_retry()

# Assegna i valori
#WINDOW_FUTURE = config["window_future"]
#PUBLISH_INTERVAL = config["publish_interval"]
#API_BASE = config["api_base"]
#CONFIG_LAST_UPDATE = config.get("last_update", time.time())

#BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
#MAIN_TOPIC = "ais.raw"
#ANALYTICS_TOPIC = "analytics_ais.raw"

#print(f"""
#[BANCHINA] Configurazione finale:
#  - BOOTSTRAP_SERVERS: {BOOTSTRAP_SERVERS}
#  - WINDOW_FUTURE: {WINDOW_FUTURE} sec ({WINDOW_FUTURE/60:.1f} min)
#  - PUBLISH_INTERVAL: {PUBLISH_INTERVAL} sec
#  - API_BASE: {API_BASE}
#  - MAIN_TOPIC: {MAIN_TOPIC}
#  - ANALYTICS_TOPIC: {ANALYTICS_TOPIC}
#""")

# =====================================================
# MONITORAGGIO PERIODICO (OPZIONALE)
# =====================================================

def periodic_config_check():
    """
    Controlla la configurazione dal backend e aggiorna le variabili globali
    se sono cambiate. Questo permette l'AUTO-UPDATE in tempo reale.
    """
    global WINDOW_FUTURE, PUBLISH_INTERVAL, API_BASE, CONFIG_LAST_UPDATE
    
    new_config = load_kafka_config_from_dashboard()
    
    # Controlla il timestamp per rilevare cambiamenti (change detection)
    if new_config.get("last_update", 0) > CONFIG_LAST_UPDATE:
        print(f"\n[CONFIG] 🔄 Configurazione aggiornata dal backend!")
        print(f"[CONFIG] Vecchia WINDOW_FUTURE={WINDOW_FUTURE}sec, PUBLISH_INTERVAL={PUBLISH_INTERVAL}sec")
        print(f"[CONFIG] Nuova  WINDOW_FUTURE={new_config['window_future']}sec, PUBLISH_INTERVAL={new_config['publish_interval']}sec")
        
        WINDOW_FUTURE = new_config["window_future"]
        PUBLISH_INTERVAL = new_config["publish_interval"]
        API_BASE = new_config["api_base"]
        CONFIG_LAST_UPDATE = new_config.get("last_update", time.time())
        
        print(f"[CONFIG] ✅ Configurazione ricaricata con successo!\n")
    
    return new_config


# =====================================================
# THREAD DI AGGIORNAMENTO ASINCRONO (OPZIONALE)
# =====================================================

import threading

def config_update_thread():
    """
    Thread che controlla la configurazione ogni x secondi e aggiorna automaticamente
    le variabili globali se sono cambiate.
    
    Questo è il CORE del sistema di AUTO-UPDATE!
    Non è necessario riavviare il servizio Docker quando modifichi i parametri.
    """
    while True:
        time.sleep(120)  # Controlla ogni x secondi -> 2 minuti per ora
        try:
            periodic_config_check()
        except Exception as e:
            print(f"[CONFIG] Errore nel check periodico: {e}")

# ✅ ATTIVATO DI DEFAULT - Il thread controlla e aggiorna automaticamente ogni x secondi
#config_thread = threading.Thread(target=config_update_thread, daemon=True)
#config_thread.start()



# =====================================================
# INTEGRAZIONE NEGLI SCRIPT KAFKA ESISTENTI
# =====================================================

"""
Per integrare questa logica negli script attuali (bridge_banchina.py, etc.):

1. Inserisci il blocco di caricamento config all'inizio del file:
   
   config = load_config_with_retry()
   WINDOW_FUTURE_MIN = config["window_future_min"]
   PUBLISH_INTERVAL = config["publish_interval"]
   API_BASE = config["api_base"]

2. Sostituisci le linee:
   
   # PRIMA:
   WINDOW_FUTURE_MIN = 30
   PUBLISH_INTERVAL = 30
   API_BASE = "http://87.26.178.190:15080"
   
   # DOPO:
   config = load_kafka_config_from_dashboard()
   WINDOW_FUTURE_MIN = config["window_future_min"]
   PUBLISH_INTERVAL = config["publish_interval"]
   API_BASE = config["api_base"]

3. Se vuoi il hot-reload, avvia il thread (opzionale):
   
   config_thread = threading.Thread(target=config_update_thread, daemon=True)
   config_thread.start()

"""

# =====================================================
# TEST ENDPOINT
# =====================================================

#if __name__ == "__main__":
#    print("\n" + "=" * 60)
#    print("TEST: Caricamento configurazione")
#    print("=" * 60 + "\n")
    
#    config = load_kafka_config_from_dashboard()
    
#    print("\nConfigurazione caricata:")
#    for key, value in config.items():
#        print(f"  {key}: {value}")
    
#    print("\n✓ Test completato")
