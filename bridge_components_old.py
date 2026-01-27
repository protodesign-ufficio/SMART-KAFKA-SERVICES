import json
import time
import threading
from kafka import KafkaConsumer, KafkaProducer
from pyais import decode as ais_decode
import os
from config_loader import load_kafka_config_from_dashboard

config = load_kafka_config_from_dashboard()
PUBLISH_INTERVAL_SEC = config["publish_interval"]
CONFIG_LAST_UPDATE = config.get("last_update", time.time())
# ----------------------------------------------------
# CONFIG
# ----------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")


INPUT_TOPIC = "ais.raw"
OUTPUT_TOPIC = "analytics_ais.raw"

# PUBLISH_INTERVAL_SEC = 30

COMPONENTS = [
    "engine_main",
    "generator",
    "gearbox"
]

# Stato globale
ships = {}  # mmsi -> ship_state

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ----------------------------------------------------
# MODELLO DI UTILIZZO (MODIFICABILE)
# ----------------------------------------------------
def is_component_active(component, ais_state):
    """
    FUNZIONE DI CALCOLO USURA.
    OGGI: componente attiva se la nave si muove.
    """
    sog = ais_state.get("speed")
    if sog is None:
        return False
    return sog > 0.1


# ----------------------------------------------------
# UPDATE CONTINUO DEL TEMPO DI UTILIZZO
# ----------------------------------------------------
def update_component_usage(ship, now):
    last_ts = ship["last_update_ts"]
    dt = now - last_ts
    ship["last_update_ts"] = now

    if dt <= 0:
        return

    for component, state in ship["components"].items():
        active = is_component_active(component, ship["ais"])
        if active:
            state["usage_total"] += dt
        state["active"] = active


def normalize_nmea(raw_value):
    try:
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8", errors="ignore")

        raw_value = raw_value.strip()

        if raw_value.startswith("{"):
            data = json.loads(raw_value)
            if "fields" in data and "value" in data["fields"]:
                return data["fields"]["value"]

        if raw_value.startswith("!AIVDM"):
            return raw_value

        if "!" in raw_value:
            return raw_value[raw_value.find("!"):]

        return None
    except:
        return None


# ----------------------------------------------------
# AIS CONSUMER LOOP (REAL-TIME)
# ----------------------------------------------------
def kafka_loop():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        value_deserializer=lambda x: x
    )

    for msg in consumer:
        try:
            raw = normalize_nmea(msg.value)
            if not raw:
                continue

            decoded = ais_decode(raw)
            data = decoded.asdict()
            #print(data)

            if "speed" not in data:
                continue

            mmsi = str(data.get("mmsi"))
            if not mmsi:
                continue

            now = time.time()

            ship = ships.setdefault(
                mmsi,
                {
                    "ais": {},
                    "last_update_ts": now,
                    "components": {
                        c: {"usage_total": 0.0, "active": False}
                        for c in COMPONENTS
                    }
                }
            )

            ship["ais"] = data
            update_component_usage(ship, now)

        except Exception as e:
            print("AIS decode error:", e)

# ----------------------------------------------------
# PUBLISH LOOP (PERIODICO)
# ----------------------------------------------------
def publish_loop():
    while True:
        time.sleep(PUBLISH_INTERVAL_SEC)
        now = time.time()

        for mmsi, ship in ships.items():
            print(mmsi)
            for component, state in ship["components"].items():
                producer.send(
                    OUTPUT_TOPIC,
                    {
                        "type": "component_usage",
                        "mmsi": mmsi,
                        "component": component,
                        "usage_seconds_total": int(state["usage_total"]),
                        "active": state["active"],
                        "timestamp": now
                    }
                )


# ----------------------------------------------------
# AUTO-RELOAD CONFIGURAZIONI
# ----------------------------------------------------      
        
        
        
def periodic_config_check():
    global PUBLISH_INTERVAL, CONFIG_LAST_UPDATE

    new_config = load_kafka_config_from_dashboard()

    if new_config.get("last_update", 0) > CONFIG_LAST_UPDATE:
        log(
            f"[CONFIG] update: "
            #f"WINDOW_FUTURE_MIN {WINDOW_FUTURE_MIN} -> {new_config['window_future']}, "
            f"PUBLISH_INTERVAL {PUBLISH_INTERVAL_SEC} -> {new_config['publish_interval']}"
        )

        #WINDOW_FUTURE_MIN = new_config["window_future"]
        PUBLISH_INTERVAL_SEC = new_config["publish_interval"]
        CONFIG_LAST_UPDATE = new_config["last_update"]

        
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


# ----------------------------------------------------
# RUN
# ----------------------------------------------------
if __name__ == "__main__":
    print("[component-usage] bridge running (continuous calc, periodic publish)", flush=True)

    threading.Thread(target=kafka_loop, daemon=True).start()
    threading.Thread(target=publish_loop, daemon=True).start()
    threading.Thread(target=config_update_thread, daemon=True).start()

    while True:
        time.sleep(1)
