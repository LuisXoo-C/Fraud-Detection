import json
import logging
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_TRANSACTIONS,
    TOTAL_ACCOUNTS,
    EVENTS_PER_SECOND,
    FRAUD_RATIO,
)
from .scenarios import (
    generate_normal_transaction,
    generate_impossible_travel_anomaly,
    generate_velocity_burst,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_kafka_producer(retries: int = 15, delay: int = 3) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
            )
            logger.info(f"Conectado exitosamente a Kafka en {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            logger.warning(f"Kafka no disponible aún ({e}). Reintento {attempt + 1}/{retries} en {delay}s...")
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar a Kafka tras múltiples intentos.")


def main():
    logger.info("Iniciando Generador Concurrente de Transacciones...")
    producer = create_kafka_producer()
    accounts = [f"ACC-{1000 + i}" for i in range(TOTAL_ACCOUNTS)]
    sleep_interval = 1.0 / EVENTS_PER_SECOND

    try:
        while True:
            account_id = random.choice(accounts)
            is_anomaly = random.random() < FRAUD_RATIO

            if is_anomaly:
                anomaly_choice = random.choice(["IMPOSSIBLE_TRAVEL", "VELOCITY_ATTACK"])
                if anomaly_choice == "IMPOSSIBLE_TRAVEL":
                    event = generate_impossible_travel_anomaly(account_id)
                    producer.send(TOPIC_TRANSACTIONS, key=event.account_id, value=event.model_dump())
                    logger.warning(f"[FRAUD SIM: Impossible Travel] Cuenta: {event.account_id}, Ciudad: {event.location.city}, Monto: ${event.amount}")
                else:
                    burst_events = generate_velocity_burst(account_id, count=random.randint(4, 7))
                    for ev in burst_events:
                        producer.send(TOPIC_TRANSACTIONS, key=ev.account_id, value=ev.model_dump())
                    logger.warning(f"[FRAUD SIM: Velocity Burst] Cuenta: {account_id} lanzó {len(burst_events)} transacciones en ráfaga")
            else:
                event = generate_normal_transaction(account_id)
                producer.send(TOPIC_TRANSACTIONS, key=event.account_id, value=event.model_dump())
                logger.info(f"[NORMAL] Tx: {event.transaction_id[:8]}... | Cuenta: {event.account_id} | ${event.amount} | {event.location.city}")

            producer.flush()
            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        logger.info("Generador detenido por el usuario.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
