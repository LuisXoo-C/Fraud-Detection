import gzip
import io
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from minio import Minio
from kafka import KafkaConsumer

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_TRANSACTIONS,
    BRONZE_CONSUMER_GROUP,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET_NAME,
    MINIO_SECURE,
    BATCH_SIZE,
    FLUSH_INTERVAL_SEC,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [BRONZE] %(message)s")
logger = logging.getLogger(__name__)


def init_minio_client() -> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    # Asegurar existencia del bucket
    if not client.bucket_exists(MINIO_BUCKET_NAME):
        client.make_bucket(MINIO_BUCKET_NAME)
        logger.info(f"Bucket '{MINIO_BUCKET_NAME}' creado exitosamente en MinIO.")
    else:
        logger.info(f"Conectado a MinIO. Bucket '{MINIO_BUCKET_NAME}' verificado.")
    return client


def init_kafka_consumer(retries: int = 15, delay: int = 3) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC_TRANSACTIONS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=BRONZE_CONSUMER_GROUP,
                auto_offset_reset="earliest",       # Leer desde el inicio si es grupo nuevo
                enable_auto_commit=False,          # COMMIT MANUAL: solo tras guardar en MinIO
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1000,          # Desbloquea poll() cada 1s para evaluar tiempo
            )
            logger.info(f"Conectado a Kafka en {KAFKA_BOOTSTRAP_SERVERS}, topic: {TOPIC_TRANSACTIONS}")
            return consumer
        except Exception as e:
            logger.warning(f"Kafka no disponible a?n ({e}). Reintento {attempt + 1}/{retries} en {delay}s...")
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar a Kafka tras varios intentos.")


def flush_buffer_to_minio(minio_client: Minio, buffer: list, reason: str):
    """
    Comprime el buffer en memoria (.jsonl.gz) y lo sube con particionado temporal en S3/MinIO.
    """
    if not buffer:
        return

    now = datetime.now(timezone.utc)
    # Estructura Hive Partitioning est?ndar para Data Lakes
    partition_path = f"transactions/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/hour={now.strftime('%H')}"
    file_id = uuid.uuid4().hex[:8]
    object_name = f"{partition_path}/bronze_{now.strftime('%Y%m%d_%H%M%S')}_{file_id}.jsonl.gz"

    # 1. Serializar y comprimir en memoria (sin tocar disco local)
    compressed_bytes = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_bytes, mode="wb") as gz_file:
        for record in buffer:
            line = json.dumps(record, ensure_ascii=False) + "\n"
            gz_file.write(line.encode("utf-8"))

    data_length = compressed_bytes.tell()
    compressed_bytes.seek(0)

    # 2. Subir a MinIO (Object Storage)
    minio_client.put_object(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=object_name,
        data=compressed_bytes,
        length=data_length,
        content_type="application/gzip",
    )

    logger.info(
        f"[FLUSH] Guardados {len(buffer)} eventos en MinIO | Motivo: {reason} | Objeto: {object_name} ({data_length / 1024:.2f} KB)"
    )


def run_bronze_consumer():
    logger.info("Iniciando Consumidor de Capa Bronze...")
    minio_client = init_minio_client()
    consumer = init_kafka_consumer()

    buffer = []
    last_flush_time = time.time()

    try:
        while True:
            # Poll por mensajes (bloquea m?ximo 1000ms si no hay mensajes nuevos)
            msg_batch = consumer.poll(timeout_ms=1000)

            for topic_partition, messages in msg_batch.items():
                for msg in messages:
                    buffer.append(msg.value)

            time_elapsed = time.time() - last_flush_time
            size_threshold_met = len(buffer) >= BATCH_SIZE
            time_threshold_met = time_elapsed >= FLUSH_INTERVAL_SEC and len(buffer) > 0

            if size_threshold_met or time_threshold_met:
                flush_reason = f"Tama?o ({len(buffer)}/{BATCH_SIZE})" if size_threshold_met else f"Tiempo ({time_elapsed:.1f}s)"
                
                # 1. Escribir a MinIO
                flush_buffer_to_minio(minio_client, buffer, flush_reason)

                # 2. Confirmar offsets en Kafka (Garant?a At-Least-Once)
                consumer.commit()

                # 3. Limpiar buffer y reiniciar reloj
                buffer.clear()
                last_flush_time = time.time()

    except KeyboardInterrupt:
        logger.info("Cerrando consumidor Bronze...")
        if buffer:
            logger.info("Vaciando registros restantes antes de salir...")
            flush_buffer_to_minio(minio_client, buffer, "Shutdown")
            consumer.commit()
    finally:
        consumer.close()


if __name__ == "__main__":
    run_bronze_consumer()
