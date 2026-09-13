import os
import json
import logging
from datetime import datetime
import clickhouse_connect

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    collect_list,
    struct,
    udf,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    ArrayType,
    StringType,
    BooleanType,
)

from .schemas import TRANSACTION_SCHEMA
from .rules import evaluate_impossible_travel, compute_risk_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [SPARK-STREAM] %(message)s")
logger = logging.getLogger(__name__)

# Entorno
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TRANSACTIONS = os.getenv("TOPIC_TRANSACTIONS", "financial-transactions")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "fraud_db")


def get_spark_session() -> SparkSession:
    """
    Crea la sesi?n de PySpark configurada con paquetes de Kafka Structured Streaming.
    """
    return (
        SparkSession.builder
        .appName("Banking-Fraud-Detection-Engine")
        .master("local[*]")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )


def write_batch_to_clickhouse(df_batch, batch_id: int):
    """
    Funcion ejecutada por foreachBatch en cada micro-batch:
    1. Procesa transacciones concurrentes por cuenta para evaluar Impossible Travel y Velocity.
    2. Calcula Risk Score.
    3. Separa y escribe en ClickHouse: Alertas Cr?ticas (>80) vs Historial Limpio.
    """
    if df_batch.isEmpty():
        return

    logger.info(f"--- Procesando Micro-Batch {batch_id} ({df_batch.count()} registros) ---")
    records = df_batch.collect()

    history_rows = []
    alert_rows = []

    # Agrupar registros por cuenta dentro del lote para evaluar secuencia temporal
    account_groups = {}
    for row in records:
        acc_id = row["account_id"]
        account_groups.setdefault(acc_id, []).append(row)

    for acc_id, tx_list in account_groups.items():
        # Ordenar cronol?gicamente las transacciones de esta cuenta
        sorted_txs = sorted(tx_list, key=lambda x: x["event_time"])

        prev_lat = None
        prev_lon = None
        prev_epoch = None

        velocity_count = len(sorted_txs)

        for tx in sorted_txs:
            curr_lat = float(tx["location"]["latitude"])
            curr_lon = float(tx["location"]["longitude"])
            curr_epoch = float(tx["event_time"].timestamp())

            is_impossible, dist_km, speed_kmh = evaluate_impossible_travel(
                prev_lat=prev_lat,
                prev_lon=prev_lon,
                prev_time_epoch=prev_epoch,
                curr_lat=curr_lat,
                curr_lon=curr_lon,
                curr_time_epoch=curr_epoch,
            )

            # Calcular Score de Riesgo
            risk_score, reasons = compute_risk_score(
                is_impossible_travel=is_impossible,
                velocity_count_10m=velocity_count,
                amount=float(tx["amount"]),
            )

            is_fraud = 1 if risk_score > 80 else 0

            # Fila para ClickHouse: transactions_history
            history_rows.append([
                str(tx["transaction_id"]),
                str(tx["account_id"]),
                tx["event_time"],
                float(tx["amount"]),
                str(tx["currency"]),
                str(tx["merchant"]["merchant_id"]),
                str(tx["merchant"]["category"]),
                curr_lat,
                curr_lon,
                str(tx["location"]["city"]),
                str(tx["location"]["country"]),
                str(tx["device"]["device_id"]),
                int(risk_score),
                int(is_fraud),
                reasons,
            ])

            # Si es Alerta Cr?tica (Score > 80), insertar en tabla de alertas
            if is_fraud:
                alert_rows.append([
                    str(tx["transaction_id"]),
                    str(tx["account_id"]),
                    tx["event_time"],
                    float(tx["amount"]),
                    int(risk_score),
                    reasons,
                    curr_lat,
                    curr_lon,
                    str(tx["location"]["city"]),
                    str(tx["location"]["country"]),
                ])

            # Actualizar estado de ?ltima posici?n para la siguiente tx
            prev_lat = curr_lat
            prev_lon = curr_lon
            prev_epoch = curr_epoch

    # Escritura en ClickHouse
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB,
        )

        if history_rows:
            client.insert(
                "transactions_history",
                history_rows,
                column_names=[
                    "transaction_id", "account_id", "timestamp", "amount", "currency",
                    "merchant_id", "merchant_category", "latitude", "longitude",
                    "city", "country", "device_id", "risk_score", "is_fraud", "reasons"
                ]
            )

        if alert_rows:
            client.insert(
                "fraud_alerts",
                alert_rows,
                column_names=[
                    "transaction_id", "account_id", "timestamp", "amount",
                    "risk_score", "reasons", "latitude", "longitude", "city", "country"
                ]
            )
            logger.warning(f"!!! [ALERTAS] Insertadas {len(alert_rows)} alertas cr?ticas de fraude en ClickHouse !!!")

        logger.info(f"Micro-Batch {batch_id} insertado exitosamente en ClickHouse (Total txs: {len(history_rows)})")

    except Exception as e:
        logger.error(f"Error escribiendo en ClickHouse en Batch {batch_id}: {e}")


def run_streaming_pipeline():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Iniciando PySpark Structured Streaming desde Kafka ({KAFKA_BOOTSTRAP_SERVERS})...")

    # 1. Lectura del Stream desde Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_TRANSACTIONS)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2. Deserializaci?n JSON con el esquema formal
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    # 3. Aplicar Watermark (Tolerancia de retraso: 5 minutos)
    watermarked_df = parsed_df.withWatermark("event_time", "5 minutes")

    # 4. Sink Stateful hacia ClickHouse con foreachBatch
    query = (
        watermarked_df.writeStream
        .foreachBatch(write_batch_to_clickhouse)
        .outputMode("update")
        .start()
    )

    logger.info("Pipeline de Streaming activo y escuchando eventos...")
    query.awaitTermination()


if __name__ == "__main__":
    run_streaming_pipeline()
