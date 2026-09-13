import os

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TRANSACTIONS = os.getenv("TOPIC_TRANSACTIONS", "financial-transactions")
BRONZE_CONSUMER_GROUP = os.getenv("BRONZE_CONSUMER_GROUP", "bronze-s3-ingestion-group")

# MinIO / S3 Config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "bronze")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Pol?ticas de Buffer H?brido (Flush por Tiempo O Cantidad)
BATCH_SIZE = int(os.getenv("BRONZE_BATCH_SIZE", "500"))         # M?ximo 500 registros en RAM
FLUSH_INTERVAL_SEC = int(os.getenv("BRONZE_FLUSH_INTERVAL", "30")) # O m?ximo 30 segundos de espera
