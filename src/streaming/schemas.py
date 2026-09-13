from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)

# Esquema para el objeto merchant
MERCHANT_SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("category", StringType(), False),
])

# Esquema para el objeto location
LOCATION_SCHEMA = StructType([
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("city", StringType(), False),
    StructField("country", StringType(), False),
])

# Esquema para el objeto device
DEVICE_SCHEMA = StructType([
    StructField("device_id", StringType(), False),
    StructField("ip_address", StringType(), False),
    StructField("channel", StringType(), False),
])

# Esquema para metadata de simulaci?n
SIMULATION_METADATA_SCHEMA = StructType([
    StructField("is_simulated_fraud", BooleanType(), True),
    StructField("anomaly_type", StringType(), True),
])

# Esquema ra?z para la transacci?n completa le?da desde Kafka
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("timestamp", StringType(), False), # Se castea a TimestampType en el pipeline
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("merchant", MERCHANT_SCHEMA, False),
    StructField("location", LOCATION_SCHEMA, False),
    StructField("device", DEVICE_SCHEMA, False),
    StructField("simulation_metadata", SIMULATION_METADATA_SCHEMA, True),
])
