CREATE DATABASE IF NOT EXISTS fraud_db;

-- Tabla para Historial Completo de Transacciones
CREATE TABLE IF NOT EXISTS fraud_db.transactions_history (
    transaction_id String,
    account_id String,
    timestamp DateTime64(3, 'UTC'),
    amount Decimal(12, 2),
    currency LowCardinality(String),
    merchant_id String,
    merchant_category LowCardinality(String),
    latitude Float64,
    longitude Float64,
    city String,
    country LowCardinality(String),
    device_id String,
    risk_score UInt8,
    is_fraud UInt8,
    reasons Array(String),
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (account_id, timestamp, transaction_id);

-- Tabla para Alertas Críticas (Score de Riesgo > 80)
CREATE TABLE IF NOT EXISTS fraud_db.fraud_alerts (
    alert_id UUID DEFAULT generateUUIDv4(),
    transaction_id String,
    account_id String,
    timestamp DateTime64(3, 'UTC'),
    amount Decimal(12, 2),
    risk_score UInt8,
    reasons Array(String),
    latitude Float64,
    longitude Float64,
    city String,
    country LowCardinality(String),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (risk_score, timestamp, account_id);
