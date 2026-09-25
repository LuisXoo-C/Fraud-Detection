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

-- =====================================================================
-- CAPA GOLD: Vistas Analíticas para Power BI & Consumo HTTP
-- =====================================================================

-- 1. KPIs Globales
CREATE OR REPLACE VIEW fraud_db.gold_vw_kpi_summary AS
SELECT
    count() AS total_transactions,
    countIf(is_fraud = 1) AS total_fraud_transactions,
    countIf(is_fraud = 0) AS total_legit_transactions,
    round((countIf(is_fraud = 1) / count()) * 100, 2) AS fraud_rate_pct,
    round(sum(amount), 2) AS total_volume_amount,
    round(sumIf(amount, is_fraud = 1), 2) AS total_fraud_amount,
    round(avg(amount), 2) AS avg_ticket_amount,
    round(avgIf(amount, is_fraud = 1), 2) AS avg_fraud_ticket_amount,
    round(avg(risk_score), 1) AS avg_risk_score,
    uniqExact(account_id) AS total_unique_accounts,
    uniqExactIf(account_id, is_fraud = 1) AS total_compromised_accounts,
    max(timestamp) AS last_processed_transaction_time
FROM fraud_db.transactions_history;

-- 1.1 KPIs Diarios para Inteligencia de Tiempo (MoM / WoW / Sparklines)
CREATE OR REPLACE VIEW fraud_db.gold_vw_kpi_daily AS
SELECT
    toDate(timestamp) AS date,
    count() AS total_transactions,
    countIf(is_fraud = 1) AS total_fraud_transactions,
    countIf(is_fraud = 0) AS total_legit_transactions,
    round((countIf(is_fraud = 1) / count()) * 100, 2) AS fraud_rate_pct,
    round(sum(amount), 2) AS total_volume_amount,
    round(sumIf(amount, is_fraud = 1), 2) AS total_fraud_amount,
    round(avg(amount), 2) AS avg_ticket_amount,
    round(avg(risk_score), 1) AS avg_risk_score,
    uniqExactIf(account_id, is_fraud = 1) AS total_compromised_accounts
FROM fraud_db.transactions_history
GROUP BY date
ORDER BY date ASC;

-- 2. Alertas Críticas de Fraude (Feed Operativo)
CREATE OR REPLACE VIEW fraud_db.gold_vw_fraud_alerts AS
SELECT
    alert_id,
    transaction_id,
    account_id,
    timestamp,
    toDateTime(timestamp) AS alert_datetime,
    toDate(timestamp) AS alert_date,
    toHour(timestamp) AS alert_hour,
    amount,
    risk_score,
    arrayStringConcat(reasons, ' | ') AS fraud_reasons,
    length(reasons) AS rules_triggered_count,
    latitude,
    longitude,
    city,
    country,
    created_at
FROM fraud_db.fraud_alerts
ORDER BY timestamp DESC;

-- 3. Distribución Geográfica de Fraude (con fecha para interactividad en Power BI)
CREATE OR REPLACE VIEW fraud_db.gold_vw_geo_risk AS
SELECT
    toDate(timestamp) AS date,
    country,
    city,
    round(avg(latitude), 4) AS latitude,
    round(avg(longitude), 4) AS longitude,
    count() AS total_transactions,
    countIf(is_fraud = 1) AS fraud_count,
    round((countIf(is_fraud = 1) / count()) * 100, 2) AS fraud_rate_pct,
    round(sum(amount), 2) AS total_amount,
    round(sumIf(amount, is_fraud = 1), 2) AS fraud_amount,
    round(avg(risk_score), 1) AS avg_risk_score
FROM fraud_db.transactions_history
GROUP BY date, country, city
ORDER BY date ASC, fraud_amount DESC;

-- 4. Riesgo por Categoría de Comercio (con fecha para interactividad en Power BI)
CREATE OR REPLACE VIEW fraud_db.gold_vw_merchant_category_risk AS
SELECT
    toDate(timestamp) AS date,
    merchant_category,
    count() AS total_transactions,
    countIf(is_fraud = 1) AS fraud_count,
    round((countIf(is_fraud = 1) / count()) * 100, 2) AS fraud_rate_pct,
    round(sum(amount), 2) AS total_amount,
    round(sumIf(amount, is_fraud = 1), 2) AS fraud_amount,
    round(avg(risk_score), 1) AS avg_risk_score,
    round(avgIf(amount, is_fraud = 1), 2) AS avg_fraud_amount
FROM fraud_db.transactions_history
GROUP BY date, merchant_category
ORDER BY date ASC, fraud_amount DESC;

-- 5. Serie Temporal por Minuto
CREATE OR REPLACE VIEW fraud_db.gold_vw_timeseries_minute AS
SELECT
    toStartOfMinute(timestamp) AS time_minute,
    count() AS total_transactions,
    countIf(is_fraud = 1) AS fraud_count,
    round(sum(amount), 2) AS total_amount,
    round(sumIf(amount, is_fraud = 1), 2) AS fraud_amount,
    round(avg(risk_score), 1) AS avg_risk_score
FROM fraud_db.transactions_history
GROUP BY time_minute
ORDER BY time_minute DESC;

-- 6. Desglose por Causa/Regla de Fraude (con columna de fecha para interactividad en Power BI)
CREATE OR REPLACE VIEW fraud_db.gold_vw_fraud_by_reason AS
SELECT
    toDate(timestamp) AS date,
    arrayJoin(reasons) AS fraud_rule,
    count() AS triggered_occurrences,
    round(sum(amount), 2) AS exposed_amount,
    round(avg(risk_score), 1) AS avg_risk_score
FROM fraud_db.transactions_history
WHERE is_fraud = 1 AND length(reasons) > 0
GROUP BY date, fraud_rule
ORDER BY date ASC, triggered_occurrences DESC;

