-- =====================================================================
-- CAPA GOLD: Vistas Analíticas para ClickHouse & Power BI Dashboard
-- Base de Datos: fraud_db
-- =====================================================================

USE fraud_db;

-- ---------------------------------------------------------------------
-- 1. Vista de KPIs Generales (Métricas Ejecutivas)
-- Ideal para tarjetas de valor clave (Cards / KPIs) en Power BI
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 2. Vista de Alertas Críticas de Fraude (Feed Operativo en Tiempo Real)
-- Incluye razones aplanadas como texto legible para segmentadores y tablas en Power BI
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 3. Vista de Distribución Geográfica de Fraude
-- Para visualizaciones de Mapa (Bubble Map / Choropleth Map)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW fraud_db.gold_vw_geo_risk AS
SELECT
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
GROUP BY country, city;

-- ---------------------------------------------------------------------
-- 4. Vista de Riesgo por Categoría de Comercio
-- Para gráficos de barras horizontales o matriz de comercio
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW fraud_db.gold_vw_merchant_category_risk AS
SELECT
    merchant_category,
    count() AS total_transactions,
    countIf(is_fraud = 1) AS fraud_count,
    round((countIf(is_fraud = 1) / count()) * 100, 2) AS fraud_rate_pct,
    round(sum(amount), 2) AS total_amount,
    round(sumIf(amount, is_fraud = 1), 2) AS fraud_amount,
    round(avg(risk_score), 1) AS avg_risk_score,
    round(avgIf(amount, is_fraud = 1), 2) AS avg_fraud_amount
FROM fraud_db.transactions_history
GROUP BY merchant_category;

-- ---------------------------------------------------------------------
-- 5. Vista de Serie Temporal (Tendencia por Minuto / Hora)
-- Para gráficos de líneas y detección de picos anómalos
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 6. Vista Desglosada por Patrón / Regla de Fraude
-- Desanida el array de motivos usando arrayJoin para análisis de causa raíz
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW fraud_db.gold_vw_fraud_by_reason AS
SELECT
    arrayJoin(reasons) AS fraud_rule,
    count() AS triggered_occurrences,
    round(sum(amount), 2) AS exposed_amount,
    round(avg(risk_score), 1) AS avg_risk_score
FROM fraud_db.transactions_history
WHERE is_fraud = 1 AND length(reasons) > 0
GROUP BY fraud_rule
ORDER BY triggered_occurrences DESC;
