"""
Generador Estadístico de Datos Históricos de Fraude Bancario (Enero 2026 - Septiembre 2026)
Genera transacciones con distribuciones probabilísticas realistas:
- Distribución de montos Log-Normal (Pareto / Ley de potencias)
- Ritmo circadiano (Proceso de Poisson no homogéneo con picos de mediodía y noche)
- Estacionalidad (efecto quincena, fines de semana, temporadas comerciales)
- Anomalías de fraude coherentes (Impossible Travel, Velocity Attack, Midnight Cashout)
- Inserción directa y eficiente por lotes en ClickHouse
"""

import os
import uuid
import math
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import clickhouse_connect

# Configuraciones de Entorno
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "fraud_db")

REFERENCE_LOCATIONS = [
    {"city": "Mexico City", "country": "MX", "lat": 19.4326, "lon": -99.1332},
    {"city": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
    {"city": "London", "country": "GB", "lat": 51.5074, "lon": -0.1278},
    {"city": "Madrid", "country": "ES", "lat": 40.4168, "lon": -3.7038},
    {"city": "Tokyo", "country": "JP", "lat": 35.6762, "lon": 139.6503},
    {"city": "Sydney", "country": "AU", "lat": -33.8688, "lon": 151.2093},
    {"city": "Buenos Aires", "country": "AR", "lat": -34.6037, "lon": -58.3816},
    {"city": "Bogota", "country": "CO", "lat": 4.7110, "lon": -74.0721},
    {"city": "Sao Paulo", "country": "BR", "lat": -23.5505, "lon": -46.6333},
]

LEGIT_CATEGORIES = [
    ("GROCERY", 0.35),
    ("RESTAURANT", 0.25),
    ("ONLINE_PAYMENT", 0.12),
    ("ELECTRONICS", 0.08),
    ("TRAVEL", 0.06),
    ("CASH_ADVANCE", 0.04),
    ("LUXURY_GOODS", 0.03),
    ("GAMBLING", 0.02),
    ("TRANSFER_OUT", 0.05),
]

FRAUD_CATEGORIES = [
    ("LUXURY_GOODS", 0.35),
    ("CASH_ADVANCE", 0.30),
    ("ELECTRONICS", 0.20),
    ("GAMBLING", 0.10),
    ("TRANSFER_OUT", 0.05),
]

HOURLY_WEIGHTS = [
    0.04, 0.02, 0.01, 0.01, 0.02, 0.06,  # 00:00 - 05:00 (valle de madrugada)
    0.18, 0.40, 0.70, 0.85, 0.90, 0.95,  # 06:00 - 11:00 (mañana)
    1.30, 1.40, 1.15,                    # 12:00 - 14:00 (pico almuerzo)
    0.85, 0.90, 1.05,                    # 15:00 - 17:00 (tarde)
    1.35, 1.45, 1.30, 1.10,              # 18:00 - 21:00 (pico compras/cena)
    0.70, 0.35                           # 22:00 - 23:00 (cierre del día)
]

def generate_accounts_pool(num_accounts: int = 500) -> list[dict]:
    """Genera perfiles de cuentas con ubicación habitual y dispositivo principal."""
    accounts = []
    for i in range(num_accounts):
        home_loc = random.choice(REFERENCE_LOCATIONS)
        accounts.append({
            "account_id": f"ACC-{1000 + i}",
            "home_loc": home_loc,
            "primary_device": f"DEV-{uuid.uuid4().hex[:10].upper()}",
            "currency": "USD",
        })
    return accounts

def get_day_multiplier(dt: datetime) -> float:
    """Calcula multiplicador de volumen según día de la semana, quincena y estacionalidad."""
    mult = 1.0

    # 1. Efecto Quincena (días 14-16 y 28-31)
    day = dt.day
    if day in [14, 15, 16]:
        mult *= 1.40
    elif day in [28, 29, 30, 31]:
        mult *= 1.45

    # 2. Día de la semana (Lunes bajo, Viernes/Sábado alto)
    weekday = dt.weekday() # 0 = Lunes, 6 = Domingo
    if weekday == 0:
        mult *= 0.85
    elif weekday in [4, 5]: # Viernes, Sábado
        mult *= 1.30
    elif weekday == 6: # Domingo
        mult *= 1.10

    # 3. Estacionalidad mensual
    month = dt.month
    if month == 2 and 12 <= day <= 15: # San Valentín
        mult *= 1.30
    elif month in [7, 8]: # Vacaciones de verano
        mult *= 1.15

    return mult

def generate_legit_amount() -> float:
    """Genera montos siguiendo una distribución Log-Normal típica de transacciones bancarias."""
    # mu=3.5 -> mediana de ~$33.00, 95% < $150
    amt = random.lognormvariate(3.5, 0.72)
    amt = max(3.50, min(amt, 750.0))
    # 1.5% de compras legítimas altas ($750 - $1,500)
    if random.random() < 0.015:
        amt = random.uniform(800.0, 1400.0)
    return round(amt, 2)

def generate_historical_dataset(start_date: datetime, end_date: datetime, accounts: list[dict], base_daily_tx: int = 380):
    current_day = start_date
    history_records = []
    alert_records = []

    cat_legit_names = [c[0] for c in LEGIT_CATEGORIES]
    cat_legit_weights = [c[1] for c in LEGIT_CATEGORIES]
    cat_fraud_names = [c[0] for c in FRAUD_CATEGORIES]
    cat_fraud_weights = [c[1] for c in FRAUD_CATEGORIES]

    print(f"Iniciando generación estadística desde {start_date.date()} hasta {end_date.date()}...")

    while current_day < end_date:
        mult = get_day_multiplier(current_day)
        daily_tx_target = int(base_daily_tx * mult * random.uniform(0.92, 1.08))

        # Distribuir las transacciones por hora según el ritmo circadiano
        hours_distribution = random.choices(range(24), weights=HOURLY_WEIGHTS, k=daily_tx_target)
        hours_distribution.sort()

        # Determinar cuántos ataques de fraude ocurren hoy (1.2% - 1.8% de fraudes)
        fraud_scenario_count = max(1, int(daily_tx_target * random.uniform(0.012, 0.018)))

        # Escoger cuentas objetivo para fraude hoy
        fraud_accounts = random.sample(accounts, k=min(fraud_scenario_count, len(accounts)))

        # Generar eventos legítimos
        for hour in hours_distribution:
            acc = random.choice(accounts)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            microsecond = random.randint(100, 999) * 1000
            tx_time = current_day.replace(hour=hour, minute=minute, second=second, microsecond=microsecond)

            base_loc = acc["home_loc"]
            lat = base_loc["lat"] + random.uniform(-0.03, 0.03)
            lon = base_loc["lon"] + random.uniform(-0.03, 0.03)
            amount = generate_legit_amount()
            cat = random.choices(cat_legit_names, weights=cat_legit_weights, k=1)[0]
            tx_id = str(uuid.uuid4())

            # Riesgo base bajo (0 a 15), o 20 si superó $800
            if amount >= 800.0:
                risk_score = 20
                reasons = ["HIGH_TRANSACTION_AMOUNT"]
            else:
                risk_score = random.randint(0, 15)
                reasons = []

            history_records.append((
                tx_id,
                acc["account_id"],
                tx_time,
                Decimal(f"{amount:.2f}"),
                acc["currency"],
                f"MER-{random.randint(100, 999)}",
                cat,
                lat,
                lon,
                base_loc["city"],
                base_loc["country"],
                acc["primary_device"],
                risk_score,
                0, # is_fraud = 0
                reasons,
                tx_time + timedelta(seconds=2),
            ))

        # Inyectar Ataques de Fraude Coherentes
        for f_acc in fraud_accounts:
            fraud_type = random.choice(["IMPOSSIBLE_TRAVEL", "VELOCITY_BURST", "MIDNIGHT_CASHOUT"])

            if fraud_type == "IMPOSSIBLE_TRAVEL":
                # Paso 1: Transacción previa en casa
                f_hour = random.randint(7, 21)
                f_min = random.randint(0, 40)
                t1 = current_day.replace(hour=f_hour, minute=f_min, second=random.randint(0, 59), microsecond=random.randint(100, 900)*1000)
                base_loc = f_acc["home_loc"]

                # Paso 2: Salto a miles de km en 15-40 minutos
                t2 = t1 + timedelta(minutes=random.randint(15, 45))
                distant_locs = [l for l in REFERENCE_LOCATIONS if l["city"] != base_loc["city"]]
                target_loc = random.choice(distant_locs)
                lat2 = target_loc["lat"] + random.uniform(-0.02, 0.02)
                lon2 = target_loc["lon"] + random.uniform(-0.02, 0.02)

                amount = round(random.uniform(750.0, 1800.0), 2)
                reasons = ["IMPOSSIBLE_TRAVEL_DETECTED"]
                risk_score = 70
                if amount >= 800.0:
                    reasons.append("HIGH_TRANSACTION_AMOUNT")
                    risk_score += 20
                reasons.append("UNKNOWN_DEVICE_ID")
                risk_score = min(100, risk_score + 10) # score >= 90 => is_fraud = 1

                tx_id = str(uuid.uuid4())
                row_history = (
                    tx_id,
                    f_acc["account_id"],
                    t2,
                    Decimal(f"{amount:.2f}"),
                    f_acc["currency"],
                    f"MER-{random.randint(800, 999)}",
                    random.choices(cat_fraud_names, weights=cat_fraud_weights, k=1)[0],
                    lat2,
                    lon2,
                    target_loc["city"],
                    target_loc["country"],
                    f"DEV-{uuid.uuid4().hex[:10].upper()}",
                    risk_score,
                    1, # is_fraud = 1
                    reasons,
                    t2 + timedelta(seconds=2),
                )
                history_records.append(row_history)

                row_alert = (
                    uuid.uuid4(),
                    tx_id,
                    f_acc["account_id"],
                    t2,
                    Decimal(f"{amount:.2f}"),
                    risk_score,
                    reasons,
                    lat2,
                    lon2,
                    target_loc["city"],
                    target_loc["country"],
                    t2 + timedelta(seconds=2),
                )
                alert_records.append(row_alert)

            elif fraud_type == "VELOCITY_BURST":
                # Ráfaga de 4 a 6 transacciones en < 5 minutos
                burst_count = random.randint(4, 6)
                burst_base_time = current_day.replace(
                    hour=random.randint(10, 22),
                    minute=random.randint(0, 50),
                    second=random.randint(0, 30)
                )
                base_loc = f_acc["home_loc"]

                for b_idx in range(burst_count):
                    t_burst = burst_base_time + timedelta(seconds=b_idx * random.randint(25, 60))
                    b_amount = round(random.uniform(250.0, 950.0), 2)
                    b_tx_id = str(uuid.uuid4())
                    b_reasons = [f"HIGH_VELOCITY_BURST({burst_count}_tx_in_10m)"]
                    b_score = 50 + (burst_count * 7) # ~78 - 92
                    if b_amount >= 800.0:
                        b_reasons.append("HIGH_TRANSACTION_AMOUNT")
                        b_score += 20
                    b_score = min(100, b_score)
                    b_is_fraud = 1 if b_score > 80 else 0

                    row_h = (
                        b_tx_id,
                        f_acc["account_id"],
                        t_burst,
                        Decimal(f"{b_amount:.2f}"),
                        f_acc["currency"],
                        f"MER-{random.randint(700, 899)}",
                        random.choices(cat_fraud_names, weights=cat_fraud_weights, k=1)[0],
                        base_loc["lat"] + random.uniform(-0.01, 0.01),
                        base_loc["lon"] + random.uniform(-0.01, 0.01),
                        base_loc["city"],
                        base_loc["country"],
                        f_acc["primary_device"],
                        b_score,
                        b_is_fraud,
                        b_reasons,
                        t_burst + timedelta(seconds=2),
                    )
                    history_records.append(row_h)

                    if b_is_fraud:
                        row_a = (
                            uuid.uuid4(),
                            b_tx_id,
                            f_acc["account_id"],
                            t_burst,
                            Decimal(f"{b_amount:.2f}"),
                            b_score,
                            b_reasons,
                            base_loc["lat"],
                            base_loc["lon"],
                            base_loc["city"],
                            base_loc["country"],
                            t_burst + timedelta(seconds=2),
                        )
                        alert_records.append(row_a)

            elif fraud_type == "MIDNIGHT_CASHOUT":
                # Retiro / Compra fuerte no autorizada en madrugada (01:00 - 04:30)
                m_hour = random.randint(1, 4)
                m_min = random.randint(0, 59)
                t_m = current_day.replace(hour=m_hour, minute=m_min, second=random.randint(0, 59))
                base_loc = f_acc["home_loc"]
                m_amount = round(random.uniform(850.0, 2500.0), 2)
                m_tx_id = str(uuid.uuid4())
                m_reasons = ["HIGH_TRANSACTION_AMOUNT", "UNUSUAL_OFF_HOURS_ACTIVITY", "SUSPICIOUS_ATM_WITHDRAWAL"]
                m_score = 90

                row_h = (
                    m_tx_id,
                    f_acc["account_id"],
                    t_m,
                    Decimal(f"{m_amount:.2f}"),
                    f_acc["currency"],
                    f"MER-{random.randint(900, 999)}",
                    "CASH_ADVANCE",
                    base_loc["lat"] + random.uniform(-0.02, 0.02),
                    base_loc["lon"] + random.uniform(-0.02, 0.02),
                    base_loc["city"],
                    base_loc["country"],
                    f"DEV-{uuid.uuid4().hex[:10].upper()}",
                    m_score,
                    1,
                    m_reasons,
                    t_m + timedelta(seconds=2),
                )
                history_records.append(row_h)

                row_a = (
                    uuid.uuid4(),
                    m_tx_id,
                    f_acc["account_id"],
                    t_m,
                    Decimal(f"{m_amount:.2f}"),
                    m_score,
                    m_reasons,
                    base_loc["lat"],
                    base_loc["lon"],
                    base_loc["city"],
                    base_loc["country"],
                    t_m + timedelta(seconds=2),
                )
                alert_records.append(row_a)

        current_day += timedelta(days=1)

    return history_records, alert_records

def main():
    print(f"Conectando a ClickHouse en {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    print("Limpiando tablas previas (TRUNCATE)...")
    client.command("TRUNCATE TABLE fraud_db.transactions_history")
    client.command("TRUNCATE TABLE fraud_db.fraud_alerts")
    print("Tablas limpiadas exitosamente.")

    # Generar perfiles de 500 cuentas
    accounts = generate_accounts_pool(500)

    start_date = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_date = datetime(2026, 9, 15, 0, 0, 0, tzinfo=timezone.utc)

    history_records, alert_records = generate_historical_dataset(start_date, end_date, accounts, base_daily_tx=380)

    # Ordenar cronológicamente todo el conjunto
    history_records.sort(key=lambda r: r[2])
    alert_records.sort(key=lambda r: r[3])

    print(f"Total registros generados en transactions_history: {len(history_records):,}")
    print(f"Total registros generados en fraud_alerts: {len(alert_records):,}")

    # Columnas de inserción
    history_cols = [
        "transaction_id", "account_id", "timestamp", "amount", "currency",
        "merchant_id", "merchant_category", "latitude", "longitude", "city",
        "country", "device_id", "risk_score", "is_fraud", "reasons", "processed_at"
    ]

    alert_cols = [
        "alert_id", "transaction_id", "account_id", "timestamp", "amount",
        "risk_score", "reasons", "latitude", "longitude", "city", "country", "created_at"
    ]

    # Inserción en batches de 15,000 registros
    BATCH_SIZE = 15000
    print("Insertando datos en fraud_db.transactions_history...")
    for i in range(0, len(history_records), BATCH_SIZE):
        batch = history_records[i : i + BATCH_SIZE]
        client.insert("transactions_history", batch, column_names=history_cols)
        print(f"  -> Insertados {min(i + BATCH_SIZE, len(history_records)):,} / {len(history_records):,}...")

    print("Insertando datos en fraud_db.fraud_alerts...")
    for i in range(0, len(alert_records), BATCH_SIZE):
        batch = alert_records[i : i + BATCH_SIZE]
        client.insert("fraud_alerts", batch, column_names=alert_cols)
        print(f"  -> Insertadas alertas {min(i + BATCH_SIZE, len(alert_records)):,} / {len(alert_records):,}...")

    # Verificación final
    total_tx = client.command("SELECT count() FROM transactions_history")
    total_fraud = client.command("SELECT countIf(is_fraud = 1) FROM transactions_history")
    total_alerts = client.command("SELECT count() FROM fraud_alerts")
    date_range = client.query("SELECT min(timestamp), max(timestamp) FROM transactions_history").result_rows

    print("\n================ RESUMEN DE POBLACIÓN DE DATOS ================")
    print(f"Rango de Fechas: {date_range[0][0]} -> {date_range[0][1]}")
    print(f"Total Transacciones: {total_tx:,}")
    print(f"Transacciones Marcadas como Fraude: {total_fraud:,} ({round(total_fraud / total_tx * 100, 2)}%)")
    print(f"Alertas Críticas Registradas: {total_alerts:,}")
    print("=================================================================\n")

if __name__ == "__main__":
    main()
