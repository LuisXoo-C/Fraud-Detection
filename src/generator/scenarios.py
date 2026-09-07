import random
import uuid
from datetime import datetime, timezone
from faker import Faker

from .models import (
    TransactionEvent,
    MerchantInfo,
    LocationInfo,
    DeviceInfo,
    SimulationMetadata,
    Currency,
    TransactionType,
    MerchantCategory,
    Channel,
    AnomalyType,
)
from .config import REFERENCE_LOCATIONS

fake = Faker()

# Memoria en caliente del generador para mantener consistencia de usuarios
account_profiles = {}


def get_or_create_account_profile(account_id: str):
    if account_id not in account_profiles:
        home_loc = random.choice(REFERENCE_LOCATIONS)
        account_profiles[account_id] = {
            "account_id": account_id,
            "home_location": home_loc,
            "last_location": home_loc,
            "last_timestamp": datetime.now(timezone.utc),
            "primary_device_id": f"DEV-{fake.md5()[:10].upper()}",
            "primary_channel": random.choice([Channel.MOBILE_APP, Channel.WEB_BROWSER]),
            "currency": Currency.USD,
        }
    return account_profiles[account_id]


def generate_normal_transaction(account_id: str) -> TransactionEvent:
    profile = get_or_create_account_profile(account_id)
    now = datetime.now(timezone.utc)

    # Pequeña variación de lat/lon alrededor de su ubicación habitual
    base_loc = profile["home_location"]
    lat = base_loc["lat"] + random.uniform(-0.05, 0.05)
    lon = base_loc["lon"] + random.uniform(-0.05, 0.05)

    profile["last_location"] = {"city": base_loc["city"], "country": base_loc["country"], "lat": lat, "lon": lon}
    profile["last_timestamp"] = now

    return TransactionEvent(
        transaction_id=str(uuid.uuid4()),
        account_id=account_id,
        timestamp=now.isoformat(),
        amount=round(random.uniform(5.0, 350.0), 2),
        currency=profile["currency"],
        transaction_type=random.choice([TransactionType.PURCHASE, TransactionType.ONLINE_PAYMENT]),
        merchant=MerchantInfo(
            merchant_id=f"MER-{random.randint(100, 999)}",
            name=fake.company(),
            category=random.choice(
                [MerchantCategory.GROCERY, MerchantCategory.RESTAURANT, MerchantCategory.ELECTRONICS]
            ),
        ),
        location=LocationInfo(
            latitude=round(lat, 4),
            longitude=round(lon, 4),
            city=base_loc["city"],
            country=base_loc["country"],
        ),
        device=DeviceInfo(
            device_id=profile["primary_device_id"],
            ip_address=fake.ipv4(),
            channel=profile["primary_channel"],
        ),
        simulation_metadata=SimulationMetadata(
            is_simulated_fraud=False,
            anomaly_type=AnomalyType.NONE,
        ),
    )


def generate_impossible_travel_anomaly(account_id: str) -> TransactionEvent:
    """
    Genera una transacción a miles de km de distancia con apenas segundos de diferencia.
    """
    profile = get_or_create_account_profile(account_id)
    now = datetime.now(timezone.utc)

    # Elegir una ubicación drásticamente diferente
    distant_locations = [loc for loc in REFERENCE_LOCATIONS if loc["city"] != profile["last_location"]["city"]]
    target_loc = random.choice(distant_locations)

    lat = target_loc["lat"] + random.uniform(-0.02, 0.02)
    lon = target_loc["lon"] + random.uniform(-0.02, 0.02)

    profile["last_location"] = {"city": target_loc["city"], "country": target_loc["country"], "lat": lat, "lon": lon}
    profile["last_timestamp"] = now

    return TransactionEvent(
        transaction_id=str(uuid.uuid4()),
        account_id=account_id,
        timestamp=now.isoformat(),
        amount=round(random.uniform(400.0, 1500.0), 2),
        currency=profile["currency"],
        transaction_type=TransactionType.ATM_WITHDRAWAL,
        merchant=MerchantInfo(
            merchant_id=f"MER-{random.randint(800, 999)}",
            name=f"{fake.company()} Global ATM",
            category=MerchantCategory.CASH_ADVANCE,
        ),
        location=LocationInfo(
            latitude=round(lat, 4),
            longitude=round(lon, 4),
            city=target_loc["city"],
            country=target_loc["country"],
        ),
        device=DeviceInfo(
            device_id=f"DEV-{fake.md5()[:10].upper()}",  # Dispositivo desconocido
            ip_address=fake.ipv4(),
            channel=Channel.ATM,
        ),
        simulation_metadata=SimulationMetadata(
            is_simulated_fraud=True,
            anomaly_type=AnomalyType.IMPOSSIBLE_TRAVEL,
        ),
    )


def generate_velocity_burst(account_id: str, count: int = 5) -> list[TransactionEvent]:
    """
    Genera un grupo de transacciones casi instantáneas (Velocity Attack).
    """
    profile = get_or_create_account_profile(account_id)
    events = []
    base_time = datetime.now(timezone.utc)

    for i in range(count):
        lat = profile["home_location"]["lat"] + random.uniform(-0.01, 0.01)
        lon = profile["home_location"]["lon"] + random.uniform(-0.01, 0.01)

        event = TransactionEvent(
            transaction_id=str(uuid.uuid4()),
            account_id=account_id,
            timestamp=base_time.isoformat(),
            amount=round(random.uniform(200.0, 800.0), 2),
            currency=profile["currency"],
            transaction_type=TransactionType.ONLINE_PAYMENT,
            merchant=MerchantInfo(
                merchant_id=f"MER-{random.randint(500, 700)}",
                name=f"QuickPay-{fake.company()}",
                category=MerchantCategory.LUXURY_GOODS,
            ),
            location=LocationInfo(
                latitude=round(lat, 4),
                longitude=round(lon, 4),
                city=profile["home_location"]["city"],
                country=profile["home_location"]["country"],
            ),
            device=DeviceInfo(
                device_id=profile["primary_device_id"],
                ip_address=fake.ipv4(),
                channel=Channel.WEB_BROWSER,
            ),
            simulation_metadata=SimulationMetadata(
                is_simulated_fraud=True,
                anomaly_type=AnomalyType.VELOCITY_ATTACK,
            ),
        )
        events.append(event)

    return events
