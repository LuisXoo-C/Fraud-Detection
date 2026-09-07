import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TRANSACTIONS = os.getenv("TOPIC_TRANSACTIONS", "financial-transactions")

# Cantidad de cuentas base para simular comportamiento repetitivo
TOTAL_ACCOUNTS = int(os.getenv("TOTAL_ACCOUNTS", "100"))

# Parámetros del simulador
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "10.0"))
FRAUD_RATIO = float(os.getenv("FRAUD_RATIO", "0.15")) # 15% de eventos anómalos

# Ciudades predefinidas para simular ubicaciones consistentes y saltos imposibles
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
