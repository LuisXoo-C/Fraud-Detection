import math


def calculate_haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calcula la distancia en kilometros entre dos puntos geograficos sobre la Tierra.
    """
    R = 6371.0  # Radio terrestre en km

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    return R * c


def evaluate_impossible_travel(
    prev_lat: float,
    prev_lon: float,
    prev_time_epoch: float,
    curr_lat: float,
    curr_lon: float,
    curr_time_epoch: float,
    max_speed_kmh: float = 800.0,
    min_distance_threshold_km: float = 150.0,
) -> tuple[bool, float, float]:
    """
    Determina si la velocidad de desplazamiento requerida entre transacciones supera el limite fisico.
    Retorna: (es_fraude, distancia_km, velocidad_kmh)
    """
    if prev_time_epoch is None or prev_lat is None or prev_lon is None:
        return False, 0.0, 0.0

    delta_seconds = abs(curr_time_epoch - prev_time_epoch)
    if delta_seconds <= 0:
        delta_seconds = 1  # Evitar division entre cero en transacciones simultaneas

    hours = delta_seconds / 3600.0
    distance_km = calculate_haversine_distance(prev_lat, prev_lon, curr_lat, curr_lon)
    speed_kmh = distance_km / hours

    # Se considera imposible si supera 800 km/h y ademas la distancia es significativa (>150 km)
    is_impossible = speed_kmh > max_speed_kmh and distance_km > min_distance_threshold_km
    return is_impossible, distance_km, speed_kmh


def compute_risk_score(
    is_impossible_travel: bool,
    velocity_count_10m: int,
    amount: float,
) -> tuple[int, list[str]]:
    """
    Calcula el Score de Riesgo (0-100) y las razones asociadas.
    Reglas:
    - Impossible Travel: +60 puntos
    - Velocity Attack (>3 transacciones en 10 min): +40 puntos
    - Monto Elevado (> $800): +20 puntos
    """
    score = 0
    reasons = []

    if is_impossible_travel:
        score += 60
        reasons.append("IMPOSSIBLE_TRAVEL_DETECTED")

    if velocity_count_10m > 3:
        score += 40
        reasons.append(f"HIGH_VELOCITY_BURST({velocity_count_10m}_tx_in_10m)")

    if amount >= 800.0:
        score += 20
        reasons.append("HIGH_TRANSACTION_AMOUNT")

    final_score = min(score, 100)
    return final_score, reasons
