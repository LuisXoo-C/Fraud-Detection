from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class Currency(str, Enum):
    USD = "USD"
    EUR = "EUR"
    MXN = "MXN"
    GBP = "GBP"
    CAD = "CAD"


class TransactionType(str, Enum):
    PURCHASE = "PURCHASE"
    ATM_WITHDRAWAL = "ATM_WITHDRAWAL"
    TRANSFER_OUT = "TRANSFER_OUT"
    ONLINE_PAYMENT = "ONLINE_PAYMENT"


class MerchantCategory(str, Enum):
    GROCERY = "GROCERY"
    ELECTRONICS = "ELECTRONICS"
    LUXURY_GOODS = "LUXURY_GOODS"
    TRAVEL = "TRAVEL"
    GAMBLING = "GAMBLING"
    RESTAURANT = "RESTAURANT"
    CASH_ADVANCE = "CASH_ADVANCE"


class Channel(str, Enum):
    MOBILE_APP = "MOBILE_APP"
    WEB_BROWSER = "WEB_BROWSER"
    POS_TERMINAL = "POS_TERMINAL"
    ATM = "ATM"


class AnomalyType(str, Enum):
    NONE = "NONE"
    IMPOSSIBLE_TRAVEL = "IMPOSSIBLE_TRAVEL"
    VELOCITY_ATTACK = "VELOCITY_ATTACK"
    HIGH_AMOUNT_BURST = "HIGH_AMOUNT_BURST"


class MerchantInfo(BaseModel):
    merchant_id: str
    name: str
    category: MerchantCategory


class LocationInfo(BaseModel):
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)
    city: str
    country: str = Field(min_length=2, max_length=2)


class DeviceInfo(BaseModel):
    device_id: str
    ip_address: str
    channel: Channel


class SimulationMetadata(BaseModel):
    is_simulated_fraud: bool = False
    anomaly_type: AnomalyType = AnomalyType.NONE


class TransactionEvent(BaseModel):
    transaction_id: str
    account_id: str
    timestamp: str  # ISO-8601 UTC
    amount: float = Field(gt=0.0)
    currency: Currency
    transaction_type: TransactionType
    merchant: MerchantInfo
    location: LocationInfo
    device: DeviceInfo
    simulation_metadata: Optional[SimulationMetadata] = None
