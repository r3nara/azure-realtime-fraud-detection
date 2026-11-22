
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field, ConfigDict


class TransactionType(str, Enum):
    ONLINE = "ONLINE"
    POS = "POS"
    ATM = "ATM"


class PaymentMethod(str, Enum):
    CREDIT_CARD = "CREDIT_CARD"
    DEBIT_CARD = "DEBIT_CARD"
    UPI = "UPI"


class TransactionStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class Transaction(BaseModel):

    model_config = ConfigDict(
        use_enum_values=True,
        extra="forbid"
    )

    transaction_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)

    amount: float = Field(gt=0)

    currency: str = Field(default="INR")

    merchant: str = Field(min_length=1)

    transaction_type: TransactionType

    location: str = Field(min_length=1)

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    payment_method: PaymentMethod

    device_id: str = Field(min_length=1)

    status: TransactionStatus
