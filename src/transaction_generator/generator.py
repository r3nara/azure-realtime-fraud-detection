
import random
import uuid

from datetime import datetime, timezone

from src.common.transaction_schema import (
    Transaction,
    TransactionType,
    PaymentMethod,
    TransactionStatus
)


CUSTOMERS = [
    f"CUST{i:04d}" for i in range(1, 101)
]

MERCHANTS = [
    "Amazon",
    "Flipkart",
    "Reliance Digital",
    "BigBasket",
    "Swiggy",
    "Zomato",
    "Myntra",
    "Uber"
]

LOCATIONS = [
    "Chennai",
    "Bangalore",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Pune"
]

DEVICES = [
    f"DEV{i:04d}" for i in range(1, 51)
]


def generate_transaction(rng=None):

    rng = rng or random.Random()

    # 5% probability of generating a suspicious transaction
    is_suspicious = rng.random() < 0.05

    if is_suspicious:
        amount = round(
            rng.uniform(50000, 200000), 2
        )
    else:
        amount = round(
            rng.uniform(100, 15000), 2
        )

    transaction = Transaction(
        transaction_id=f"TXN-{uuid.uuid4()}",
        customer_id=rng.choice(CUSTOMERS),
        amount=amount,
        currency="INR",
        merchant=rng.choice(MERCHANTS),
        transaction_type=rng.choice(
            list(TransactionType)
        ),
        location=rng.choice(LOCATIONS),
        timestamp=datetime.now(timezone.utc),
        payment_method=rng.choice(
            list(PaymentMethod)
        ),
        device_id=rng.choice(DEVICES),
        status=TransactionStatus.SUCCESS
    )

    return transaction


def generate_batch(count=100, seed=None):

    if count < 1:
        raise ValueError("Count must be greater than zero")

    rng = random.Random(seed)

    transactions = [
        generate_transaction(rng)
        for _ in range(count)
    ]

    return transactions
