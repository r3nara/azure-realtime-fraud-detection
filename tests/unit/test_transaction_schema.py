
import pytest
from pydantic import ValidationError

from src.common.transaction_schema import (
    Transaction,
    TransactionType,
    PaymentMethod,
    TransactionStatus
)


def create_valid_transaction():
    return Transaction(
        transaction_id="TXN10001",
        customer_id="CUST1001",
        amount=4500.00,
        currency="INR",
        merchant="Amazon",
        transaction_type=TransactionType.ONLINE,
        location="Chennai",
        payment_method=PaymentMethod.CREDIT_CARD,
        device_id="DEV1001",
        status=TransactionStatus.SUCCESS
    )


def test_valid_transaction():
    transaction = create_valid_transaction()

    assert transaction.transaction_id == "TXN10001"
    assert transaction.amount == 4500.00


def test_negative_amount_rejected():
    with pytest.raises(ValidationError):
        Transaction(
            transaction_id="TXN10002",
            customer_id="CUST1001",
            amount=-100,
            currency="INR",
            merchant="Amazon",
            transaction_type="ONLINE",
            location="Chennai",
            payment_method="UPI",
            device_id="DEV1001",
            status="SUCCESS"
        )


def test_invalid_transaction_type_rejected():
    with pytest.raises(ValidationError):
        Transaction(
            transaction_id="TXN10003",
            customer_id="CUST1001",
            amount=100,
            currency="INR",
            merchant="Amazon",
            transaction_type="INVALID",
            location="Chennai",
            payment_method="UPI",
            device_id="DEV1001",
            status="SUCCESS"
        )
