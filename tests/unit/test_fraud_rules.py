
from datetime import datetime, timedelta, timezone

from src.common.transaction_schema import (
    Transaction,
    TransactionType,
    PaymentMethod,
    TransactionStatus,
)
from src.fraud_processor.fraud_rules import evaluate_transaction


def create_transaction(
    transaction_id,
    timestamp,
    amount=1000,
    location="Chennai",
    status=TransactionStatus.SUCCESS,
):
    return Transaction(
        transaction_id=transaction_id,
        customer_id="C101",
        amount=amount,
        currency="INR",
        merchant="Test Merchant",
        transaction_type=TransactionType.ONLINE,
        location=location,
        timestamp=timestamp,
        payment_method=PaymentMethod.UPI,
        device_id="DEVICE001",
        status=status,
    )


def test_high_value_rule():
    now = datetime.now(timezone.utc)
    transaction = create_transaction("T001", now, amount=60000)

    result = evaluate_transaction(transaction, [])

    assert result["risk_score"] == 30
    assert result["risk_level"] == "MEDIUM"
    assert "F01_HIGH_VALUE" in result["triggered_rules"]


def test_high_velocity_rule():
    now = datetime.now(timezone.utc)

    history = [
        create_transaction(
            f"T{i}",
            now - timedelta(minutes=i),
        )
        for i in range(1, 6)
    ]

    current = create_transaction("T006", now)

    result = evaluate_transaction(current, history)

    assert result["risk_score"] == 25
    assert result["risk_level"] == "LOW"
    assert "F02_HIGH_VELOCITY" in result["triggered_rules"]


def test_location_change_rule():
    now = datetime.now(timezone.utc)

    history = [
        create_transaction(
            "T001",
            now - timedelta(minutes=10),
            location="Chennai",
        )
    ]

    current = create_transaction(
        "T002",
        now,
        location="Mumbai",
    )

    result = evaluate_transaction(current, history)

    assert result["risk_score"] == 30
    assert "F03_LOCATION_CHANGE" in result["triggered_rules"]


def test_repeated_failures_rule():
    now = datetime.now(timezone.utc)

    history = [
        create_transaction(
            f"T{i}",
            now - timedelta(minutes=i),
            status=TransactionStatus.FAILED,
        )
        for i in range(1, 3)
    ]

    current = create_transaction(
        "T003",
        now,
        status=TransactionStatus.FAILED,
    )

    result = evaluate_transaction(current, history)

    assert result["risk_score"] == 15
    assert "F04_REPEATED_FAILURES" in result["triggered_rules"]


def test_multiple_rules_are_combined():
    now = datetime.now(timezone.utc)

    history = [
        create_transaction(
            f"T{i}",
            now - timedelta(minutes=i),
            location="Chennai",
        )
        for i in range(1, 6)
    ]

    current = create_transaction(
        "T006",
        now,
        amount=75000,
        location="Mumbai",
    )

    result = evaluate_transaction(current, history)

    assert result["risk_score"] == 85
    assert result["risk_level"] == "HIGH"
    assert len(result["triggered_rules"]) == 3


def test_normal_transaction_has_low_risk():
    now = datetime.now(timezone.utc)

    current = create_transaction("T001", now)

    result = evaluate_transaction(current, [])

    assert result["risk_score"] == 0
    assert result["risk_level"] == "LOW"
    assert result["triggered_rules"] == []
