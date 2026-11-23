
from datetime import datetime, timedelta, timezone

from src.common.transaction_schema import (
    Transaction,
    TransactionType,
    PaymentMethod,
    TransactionStatus,
)
from src.fraud_processor.customer_state import CustomerStateManager


def create_transaction(
    transaction_id,
    customer_id,
    timestamp,
    amount=1000,
):
    return Transaction(
        transaction_id=transaction_id,
        customer_id=customer_id,
        amount=amount,
        currency="INR",
        merchant="Test Merchant",
        transaction_type=TransactionType.ONLINE,
        location="Chennai",
        timestamp=timestamp,
        payment_method=PaymentMethod.UPI,
        device_id="DEVICE001",
        status=TransactionStatus.SUCCESS,
    )


def test_transactions_are_separated_by_customer():
    manager = CustomerStateManager()

    now = datetime.now(timezone.utc)

    t1 = create_transaction("T001", "C101", now)
    t2 = create_transaction("T002", "C102", now)

    manager.add_transaction(t1)
    manager.add_transaction(t2)

    assert len(manager.get_recent_transactions("C101", now + timedelta(seconds=1))) == 1
    assert len(manager.get_recent_transactions("C102", now + timedelta(seconds=1))) == 1


def test_recent_transactions_are_returned():
    manager = CustomerStateManager()

    now = datetime.now(timezone.utc)

    t1 = create_transaction("T001", "C101", now - timedelta(minutes=5))
    t2 = create_transaction("T002", "C101", now - timedelta(minutes=15))

    manager.add_transaction(t1)
    manager.add_transaction(t2)

    recent = manager.get_recent_transactions(
        "C101",
        now,
        window_minutes=10,
    )

    assert len(recent) == 1
    assert recent[0].transaction_id == "T001"


def test_duplicate_transaction_is_rejected():
    manager = CustomerStateManager()

    now = datetime.now(timezone.utc)

    t1 = create_transaction("T001", "C101", now)
    t2 = create_transaction("T001", "C101", now)

    assert manager.add_transaction(t1) is True
    assert manager.add_transaction(t2) is False


def test_old_transactions_are_removed():
    manager = CustomerStateManager(history_window_minutes=60)

    now = datetime.now(timezone.utc)

    old_transaction = create_transaction(
        "T001",
        "C101",
        now - timedelta(minutes=90),
    )

    new_transaction = create_transaction(
        "T002",
        "C101",
        now,
    )

    manager.add_transaction(old_transaction)
    manager.add_transaction(new_transaction)

    history = manager.get_recent_transactions(
        "C101",
        now + timedelta(seconds=1),
    )

    assert len(history) == 1
    assert history[0].transaction_id == "T002"
