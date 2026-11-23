
from datetime import datetime, timedelta, timezone

from src.common.transaction_schema import (
    Transaction,
    TransactionType,
    PaymentMethod,
    TransactionStatus,
)
from src.fraud_processor.customer_state import CustomerStateManager
from src.fraud_processor.fraud_rules import evaluate_transaction


def create_transaction(
    transaction_id,
    timestamp,
    amount,
    location,
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


def process_transaction(transaction, state_manager):
    """
    Simulate the fraud processor's transaction processing order.
    """

    # 1. Retrieve previous customer history
    recent_transactions = state_manager.get_recent_transactions(
        customer_id=transaction.customer_id,
        current_timestamp=transaction.timestamp,
        window_minutes=60,
    )

    # 2. Evaluate fraud rules
    fraud_result = evaluate_transaction(
        transaction=transaction,
        recent_transactions=recent_transactions,
    )

    # 3. Update customer history
    added = state_manager.add_transaction(transaction)

    assert added is True

    return fraud_result


def test_complete_fraud_detection_flow():

    state_manager = CustomerStateManager(
        history_window_minutes=60
    )

    start_time = datetime.now(timezone.utc)

    # Transaction 1: Normal transaction
    t1 = create_transaction(
        "T001",
        start_time,
        2000,
        "Chennai",
    )

    result1 = process_transaction(t1, state_manager)

    assert result1["risk_score"] == 0
    assert result1["risk_level"] == "LOW"

    # Transaction 2: Another normal transaction
    t2 = create_transaction(
        "T002",
        start_time + timedelta(minutes=2),
        3000,
        "Chennai",
    )

    result2 = process_transaction(t2, state_manager)

    assert result2["risk_score"] == 0
    assert result2["risk_level"] == "LOW"

    # Transaction 3: High-value transaction from another location
    t3 = create_transaction(
        "T003",
        start_time + timedelta(minutes=5),
        75000,
        "Mumbai",
    )

    result3 = process_transaction(t3, state_manager)

    assert result3["risk_score"] == 60
    assert result3["risk_level"] == "HIGH"

    assert "F01_HIGH_VALUE" in result3["triggered_rules"]
    assert "F03_LOCATION_CHANGE" in result3["triggered_rules"]

    # Transaction 4: Fourth transaction within 10 minutes
    t4 = create_transaction(
        "T004",
        start_time + timedelta(minutes=7),
        1000,
        "Mumbai",
    )

    result4 = process_transaction(t4, state_manager)

    assert result4["risk_score"] == 0
    assert result4["risk_level"] == "LOW"

    # Verify all four transactions are retained
    history = state_manager.get_recent_transactions(
        customer_id="C101",
        current_timestamp=start_time + timedelta(minutes=8),
        window_minutes=60,
    )

    assert len(history) == 4
