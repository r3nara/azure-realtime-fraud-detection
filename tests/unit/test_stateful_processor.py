
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.common.transaction_schema import (
    Transaction,
    TransactionType,
    PaymentMethod,
    TransactionStatus,
)
from src.fraud_processor.stateful_processor import (
    StatefulFraudProcessor,
)


def create_transaction(
    transaction_id="T001",
    customer_id="C101",
    amount=2000,
):
    return Transaction(
        transaction_id=transaction_id,
        customer_id=customer_id,
        amount=amount,
        currency="INR",
        merchant="Test Merchant",
        transaction_type=TransactionType.ONLINE,
        location="Chennai",
        timestamp=datetime.now(timezone.utc),
        payment_method=PaymentMethod.UPI,
        device_id="DEVICE001",
        status=TransactionStatus.SUCCESS,
    )


@pytest.fixture
def durable_store():
    store = MagicMock()

    store.load_customer_state.return_value = {
        "customer_id": "C101",
        "transactions": [],
        "processed_transaction_ids": set(),
    }

    store.load_fraud_result.return_value = None
    store.save_fraud_result.return_value = True

    return store


def test_new_transaction_is_processed_and_saved(durable_store):
    processor = StatefulFraudProcessor(durable_store)

    transaction = create_transaction()

    result = processor.process_transaction(transaction)

    assert result["status"] == "PROCESSED"
    assert result["risk_score"] == 0
    assert result["risk_level"] == "LOW"

    durable_store.load_fraud_result.assert_called_once_with("T001")
    durable_store.save_fraud_result.assert_called_once()
    durable_store.save_customer_state.assert_called_once()


def test_duplicate_transaction_is_rejected(durable_store):
    processor = StatefulFraudProcessor(durable_store)

    transaction = create_transaction()

    processor.process_transaction(transaction)
    result = processor.process_transaction(transaction)

    assert result["status"] == "DUPLICATE"

    assert durable_store.save_fraud_result.call_count == 1
    assert durable_store.save_customer_state.call_count == 1


def test_customer_state_is_restored_on_new_processor(durable_store):
    transaction = create_transaction()

    saved_state = {
        "customer_id": "C101",
        "transactions": [transaction],
        "processed_transaction_ids": {"T001"},
    }

    durable_store.load_customer_state.return_value = saved_state

    processor = StatefulFraudProcessor(durable_store)

    result = processor.process_transaction(transaction)

    assert result["status"] == "DUPLICATE"

    durable_store.load_fraud_result.assert_not_called()
    durable_store.save_fraud_result.assert_not_called()
    durable_store.save_customer_state.assert_not_called()


def test_recovery_after_fraud_result_saved_but_state_save_failed(
    durable_store,
):
    transaction = create_transaction()

    original_fraud_result = {
        "transaction_id": "T001",
        "customer_id": "C101",
        "risk_score": 30,
        "risk_level": "MEDIUM",
        "triggered_rules": ["F01_HIGH_VALUE"],
    }

    # First attempt: no saved result exists.
    # The fraud result is saved, but customer-state saving fails.
    durable_store.load_fraud_result.side_effect = [
        None,
        original_fraud_result,
    ]

    durable_store.save_customer_state.side_effect = RuntimeError(
        "Simulated storage failure"
    )

    processor1 = StatefulFraudProcessor(durable_store)

    with pytest.raises(RuntimeError):
        processor1.process_transaction(transaction)

    durable_store.save_fraud_result.assert_called_once()

    # Simulate application restart.
    # Customer state was not saved, but the fraud result exists.
    durable_store.load_customer_state.return_value = {
        "customer_id": "C101",
        "transactions": [],
        "processed_transaction_ids": set(),
    }

    durable_store.save_customer_state.side_effect = None

    processor2 = StatefulFraudProcessor(durable_store)

    result = processor2.process_transaction(transaction)

    # The original saved result must be reused.
    assert result["risk_score"] == 30
    assert result["risk_level"] == "MEDIUM"
    assert result["triggered_rules"] == ["F01_HIGH_VALUE"]
    assert result["status"] == "PROCESSED"

    # The fraud result must not be created a second time.
    assert durable_store.save_fraud_result.call_count == 1

    # Customer state is retried and successfully saved.
    assert durable_store.save_customer_state.call_count == 2

    final_saved_state = (
        durable_store.save_customer_state.call_args.kwargs
    )

    assert final_saved_state["customer_id"] == "C101"
    assert len(final_saved_state["transactions"]) == 1
    assert final_saved_state["processed_transaction_ids"] == {"T001"}


def test_recovery_when_another_attempt_creates_result_first(
    durable_store,
):
    transaction = create_transaction()

    original_fraud_result = {
        "transaction_id": "T001",
        "customer_id": "C101",
        "risk_score": 30,
        "risk_level": "MEDIUM",
        "triggered_rules": ["F01_HIGH_VALUE"],
    }

    # Initial read finds nothing. The create-only save then reports
    # that another attempt has already created the result.
    durable_store.load_fraud_result.side_effect = [
        None,
        original_fraud_result,
    ]
    durable_store.save_fraud_result.return_value = False

    processor = StatefulFraudProcessor(durable_store)

    result = processor.process_transaction(transaction)

    assert result["risk_score"] == 30
    assert result["risk_level"] == "MEDIUM"
    assert result["status"] == "PROCESSED"

    # It retrieves the canonical result after the create conflict.
    assert durable_store.load_fraud_result.call_count == 2

    final_saved_state = (
        durable_store.save_customer_state.call_args.kwargs
    )

    assert final_saved_state["processed_transaction_ids"] == {"T001"}
    assert len(final_saved_state["transactions"]) == 1
