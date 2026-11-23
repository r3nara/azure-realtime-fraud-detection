
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

    store.save_fraud_result.return_value = True

    return store


def test_new_transaction_is_processed_and_saved(durable_store):

    processor = StatefulFraudProcessor(durable_store)

    transaction = create_transaction()

    result = processor.process_transaction(transaction)

    assert result["status"] == "PROCESSED"
    assert result["risk_score"] == 0
    assert result["risk_level"] == "LOW"

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

    # Simulate customer state already saved in durable storage
    durable_store.load_customer_state.return_value = saved_state

    # Simulate application restart
    processor = StatefulFraudProcessor(durable_store)

    result = processor.process_transaction(transaction)

    assert result["status"] == "DUPLICATE"

    durable_store.save_fraud_result.assert_not_called()
    durable_store.save_customer_state.assert_not_called()


def test_recovery_after_fraud_result_saved_but_state_save_failed(
    durable_store,
):

    transaction = create_transaction()

    # First attempt: fraud result succeeds, customer-state save fails
    durable_store.save_customer_state.side_effect = RuntimeError(
        "Simulated storage failure"
    )

    processor1 = StatefulFraudProcessor(durable_store)

    with pytest.raises(RuntimeError):
        processor1.process_transaction(transaction)

    durable_store.save_fraud_result.assert_called_once()

    # Simulate restart: customer state was not saved
    durable_store.load_customer_state.return_value = {
        "customer_id": "C101",
        "transactions": [],
        "processed_transaction_ids": set(),
    }

    # Fraud result already exists in durable storage
    durable_store.save_fraud_result.return_value = False

    # Allow customer-state save to succeed after restart
    durable_store.save_customer_state.side_effect = None

    processor2 = StatefulFraudProcessor(durable_store)

    result = processor2.process_transaction(transaction)

    assert result["status"] == "PROCESSED"

    # Fraud-result creation was attempted on both processing attempts
    assert durable_store.save_fraud_result.call_count == 2

    # Customer-state save was attempted twice:
    # first attempt failed, second attempt succeeded
    assert durable_store.save_customer_state.call_count == 2

    # Verify the final successful state-save call
    final_saved_state = (
        durable_store.save_customer_state.call_args.kwargs
    )

    assert final_saved_state["customer_id"] == "C101"
    assert len(final_saved_state["transactions"]) == 1
    assert final_saved_state["processed_transaction_ids"] == {"T001"}
