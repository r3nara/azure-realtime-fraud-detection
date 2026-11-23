
import json
from unittest.mock import MagicMock

import pytest

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from src.fraud_processor.durable_store import DurableFraudStore


@pytest.fixture
def store():
    blob_service = MagicMock()
    return DurableFraudStore(blob_service)


def test_load_missing_customer_state_returns_empty_state(store):
    blob = store._customer_state_blob("C101")
    blob.download_blob.side_effect = ResourceNotFoundError("Not found")

    state = store.load_customer_state("C101")

    assert state["customer_id"] == "C101"
    assert state["transactions"] == []
    assert state["processed_transaction_ids"] == set()


def test_load_customer_state_restores_saved_data(store):
    saved_state = {
        "customer_id": "C101",
        "transactions": [],
        "processed_transaction_ids": ["T001", "T002"],
    }

    blob = store._customer_state_blob("C101")
    download = MagicMock()
    download.readall.return_value = json.dumps(saved_state).encode("utf-8")
    blob.download_blob.return_value = download

    state = store.load_customer_state("C101")

    assert state["customer_id"] == "C101"
    assert state["processed_transaction_ids"] == {"T001", "T002"}


def test_save_fraud_result_creates_new_record(store):
    result = {
        "transaction_id": "T001",
        "customer_id": "C101",
        "risk_score": 30,
        "risk_level": "MEDIUM",
        "triggered_rules": ["F01_HIGH_VALUE"],
    }

    blob = store._fraud_result_blob("T001")

    created = store.save_fraud_result(result)

    assert created is True
    blob.upload_blob.assert_called_once()

    args, kwargs = blob.upload_blob.call_args
    assert kwargs["overwrite"] is False

    saved_record = json.loads(args[0])
    assert saved_record["transaction_id"] == "T001"


def test_save_existing_fraud_result_returns_false(store):
    result = {
        "transaction_id": "T001",
        "customer_id": "C101",
        "risk_score": 30,
        "risk_level": "MEDIUM",
        "triggered_rules": ["F01_HIGH_VALUE"],
    }

    blob = store._fraud_result_blob("T001")
    blob.upload_blob.side_effect = ResourceExistsError("Already exists")

    created = store.save_fraud_result(result)

    assert created is False


def test_fraud_result_exists(store):
    blob = store._fraud_result_blob("T001")
    blob.exists.return_value = True

    assert store.fraud_result_exists("T001") is True
