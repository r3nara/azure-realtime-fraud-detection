
import hashlib
import json

from datetime import datetime, timezone

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from src.common.transaction_schema import Transaction


class DurableFraudStore:
    """
    Stores fraud results and customer transaction state in Azure Blob Storage.

    Uses the existing ADLS Gen2 storage account.
    """

    def __init__(
        self,
        blob_service_client: BlobServiceClient,
        container_name: str = "fraud-data",
        history_window_minutes: int = 60,
    ):
        self.container = (
            blob_service_client.get_container_client(container_name)
        )

        self.history_window_minutes = history_window_minutes

    @staticmethod
    def _safe_key(value: str) -> str:
        """Create a deterministic, storage-safe key."""
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _customer_state_blob(self, customer_id: str):
        key = self._safe_key(customer_id)

        return self.container.get_blob_client(
            f"processed/customer-state/{key}.json"
        )

    def _fraud_result_blob(self, transaction_id: str):
        key = self._safe_key(transaction_id)

        return self.container.get_blob_client(
            f"processed/fraud-results/{key}.json"
        )

    def load_customer_state(self, customer_id: str) -> dict:
        """
        Load a customer's saved transactions and processed IDs.

        Returns an empty state if no snapshot exists.
        """
        blob = self._customer_state_blob(customer_id)

        try:
            content = blob.download_blob().readall()
            state = json.loads(content)

            return {
                "customer_id": customer_id,
                "transactions": [
                    Transaction.model_validate(item)
                    for item in state.get("transactions", [])
                ],
                "processed_transaction_ids": set(
                    state.get("processed_transaction_ids", [])
                ),
            }

        except ResourceNotFoundError:
            return {
                "customer_id": customer_id,
                "transactions": [],
                "processed_transaction_ids": set(),
            }

    def save_customer_state(
        self,
        customer_id: str,
        transactions: list[Transaction],
        processed_transaction_ids: set[str],
    ):
        """Save the customer's current rolling history."""
        state = {
            "customer_id": customer_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "transactions": [
                item.model_dump(mode="json")
                for item in transactions
            ],
            "processed_transaction_ids": sorted(
                processed_transaction_ids
            ),
        }

        blob = self._customer_state_blob(customer_id)

        blob.upload_blob(
            json.dumps(state, indent=2),
            overwrite=True,
        )

    def save_fraud_result(self, fraud_result: dict) -> bool:
        """
        Save a fraud result only if it does not already exist.

        Returns:
            True  - new result created
            False - result already exists
        """
        transaction_id = fraud_result["transaction_id"]
        blob = self._fraud_result_blob(transaction_id)

        record = {
            **fraud_result,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            blob.upload_blob(
                json.dumps(record, indent=2),
                overwrite=False,
            )
            return True

        except ResourceExistsError:
            return False

    def load_fraud_result(self, transaction_id: str) -> dict | None:
        """
        Retrieve an existing fraud result.

        Returns:
            The saved fraud result if it exists.
            None if no result has been saved yet.
        """
        blob = self._fraud_result_blob(transaction_id)

        try:
            content = blob.download_blob().readall()
            return json.loads(content)

        except ResourceNotFoundError:
            return None

    def fraud_result_exists(self, transaction_id: str) -> bool:
        """Check whether a transaction already has a saved result."""
        blob = self._fraud_result_blob(transaction_id)

        return blob.exists()
