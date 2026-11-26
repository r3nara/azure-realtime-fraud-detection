
from datetime import timedelta
from threading import RLock

from src.common.transaction_schema import Transaction
from src.fraud_processor.fraud_rules import evaluate_transaction
from src.fraud_processor.durable_store import DurableFraudStore


class StatefulFraudProcessor:
    """
    Coordinates fraud evaluation, durable results,
    customer history and duplicate detection.

    Initial MVP assumption:
    One active consumer process manages customer state.
    """

    def __init__(
        self,
        durable_store: DurableFraudStore,
        history_window_minutes: int = 60,
    ):
        self.durable_store = durable_store
        self.history_window = timedelta(
            minutes=history_window_minutes
        )

        # Cache loaded customer state during this process
        self.customer_states = {}

        # Serialize state changes within this process
        self.lock = RLock()

    def _get_customer_state(self, customer_id: str) -> dict:
        if customer_id not in self.customer_states:
            self.customer_states[customer_id] = (
                self.durable_store.load_customer_state(
                    customer_id
                )
            )

        return self.customer_states[customer_id]

    def _get_or_create_fraud_result(
        self,
        transaction: Transaction,
        recent_transactions: list[Transaction],
    ) -> dict:
        """
        Reuse an existing result or calculate and save a new one.

        Handles the case where another processing attempt creates
        the result between the initial read and the create operation.
        """
        transaction_id = transaction.transaction_id

        existing_result = self.durable_store.load_fraud_result(
            transaction_id
        )

        if existing_result is not None:
            return existing_result

        fraud_result = evaluate_transaction(
            transaction=transaction,
            recent_transactions=recent_transactions,
        )

        created = self.durable_store.save_fraud_result(
            fraud_result
        )

        if created:
            return fraud_result

        # Another attempt may have created the result first.
        # Retrieve and reuse that canonical saved result.
        existing_result = self.durable_store.load_fraud_result(
            transaction_id
        )

        if existing_result is None:
            raise RuntimeError(
                "Fraud result creation reported an existing record, "
                f"but it could not be retrieved: {transaction_id}"
            )

        return existing_result

    def process_transaction(
        self,
        transaction: Transaction,
    ) -> dict:

        with self.lock:
            state = self._get_customer_state(
                transaction.customer_id
            )

            transaction_id = transaction.transaction_id

            # Check durable customer state for duplicates
            if transaction_id in state["processed_transaction_ids"]:
                return {
                    "transaction_id": transaction_id,
                    "customer_id": transaction.customer_id,
                    "status": "DUPLICATE",
                    "risk_score": None,
                    "risk_level": None,
                    "triggered_rules": [],
                }

            # Retrieve previous transactions in the history window
            cutoff_time = (
                transaction.timestamp - self.history_window
            )

            recent_transactions = [
                item
                for item in state["transactions"]
                if cutoff_time <= item.timestamp < transaction.timestamp
            ]

            # Reuse an existing fraud result during recovery.
            # Evaluate and save only if no result exists.
            fraud_result = self._get_or_create_fraud_result(
                transaction=transaction,
                recent_transactions=recent_transactions,
            )

            # Build a candidate state without modifying cached state
            updated_transactions = (
                state["transactions"] + [transaction]
            )

            updated_transactions.sort(
                key=lambda item: item.timestamp
            )

            # Retain the latest history window based on event time
            latest_timestamp = max(
                item.timestamp for item in updated_transactions
            )

            retention_cutoff = (
                latest_timestamp - self.history_window
            )

            updated_transactions = [
                item
                for item in updated_transactions
                if item.timestamp >= retention_cutoff
            ]

            updated_processed_ids = (
                state["processed_transaction_ids"] | {transaction_id}
            )

            # Persist customer state before updating the cache.
            # If this fails, the next retry can recover using the
            # already-saved fraud result.
            self.durable_store.save_customer_state(
                customer_id=transaction.customer_id,
                transactions=updated_transactions,
                processed_transaction_ids=updated_processed_ids,
            )

            # Update cache only after successful durable save
            state["transactions"] = updated_transactions
            state["processed_transaction_ids"] = updated_processed_ids

            return {
                **fraud_result,
                "status": "PROCESSED",
            }
