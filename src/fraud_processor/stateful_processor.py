
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

            # Retrieve previous transactions in the 60-minute window
            cutoff_time = (
                transaction.timestamp - self.history_window
            )

            recent_transactions = [
                item
                for item in state["transactions"]
                if cutoff_time <= item.timestamp < transaction.timestamp
            ]

            # Evaluate fraud rules
            fraud_result = evaluate_transaction(
                transaction=transaction,
                recent_transactions=recent_transactions,
            )

            # Save result using create-only storage operation.
            # If it already exists, this may be a replay after a crash.
            self.durable_store.save_fraud_result(
                fraud_result
            )

            # Build a candidate state without modifying the cached state
            updated_transactions = (
                state["transactions"] + [transaction]
            )

            updated_transactions.sort(
                key=lambda item: item.timestamp
            )

            # Retain the latest 60 minutes of event-time history
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

            # Persist state before updating the in-memory cache
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
