
from collections import defaultdict
from datetime import timedelta
from threading import Lock

from src.common.transaction_schema import Transaction


class CustomerStateManager:
    """
    Maintains recent transaction history separately for each customer.

    This is an in-memory implementation intended for our initial MVP.
    """

    def __init__(self, history_window_minutes: int = 60):
        if history_window_minutes <= 0:
            raise ValueError("History window must be greater than zero")

        self.history_window = timedelta(minutes=history_window_minutes)

        # customer_id -> list of Transaction objects
        self.customer_history = defaultdict(list)

        self.lock = Lock()

    def get_recent_transactions(
        self,
        customer_id: str,
        current_timestamp,
        window_minutes: int = 60,
    ) -> list[Transaction]:
        """
        Return transactions within the requested window,
        excluding the current transaction.
        """

        cutoff_time = current_timestamp - timedelta(
            minutes=window_minutes
        )

        with self.lock:
            history = self.customer_history[customer_id]

            return [
                transaction
                for transaction in history
                if cutoff_time <= transaction.timestamp < current_timestamp
            ]

    def add_transaction(self, transaction: Transaction) -> bool:
        """
        Add a transaction to customer history.

        Returns:
            True  - transaction added
            False - duplicate transaction ID found
        """

        with self.lock:
            history = self.customer_history[transaction.customer_id]

            # Prevent duplicates within the retained history
            if any(
                existing.transaction_id == transaction.transaction_id
                for existing in history
            ):
                return False

            history.append(transaction)

            # Keep transactions ordered by event timestamp
            history.sort(key=lambda item: item.timestamp)

            # Retain only the configured rolling history window
            latest_timestamp = history[-1].timestamp
            cutoff_time = latest_timestamp - self.history_window

            self.customer_history[transaction.customer_id] = [
                item
                for item in history
                if item.timestamp >= cutoff_time
            ]

            return True

    def get_customer_count(self) -> int:
        """Return the number of customers currently tracked."""

        with self.lock:
            return len(self.customer_history)
