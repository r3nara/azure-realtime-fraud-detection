
from datetime import timedelta

from src.common.transaction_schema import (
    Transaction,
    TransactionStatus,
)


# ============================================================
# Fraud Detection Configuration
# ============================================================

# F01: High-value transaction
HIGH_VALUE_THRESHOLD = 50_000
HIGH_VALUE_SCORE = 30

# F02: High transaction velocity
VELOCITY_WINDOW_MINUTES = 10
VELOCITY_TRANSACTION_LIMIT = 5
VELOCITY_SCORE = 25

# F03: Location change
LOCATION_WINDOW_MINUTES = 30
LOCATION_CHANGE_SCORE = 30

# F04: Repeated failed transactions
FAILED_WINDOW_MINUTES = 10
FAILED_TRANSACTION_LIMIT = 3
FAILED_ATTEMPTS_SCORE = 15


# ============================================================
# Fraud Evaluation Engine
# ============================================================

def evaluate_transaction(
    transaction: Transaction,
    recent_transactions: list[Transaction],
) -> dict:
    """
    Evaluate an incoming transaction against recent customer history.

    The incoming transaction is evaluated against previous transactions.
    It is not added to customer history by this function.

    Returns:
        Dictionary containing:
        - transaction_id
        - customer_id
        - risk_score
        - risk_level
        - triggered_rules
    """

    risk_score = 0
    triggered_rules = []

    current_time = transaction.timestamp

    # ========================================================
    # F01: High-value transaction
    # ========================================================

    if transaction.amount > HIGH_VALUE_THRESHOLD:

        risk_score += HIGH_VALUE_SCORE
        triggered_rules.append("F01_HIGH_VALUE")

    # ========================================================
    # F02: High transaction velocity
    # ========================================================

    velocity_cutoff = current_time - timedelta(
        minutes=VELOCITY_WINDOW_MINUTES
    )

    transactions_in_velocity_window = [
        item
        for item in recent_transactions
        if velocity_cutoff <= item.timestamp < current_time
    ]

    # Include the incoming transaction in the count
    velocity_count = len(transactions_in_velocity_window) + 1

    if velocity_count > VELOCITY_TRANSACTION_LIMIT:

        risk_score += VELOCITY_SCORE
        triggered_rules.append("F02_HIGH_VELOCITY")

    # ========================================================
    # F03: Location change within 30 minutes
    # ========================================================

    location_cutoff = current_time - timedelta(
        minutes=LOCATION_WINDOW_MINUTES
    )

    recent_location_transactions = [
        item
        for item in recent_transactions
        if location_cutoff <= item.timestamp < current_time
    ]

    # Compare the incoming transaction with the most recent
    # previous transaction, rather than every old location.
    if recent_location_transactions:

        latest_transaction = max(
            recent_location_transactions,
            key=lambda item: item.timestamp,
        )

        if latest_transaction.location != transaction.location:

            risk_score += LOCATION_CHANGE_SCORE
            triggered_rules.append("F03_LOCATION_CHANGE")

    # ========================================================
    # F04: Repeated failed transactions
    # ========================================================

    failed_cutoff = current_time - timedelta(
        minutes=FAILED_WINDOW_MINUTES
    )

    failed_transactions = [
        item
        for item in recent_transactions
        if (
            failed_cutoff <= item.timestamp < current_time
            and item.status == TransactionStatus.FAILED
        )
    ]

    # Include the incoming transaction if it has failed
    current_transaction_failed = (
        transaction.status == TransactionStatus.FAILED
    )

    failed_count = (
        len(failed_transactions)
        + int(current_transaction_failed)
    )

    if failed_count >= FAILED_TRANSACTION_LIMIT:

        risk_score += FAILED_ATTEMPTS_SCORE
        triggered_rules.append("F04_REPEATED_FAILURES")

    # ========================================================
    # Calculate final risk score
    # ========================================================

    risk_score = min(risk_score, 100)

    if risk_score >= 60:
        risk_level = "HIGH"

    elif risk_score >= 30:
        risk_level = "MEDIUM"

    else:
        risk_level = "LOW"

    # ========================================================
    # Return fraud evaluation result
    # ========================================================

    return {
        "transaction_id": transaction.transaction_id,
        "customer_id": transaction.customer_id,
        "risk_score": risk_score,
        "risk_level": risk_level,
        "triggered_rules": triggered_rules,
    }
