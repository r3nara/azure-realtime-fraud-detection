
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from src.common.transaction_schema import (
    Transaction,
    TransactionType,
    PaymentMethod,
    TransactionStatus,
)
from src.event_producer.producer import TransactionProducer


def create_transaction(
    customer_id,
    timestamp,
    amount=1000,
    location="Chennai",
    status=TransactionStatus.SUCCESS,
):
    return Transaction(
        transaction_id=f"TXN-DAY08-{uuid4()}",
        customer_id=customer_id,
        amount=amount,
        currency="INR",
        merchant="Day08-Test-Merchant",
        transaction_type=TransactionType.ONLINE,
        location=location,
        timestamp=timestamp,
        payment_method=PaymentMethod.UPI,
        device_id="DAY08-TEST-DEVICE",
        status=status,
    )


def build_test_scenarios():
    now = datetime.now(timezone.utc)
    transactions = []

    # --------------------------------------------------------
    # F01: High-value transaction
    # Expected score: 30
    # --------------------------------------------------------

    high_value = create_transaction(
        customer_id="TEST-HIGH-001",
        timestamp=now,
        amount=75000,
    )

    transactions.append(high_value)

    # --------------------------------------------------------
    # F02: High transaction velocity
    # Six transactions within ten minutes
    # Expected score on sixth transaction: 25
    # --------------------------------------------------------

    for minute in range(5, -1, -1):
        transactions.append(
            create_transaction(
                customer_id="TEST-VEL-001",
                timestamp=now - timedelta(minutes=minute),
            )
        )

    # --------------------------------------------------------
    # F03: Location change
    # Chennai followed by Mumbai within 30 minutes
    # Expected score on second transaction: 30
    # --------------------------------------------------------

    transactions.extend([
        create_transaction(
            customer_id="TEST-LOC-001",
            timestamp=now - timedelta(minutes=10),
            location="Chennai",
        ),
        create_transaction(
            customer_id="TEST-LOC-001",
            timestamp=now,
            location="Mumbai",
        ),
    ])

    # --------------------------------------------------------
    # F04: Repeated failed transactions
    # Three failed transactions within ten minutes
    # Expected score on third transaction: 15
    # --------------------------------------------------------

    for minute in (2, 1, 0):
        transactions.append(
            create_transaction(
                customer_id="TEST-FAIL-001",
                timestamp=now - timedelta(minutes=minute),
                status=TransactionStatus.FAILED,
            )
        )

    return transactions


def main():
    transactions = build_test_scenarios()

    print("=" * 65)
    print("DAY 08 - LIVE FRAUD DETECTION TEST DATA")
    print("=" * 65)

    print(f"Total transactions: {len(transactions)}")
    print()

    for transaction in transactions:
        print(
            f"{transaction.customer_id} | "
            f"{transaction.transaction_id} | "
            f"Amount: INR {transaction.amount:.2f} | "
            f"Location: {transaction.location} | "
            f"Status: {transaction.status}"
        )

    print()
    print("Publishing transactions to Azure Event Hubs...")
    print()

    producer = TransactionProducer()

    try:
        total_sent = producer.send_batch(transactions)

        print(f"Successfully published: {total_sent}/{len(transactions)}")

        if total_sent != len(transactions):
            raise RuntimeError(
                "Not all transactions were published."
            )

    finally:
        producer.close()

    print()
    print("TARGET TRANSACTION IDS")
    print("=" * 65)

    for transaction in transactions:
        print(transaction.transaction_id)

    print()
    print("Publishing completed.")


if __name__ == "__main__":
    main()
