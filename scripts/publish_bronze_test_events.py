
import logging
from datetime import datetime, timezone

from src.common.transaction_schema import Transaction
from src.event_producer.producer import TransactionProducer


def main():

    logging.basicConfig(level=logging.INFO)

    # Unique run identifier so reruns create new events.
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    transactions = []

    for i in range(1, 6):

        transaction = Transaction(
            transaction_id=f"BRONZE-TEST-{run_id}-{i:03d}",
            customer_id=f"BRONZE-CUSTOMER-{i:03d}",
            amount=1500.0 * i,
            currency="INR",
            merchant="Bronze Archive Test Merchant",
            transaction_type="ONLINE",
            location="Chennai",
            payment_method="UPI",
            device_id=f"BRONZE-DEVICE-{i:03d}",
            status="SUCCESS"
        )

        transactions.append(transaction)

    producer = TransactionProducer()

    try:

        print("\n========== BRONZE TEST EVENT PUBLISHER ==========")

        total_sent = producer.send_batch(transactions)

        print(f"\nTransactions submitted: {total_sent}")

        for transaction in transactions:
            print(f"Published: {transaction.transaction_id}")

        if total_sent != 5:
            raise RuntimeError(
                f"Expected to publish 5 events, got {total_sent}"
            )

        print("\nSUCCESS: Five test transactions published.")

    finally:

        producer.close()


if __name__ == "__main__":
    main()
