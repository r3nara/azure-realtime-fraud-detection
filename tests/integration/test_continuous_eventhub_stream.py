
import logging

from src.transaction_generator.stream import generate_continuously
from src.event_producer.producer import TransactionProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logging.getLogger("azure").setLevel(logging.WARNING)


def main():

    total_events = 30
    rate = 2

    producer = TransactionProducer()

    sent_count = 0

    print("Starting continuous Event Hubs stream")
    print(f"Rate: {rate} transactions/second")
    print(f"Total transactions: {total_events}")
    print("-" * 55)

    try:

        for transaction in generate_continuously(
            rate_per_second=rate,
            max_events=total_events,
            seed=42
        ):

            producer.send_transaction(transaction)

            sent_count += 1

            print(
                f"Sent {sent_count}/{total_events} | "
                f"Customer: {transaction.customer_id} | "
                f"Amount: INR {transaction.amount:.2f}"
            )

    finally:
        producer.close()

    print("-" * 55)
    print("STREAM COMPLETED")
    print(f"Total transactions submitted: {sent_count}")


if __name__ == "__main__":
    main()
