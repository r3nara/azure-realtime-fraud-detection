
import logging

from src.transaction_generator.generator import generate_batch
from src.event_producer.producer import TransactionProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logging.getLogger("azure").setLevel(logging.WARNING)


def main():

    transactions = generate_batch(20, seed=42)

    producer = TransactionProducer()

    try:
        sent_count = producer.send_batch(transactions)

        print("SUCCESS: Transactions sent to Event Hubs!")
        print(f"Expected transactions: {len(transactions)}")
        print(f"Submitted transactions: {sent_count}")
        print("Event Hub: transactions")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
