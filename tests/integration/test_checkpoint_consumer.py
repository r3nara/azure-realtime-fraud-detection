
import logging

from src.fraud_processor.consumer import TransactionConsumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logging.getLogger("azure").setLevel(logging.WARNING)


def main():

    consumer = TransactionConsumer()

    try:
        processed_count = consumer.receive_events(max_events=5)

        print()
        print("DURABLE FRAUD DETECTION TEST COMPLETED")
        print(f"Events handled: {processed_count}")

    except KeyboardInterrupt:
        print("\nTest interrupted by user")


if __name__ == "__main__":
    main()
