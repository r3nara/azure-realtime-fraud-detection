
import logging

from src.fraud_processor.consumer import TransactionConsumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logging.getLogger("azure").setLevel(logging.WARNING)


def main():

    consumer = TransactionConsumer()

    print("Starting Event Hubs consumer...")
    print("Consumer group: cg-fraud-detection")
    print("Starting position: Earliest retained event")
    print("Press Ctrl+C to stop")

    try:
        consumer.receive_events()

    except KeyboardInterrupt:
        print("\nConsumer stopped by user")


if __name__ == "__main__":
    main()
