
from src.transaction_generator.stream import generate_continuously

def main():

    print("Starting continuous transaction generation...")
    print("Rate: 2 transactions per second")
    print("Maximum: 10 transactions")
    print("-" * 50)

    count = 0

    for transaction in generate_continuously(
        rate_per_second=2,
        max_events=10,
        seed=42
    ):
        count += 1

        print(
            f"{count}. "
            f"Transaction: {transaction.transaction_id} | "
            f"Customer: {transaction.customer_id} | "
            f"Amount: INR {transaction.amount}"
        )

    print("-" * 50)
    print(f"Completed. Total transactions: {count}")


if __name__ == "__main__":
    main()
