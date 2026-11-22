
from src.transaction_generator.generator import generate_batch


def test_suspicious_transaction_distribution():
    transactions = generate_batch(10000, seed=42)

    suspicious = [
        t for t in transactions
        if t.amount >= 50000
    ]

    percentage = len(suspicious) / len(transactions) * 100

    print(f"\nTotal transactions: {len(transactions)}")
    print(f"Suspicious transactions: {len(suspicious)}")
    print(f"Suspicious percentage: {percentage:.2f}%")

    # Allow statistical variation around the expected 5%
    assert 4.0 <= percentage <= 6.0
