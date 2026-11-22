
from src.transaction_generator.generator import generate_batch


def test_generate_batch_count():
    transactions = generate_batch(100, seed=42)

    assert len(transactions) == 100


def test_transaction_ids_are_unique():
    transactions = generate_batch(100, seed=42)

    ids = [
        transaction.transaction_id
        for transaction in transactions
    ]

    assert len(ids) == len(set(ids))


def test_generated_amounts_are_positive():
    transactions = generate_batch(100, seed=42)

    assert all(
        transaction.amount > 0
        for transaction in transactions
    )


def test_all_transactions_have_customer_ids():
    transactions = generate_batch(100, seed=42)

    assert all(
        transaction.customer_id
        for transaction in transactions
    )
